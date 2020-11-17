package rpclib

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/deejross/mydis/pkg/logger"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/yamux"
)

var (
	log = logger.New("rcplib")
)

// RPC multiplexed transport for out-of-band cluster RPC communications.
// This can act as both the client and server for RPC communications.
// Client connections are multiplexed over a single TCP connection to
// allow for safe and cheap concurrent streams.
type RPC struct {
	net.Listener
	address    *net.TCPAddr
	clients    map[string]*yamux.Session
	newStreams chan net.Conn
	mu         sync.RWMutex
}

// NewRPC creates a new RPC object. Requires and address and port number
// to listen on.
func NewRPC(address, port string) (*RPC, error) {
	portI, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("could not parse RPC port: %s: %v", port, err)
	}

	ip := &net.TCPAddr{
		IP:   net.ParseIP(address),
		Port: portI,
	}

	if ip.IP == nil {
		return nil, fmt.Errorf("unable to parse RPC address: %s", address)
	}

	listener, err := net.ListenTCP("tcp", ip)
	if err != nil {
		return nil, fmt.Errorf("unable to listen for RPC on: %s: %v", ip.String(), err)
	}

	r := &RPC{
		Listener:   listener,
		address:    ip,
		clients:    map[string]*yamux.Session{},
		newStreams: make(chan net.Conn, 10),
	}

	go r.handleAccept()

	return r, nil
}

// Dial attempts to create a new stream on an existing TCP, or creates a
// new TCP connection if one doesn't already exist or is closed.
func (r *RPC) Dial(address string, timeout time.Duration) (net.Conn, error) {
	r.mu.RLock()
	session, ok := r.clients[address]
	r.mu.RUnlock()

	if ok && !session.IsClosed() {
		conn, err := session.Open()
		if err != nil {
			return nil, fmt.Errorf("unable to open new RPC stream on existing TCP connection: %v", err)
		}

		return &Connection{conn}, nil
	}

	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, fmt.Errorf("unable to dial RPC address: %s: %v", address, err)
	}

	session, err = yamux.Client(conn, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to start multiplex RPC stream: %v", err)
	}

	r.mu.Lock()
	r.clients[address] = session
	r.mu.Unlock()

	conn, err = session.Open()
	if err != nil {
		return nil, fmt.Errorf("unable to open new RPC stream on new TCP connection: %v", err)
	}

	return &Connection{conn}, nil
}

// Close the RPC controller.
func (r *RPC) Close() error {
	return r.Listener.Close()
}

// AcceptStream accepts any queued incoming streams. This will block until a new stream is available.
func (r *RPC) AcceptStream() net.Conn {
	return <-r.newStreams
}

// handleAccept handles the accepting of new TCP connections and queues new accept streams.
// Streams can be accepted by calling AcceptStream().
func (r *RPC) handleAccept() {
	for {
		conn, err := r.Accept()
		if err != nil {
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				log.Debug("stopped accepting RPC connections due to RPC port being closed")
				return
			}
			log.Error(fmt.Sprintf("dropping incoming RPC connection: %v", err))
			continue
		}

		session, err := yamux.Server(conn, nil)
		if err != nil {
			log.Error(fmt.Sprintf("unable to start multiplexing RPC connection: %v", err))
			conn.Close()
			continue
		}

		go func(session *yamux.Session) {
			for {
				stream, err := session.Accept()
				if err != nil {
					if err == io.ErrClosedPipe {
						return
					}
					log.Error(fmt.Sprintf("unable to accept new RPC stream: %v", err))
					stream.Close()
					continue
				}

				r.newStreams <- stream
			}
		}(session)
	}
}

// StreamLayer implements raft.StreamLayer to include additional features to the Raft transport.
type StreamLayer struct {
	net.Listener

	address *net.TCPAddr
}

// NewStreamLayer creates a new StreamLayer object.
func NewStreamLayer(address, port string) (*StreamLayer, error) {
	portI, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("could not parse Raft port: %s: %v", port, err)
	}

	ip := &net.TCPAddr{
		IP:   net.ParseIP(address),
		Port: portI,
	}

	if ip.IP == nil {
		return nil, fmt.Errorf("unable to parse Raft address: %s", address)
	}

	listener, err := net.ListenTCP("tcp", ip)
	if err != nil {
		return nil, fmt.Errorf("unable to listen for Raft on: %s: %v", ip.String(), err)
	}

	return &StreamLayer{
		Listener: listener,
		address:  ip,
	}, nil
}

// Dial implements raft.StreamLayer by establishing a connection with the given address.
func (s *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, fmt.Errorf("unable to dial Raft: %s: %v", address, err)
	}

	return &Connection{conn}, nil
}

// Accept the connection and add multiplexing to it.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("unable to accept incoming connection: %v", err)
	}

	return &Connection{conn}, nil
}

// ServerAddr takes a raft.ServerID and returns it as a ServerAddress.
// This implements raft.ServerAddressProvider.
func (s *StreamLayer) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	return raft.ServerAddress(id), nil
}

// Connection object that wraps net.Conn to provide additional features.
type Connection struct {
	net.Conn
}
