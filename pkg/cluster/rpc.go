package cluster

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/encoding"
	"github.com/deejross/mydis/pkg/rpclib"
	"github.com/hashicorp/raft"
)

const null = byte(0)

var nullB = []byte{0}

// RPC controller for Cluster.
type RPC struct {
	*rpclib.RPC

	cluster *Cluster
}

// NewRPC returns a new RPC controller for the Cluster.
func NewRPC(cluster *Cluster) (*RPC, error) {
	rpc, err := rpclib.NewRPC(cluster.config.BindAddress, cluster.config.RPCPort)
	if err != nil {
		return nil, err
	}

	r := &RPC{
		RPC:     rpc,
		cluster: cluster,
	}

	go r.start()

	return r, nil
}

// WriteCommand writes a command to the RPC controller and waits for a response.
func (r *RPC) WriteCommand(conn net.Conn, cmd *fsm.Command) *fsm.CommandResult {
	conn.Write(cmd.Encode())
	_, err := conn.Write(nullB)
	if err != nil {
		return fsm.CommandResultError(err)
	}

	b, err := bufio.NewReader(conn).ReadBytes(null)
	if err != nil {
		return fsm.CommandResultError(err)
	}

	result := &fsm.CommandResult{}
	if err := encoding.DecodeMsgPack(b, result); err != nil {
		return fsm.CommandResultError(err)
	}

	return result
}

// WriteResult writes a result back to the sender with a CommandResult.
func (r *RPC) WriteResult(conn net.Conn, result *fsm.CommandResult) error {
	conn.Write(result.Encode())
	_, err := conn.Write(nullB)
	return err
}

// start listening for RPC commands.
func (r *RPC) start() {
	for {
		conn := r.AcceptStream()
		go r.readMessages(conn)
	}
}

// readMessages continually reads and processes incoming messages.
func (r *RPC) readMessages(conn net.Conn) {
	for {
		b, err := bufio.NewReader(conn).ReadBytes(null)
		if err != nil {
			log.Debug(fmt.Sprintf("error while reading RPC message: %v", err))
			return
		}

		cmd := &fsm.Command{}
		if err := encoding.DecodeMsgPack(b, cmd); err != nil {
			errStr := fmt.Sprintf("error decoding RPC message: %v", err)
			r.WriteResult(conn, fsm.CommandResultError(fmt.Errorf(errStr)))
			continue
		}

		result := r.processCommand(cmd)
		r.WriteResult(conn, result)
		log.Debug("RPC.processCommand.result", "err", result.Error(), "vals", result.ValStrings())
	}
}

// processCommand processes a single RPC command.
func (r *RPC) processCommand(cmd *fsm.Command) *fsm.CommandResult {
	cmd.Op = strings.ToUpper(cmd.Op)

	log.Debug("RPC.processCommand", "op", cmd.Op, "key", string(cmd.Key), "args", cmd.ArgStrings())

	// handle cluster operations commands
	switch cmd.Op {
	case "PING":
		return fsm.CommandResultString("PONG")
	case "JOIN":
		if cmd.Args == nil || len(cmd.Args) == 0 {
			return fsm.CommandResultError(fmt.Errorf("address:port required for JOIN"))
		}

		if !r.cluster.IsLeader() {
			return r.forwardCommandToLeader(cmd)
		}

		raftAddr := string(cmd.Args[0])
		f := r.cluster.raft.AddVoter(raft.ServerID(raftAddr), raft.ServerAddress(raftAddr), 0, r.cluster.config.RaftTimeout)
		if err := f.Error(); err != nil {
			return fsm.CommandResultError(fmt.Errorf("while joining Raft: %s: %v", raftAddr, err))
		}

		log.Info("joined", "addr", string(cmd.Args[0]))
		return fsm.CommandResultString("OK")
	}

	// handle registered KV commands that may have been forwarded via RPC to the Leader
	return r.cluster.Command(cmd)
}

// forwardCommandToLeader forwards the given command to the current leader.
func (r *RPC) forwardCommandToLeader(cmd *fsm.Command) *fsm.CommandResult {
	leader := string(r.cluster.raft.Leader())
	for attempts := 0; len(leader) == 0; attempts++ {
		if attempts >= 100 {
			return fsm.CommandResultError(fmt.Errorf("unable to determine the leader of the cluster"))
		}

		time.Sleep(100 * time.Millisecond)
		leader = string(r.cluster.raft.Leader())
	}

	leader = GetRPCAddress(leader)

	log.Debug("RPC.forwardCommandToLeader", "op", cmd.Op, "key", string(cmd.Key), "args", cmd.ArgStrings(), "leader", leader)
	conn, err := r.Dial(string(leader), r.cluster.config.RaftTimeout)
	if err != nil {
		return fsm.CommandResultError(fmt.Errorf("unable to contact leader: %v", err))
	}

	result := r.WriteCommand(conn, cmd)
	conn.Close()
	return result
}

// GetRPCAddress from the given raftAddr, which is always one port higher than raftAddr.
func GetRPCAddress(raftAddr string) string {
	addr, port, _ := net.SplitHostPort(raftAddr)
	portI, _ := strconv.Atoi(port)
	return net.JoinHostPort(addr, strconv.Itoa(portI+1))
}
