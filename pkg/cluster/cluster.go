package cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/deejross/mydis/pkg/cluster/storage/bbolt"
	"github.com/deejross/mydis/pkg/encoding"
	"github.com/deejross/mydis/pkg/logger"
	"github.com/deejross/mydis/pkg/rpclib"
	"github.com/hashicorp/raft"
)

const (
	fileRaftLog     = "raft-log.db"
	fileRaftKV      = "raft-kv.db"
	snapshotsToKeep = 2
	logCacheSize    = 512
)

var (
	log = logger.New("pkg/cluster")
)

// OnLeadershipChangeFunc is the function used by OnLeadershipChange.
type OnLeadershipChangeFunc func(isLeader bool)

// Cluster object represents cluster operations.
type Cluster struct {
	config             *Config
	closeCh            chan struct{}
	clusterDir         string
	rpc                *RPC
	raft               *raft.Raft
	raftConfig         *raft.Config
	raftTransport      *raft.NetworkTransport
	raftNotifyCh       <-chan bool
	logStore           storage.Store
	logCache           *raft.LogCache
	kvStore            storage.Store
	snap               raft.SnapshotStore
	fsm                *fsm.FSM
	readHandlers       map[string]fsm.CommandHandler
	cmdNotFoundHandler fsm.CommandHandler
	onLeaderChangeFunc OnLeadershipChangeFunc

	mu sync.RWMutex
}

// NewCluster returns a new Cluster object.
func NewCluster(config *Config) (*Cluster, error) {
	if config == nil {
		config = &Config{}
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	c := &Cluster{
		config:       config,
		closeCh:      make(chan struct{}),
		readHandlers: map[string]fsm.CommandHandler{},
		cmdNotFoundHandler: func(cmd *fsm.Command, db storage.Store) *fsm.CommandResult {
			return fsm.CommandResultError(fmt.Errorf("unknown command: %s", cmd.Op))
		},
	}

	if err := c.initDataDir(); err != nil {
		return nil, err
	}

	if err := c.initRaft(); err != nil {
		return nil, err
	}

	r, err := NewRPC(c)
	if err != nil {
		return nil, err
	}
	c.rpc = r

	for {
		// auto retry to init cluster in case of failure
		err := c.initCluster()
		if err != nil {
			log.Error(err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	log = log.With("source", net.JoinHostPort(config.BindAddress, config.RPCPort))

	return c, nil
}

// Shutdown the this instance in the cluster.
func (c *Cluster) Shutdown() {
	c.raft.Shutdown().Error()
	close(c.closeCh)

	c.kvStore.Close()
	c.logStore.Close()
}

// IsLeader returns `true` if this instance is the cluster leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// OnLeadershipChange sets a function to call whenever the leadership in the cluster changes.
func (c *Cluster) OnLeadershipChange(f func(isLeader bool)) {
	c.mu.Lock()
	c.onLeaderChangeFunc = f
	c.mu.Unlock()
}

// RegisterHandler registers a new Command handle. If writable is true, the Command
// is registered on the FSM and forwarded to the Leader for writing, otherwise
// the Cluster handles reads on the local instance.
func (c *Cluster) RegisterHandler(op string, handler fsm.CommandHandler, writable bool) {
	if writable {
		c.fsm.RegisterWriteHandler(op, handler)
	} else {
		c.readHandlers[strings.ToUpper(op)] = handler
	}
}

// RegisterCommandNotFoundHandler registers the handler for when an unknown Command is issued.
func (c *Cluster) RegisterCommandNotFoundHandler(handler fsm.CommandHandler) {
	c.cmdNotFoundHandler = handler
}

// Command performs the requested operation on the cluster.
func (c *Cluster) Command(cmd *fsm.Command) *fsm.CommandResult {
	// cmd.Op should always be uppercase
	cmd.Op = strings.ToUpper(cmd.Op)

	// handle reads depending on ReadConsistency setting
	fn, ok := c.readHandlers[cmd.Op]
	if ok {
		if cmd.ReadConsistencyMode == fsm.ConsistencyEventual ||
			(cmd.ReadConsistencyMode == fsm.ConsistencyStrong && c.IsLeader()) {
			return fn(cmd, c.kvStore)
		} else if cmd.ReadConsistencyMode == fsm.ConsistencyStrong {
			return c.rpc.forwardCommandToLeader(cmd)
		}

		return fsm.CommandResultError(fmt.Errorf("unknown ReadConsistencyMode: %d", cmd.ReadConsistencyMode))
	}

	// handle writes by writing them to the Raft log if Leader,
	// otherwise forward Command to Leader.
	if c.fsm.IsWriteCommandRegistered(cmd.Op) {
		if c.IsLeader() {
			log.Debug("apply", "op", cmd.Op, "key", string(cmd.Key))

			b, err := encoding.EncodeMsgPack(cmd)
			if err != nil {
				return fsm.CommandResultError(fmt.Errorf("unable to encode command: %v", err))
			}

			// apply the command to the Raft log
			f := c.raft.Apply(b.Bytes(), c.config.RaftTimeout)
			if err := f.Error(); err != nil {
				return fsm.CommandResultError(err)
			}

			// if strong consistency is required, wait for all instances to commit the write
			if c.config.DefaultWriteConsistency == WriteConsistencyStrong || cmd.WriteConsistencyMode == fsm.ConsistencyStrong {
				if err := c.raft.Barrier(c.config.RaftTimeout).Error(); err != nil {
					return fsm.CommandResultError(err)
				}
			}

			return fsm.DecodeCommandResult(f.Response())
		}

		return c.rpc.forwardCommandToLeader(cmd)
	}

	// this command is unknown, so use the cmdNotFound handler.
	return c.cmdNotFoundHandler(cmd, c.kvStore)
}

// initDataDir initializes the data storage directory.
func (c *Cluster) initDataDir() error {
	dataDir, err := filepath.Abs(c.config.DataDir)
	if err != nil {
		return fmt.Errorf("unable to determine absolute DataDir path: %v", err)
	}

	clusterDir := filepath.Join(dataDir, c.config.Name)
	if err := os.MkdirAll(clusterDir, 0700); err != nil {
		return fmt.Errorf("unable to make directory: %s: %v", clusterDir, err)
	}

	c.clusterDir = clusterDir

	return nil
}

// initRaft initializes Raft.
func (c *Cluster) initRaft() error {
	// begin configuring Raft
	c.raftConfig = raft.DefaultConfig()
	c.raftConfig.LocalID = raft.ServerID(net.JoinHostPort(c.config.BindAddress, c.config.RaftPort))
	c.raftConfig.Logger = log.Named("raft")

	if level := os.Getenv("LOG_LEVEL"); len(level) > 0 {
		c.raftConfig.LogLevel = level
	}

	// configure the Raft network transport
	if err := c.initRaftTransport(); err != nil {
		return fmt.Errorf("unable to initialize Raft transport: %v", err)
	}

	if err := c.initRaftStorage(); err != nil {
		return fmt.Errorf("unable to initialize Raft storage: %v", err)
	}

	// create the FSM
	c.fsm = fsm.NewFSM(string(c.raftConfig.LocalID), c.kvStore)

	// setup channel for reliable leadership notifications
	raftNotifyCh := make(chan bool, 10)
	c.raftConfig.NotifyCh = raftNotifyCh
	c.raftNotifyCh = raftNotifyCh

	// setup Raft
	var err error
	c.raft, err = raft.NewRaft(
		c.raftConfig,
		c.fsm,
		c.logCache,
		c.kvStore,
		c.snap,
		c.raftTransport,
	)

	go c.monitorLeadership()

	return err
}

// initRaftTransport initializes the Raft TCP transport.
func (c *Cluster) initRaftTransport() error {
	sl, err := rpclib.NewStreamLayer(c.config.BindAddress, c.config.RaftPort)
	if err != nil {
		return err
	}

	conf := &raft.NetworkTransportConfig{
		ServerAddressProvider: sl,
		Stream:                sl,
		MaxPool:               3,
		Timeout:               10 * time.Second,
		Logger:                log.Named("transport"),
	}

	c.raftTransport = raft.NewNetworkTransportWithConfig(conf)

	return nil
}

// initRaftStorage initializes the Raft log and kv storage.
func (c *Cluster) initRaftStorage() error {
	var err error

	// create the Raft log database
	logPath := filepath.Join(c.clusterDir, fileRaftLog)
	c.logStore, err = bbolt.NewBoltStore(bbolt.DefaultConfig(logPath))
	if err != nil {
		return fmt.Errorf("unable to create Raft log database: %v", err)
	}

	// create the key/value database
	kvPath := filepath.Join(c.clusterDir, fileRaftKV)
	c.kvStore, err = bbolt.NewBoltStore(bbolt.DefaultConfig(kvPath))
	if err != nil {
		return fmt.Errorf("unable to create Raft KV database: %v", err)
	}

	// wrap the Raft log database with a caching layer to increase the read performance of new logs
	c.logCache, err = raft.NewLogCache(logCacheSize, c.logStore)
	if err != nil {
		return fmt.Errorf("unable to create Raft log cache: %v", err)
	}

	// create the snapshot store
	c.snap, err = raft.NewFileSnapshotStore(c.clusterDir, snapshotsToKeep, c.raftConfig.LogOutput)
	if err != nil {
		return fmt.Errorf("unable to create snapshot store: %v", err)
	}

	return nil
}

// initCluster attempts to bootstrap a new cluster, or join an existing one.
func (c *Cluster) initCluster() error {
	// attempt to bootstrap the cluster if needed
	hasState, err := raft.HasExistingState(c.logStore, c.kvStore, c.snap)
	if err != nil {
		return fmt.Errorf("unable to bootstrap cluster: %v", err)
	}

	if !hasState && len(c.config.joinAddrs) == 0 {
		log.Info("attempting to bootstrap cluster")

		serverConf := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:       c.raftConfig.LocalID,
					Address:  c.raftTransport.LocalAddr(),
					Suffrage: raft.Voter,
				},
			},
		}

		f := c.raft.BootstrapCluster(serverConf)
		if err := f.Error(); err != nil {
			return fmt.Errorf("bootstrapping cluster failed: %v", err)
		}

		log.Info("bootstrapping cluster succeeded")
		return nil
	}

	for _, remoteRaftAddr := range c.config.joinAddrs {
		localRaftAddr := string(c.raftTransport.LocalAddr())
		if remoteRaftAddr == localRaftAddr {
			continue
		}

		log.Info(fmt.Sprintf("attempting to join: %s", remoteRaftAddr))

		rpcAddr := GetRPCAddress(remoteRaftAddr)
		conn, err := c.rpc.Dial(rpcAddr, c.config.RaftTimeout)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		result := c.rpc.WriteCommand(conn, &fsm.Command{
			Op: "JOIN",
			Args: [][]byte{
				[]byte(localRaftAddr),
			},
		})
		if result.Error() != nil {
			log.Error(result.Err)
			continue
		}

		if result.ValString() == "OK" {
			log.Info(fmt.Sprintf("join to %s was successful", remoteRaftAddr))
			return nil
		}

		log.Error(fmt.Sprintf("unable to join %s due to unknown error", remoteRaftAddr))
	}

	return fmt.Errorf("all attempts to join an existing cluster have failed")
}

// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster.
func (c *Cluster) monitorLeadership() {
	for {
		select {
		case isLeader := <-c.raftNotifyCh:
			if isLeader {
				log.Info("this instance is now the leader of the cluster")
			} else {
				log.Info("this instance is no longer the leader of the cluster")
			}

			c.mu.RLock()
			if c.onLeaderChangeFunc != nil {
				c.onLeaderChangeFunc(isLeader)
			}
			c.mu.RUnlock()
		case <-c.closeCh:
			return
		}
	}
}
