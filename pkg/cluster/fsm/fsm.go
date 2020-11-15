package fsm

import (
	"fmt"
	"io"
	"strings"

	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/deejross/mydis/pkg/encoding"
	"github.com/deejross/mydis/pkg/logger"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

var (
	log = logger.New("pkg/cluster/fsm")
)

const (
	// ConsistencyEventual reads from the local instance's database which may fall several milliseconds behind the leader.
	// Writes are blocked until committed by the leader instance.
	ConsistencyEventual = iota

	// ConsistencyStrong forces reads to occur from the leader, ensuring consistent data but at the cost of performance.
	// Writes are blocked until committed by all instances in the cluster.
	ConsistencyStrong
)

// Command represents a request to perform an operation against the FSM.
type Command struct {
	Op                   string   `json:"op"`
	Key                  []byte   `json:"key,omitempty"`
	Args                 [][]byte `json:"args,omitempty"`
	ReadConsistencyMode  int      `json:"rcm,omitempty"`
	WriteConsistencyMode int      `json:"wcm,omitempty"`
}

// Encode will encode the Command.
func (c *Command) Encode() []byte {
	if c == nil {
		return nil
	}

	buf, err := encoding.EncodeMsgPack(c)
	if err != nil {
		log.Error("failed to encode Command: %v. This is likely a bug and should be reported.", err)
		return nil
	}

	return buf.Bytes()
}

// ArgStrings returns Val as a string slice or nil.
// This is mostly used for DEBUG logs.
func (c *Command) ArgStrings() []string {
	if c.Args != nil && len(c.Args) > 0 {
		ss := []string{}
		for _, b := range c.Args {
			if b != nil {
				ss = append(ss, string(b))
			}
		}
		return ss
	}
	return nil
}

// CommandResult represents the result of a Command.
type CommandResult struct {
	Err string   `json:"err"`
	Val [][]byte `json:"val"`
}

// DecodeCommandResult will decode a CommandResult from an interface{}.
// This will return nil if CommandResult result was nil.
func DecodeCommandResult(v interface{}) *CommandResult {
	if v == nil {
		return nil
	}

	result := &CommandResult{}

	b, ok := v.([]byte)
	if !ok {
		result.Err = "unable to decode CommandResult: expected byte slice, got unknown type"
		return result
	}

	if err := encoding.DecodeMsgPack(b, result); err != nil {
		result.Err = fmt.Sprintf("unable to decode CommandResult: %v", err)
		return result
	}

	return result
}

// Encode will encode the CommandResult.
func (c *CommandResult) Encode() []byte {
	if c == nil {
		return nil
	}

	buf, err := encoding.EncodeMsgPack(c)
	if err != nil {
		log.Error("failed to encode CommandResult: %v. This is likely a bug and should be reported.", err)
		return nil
	}

	return buf.Bytes()
}

// Error will return an error if there is an error.
func (c *CommandResult) Error() error {
	if len(c.Err) > 0 {
		return fmt.Errorf(c.Err)
	}
	return nil
}

// ValString returns the first Val as a string, or empty string.
func (c *CommandResult) ValString() string {
	if c.Val != nil && len(c.Val) > 0 && c.Val[0] != nil {
		return string(c.Val[0])
	}
	return ""
}

// ValStrings returns Val as a string slice or nil.
func (c *CommandResult) ValStrings() []string {
	if c.Val != nil && len(c.Val) > 0 {
		ss := []string{}
		for _, b := range c.Val {
			if b != nil {
				ss = append(ss, string(b))
			}
		}
		return ss
	}
	return nil
}

// CommandResultError represents the result of a Command with an error.
func CommandResultError(err error) *CommandResult {
	if err == nil {
		return &CommandResult{}
	}

	return &CommandResult{
		Err: err.Error(),
	}
}

// CommandResultBytes represents the results of a Command with a byte slice result.
func CommandResultBytes(b []byte) *CommandResult {
	return &CommandResult{
		Val: [][]byte{
			b,
		},
	}
}

// CommandResultString represents the result of a Command with a string result.
func CommandResultString(str string) *CommandResult {
	return &CommandResult{
		Val: [][]byte{
			[]byte(str),
		},
	}
}

// CommandResultStrings represents a list of string results of a Command.
func CommandResultStrings(ss []string) *CommandResult {
	c := &CommandResult{
		Val: make([][]byte, len(ss)),
	}

	for i, s := range ss {
		c.Val[i] = []byte(s)
	}

	return c
}

// CommandHandler is the function used in RegisterHandler.
type CommandHandler func(*Command, storage.Store) *CommandResult

// FSM (Finite State Machine) object.
type FSM struct {
	kvStore       storage.Store
	writeHandlers map[string]CommandHandler
	log           hclog.Logger
}

// NewFSM returns a new FSM object.
func NewFSM(source string, kvStore storage.Store) *FSM {
	f := &FSM{
		kvStore:       kvStore,
		writeHandlers: map[string]CommandHandler{},
		log:           logger.New("pkg/cluster/fsm").With("source", source),
	}

	return f
}

// RegisterWriteHandler registers a new CommandHandler for commands that modify the state.
func (f *FSM) RegisterWriteHandler(op string, handler CommandHandler) {
	// command ops are always uppercase
	f.writeHandlers[strings.ToUpper(op)] = handler
}

// IsWriteCommandRegistered determines if the given Command has been registered.
func (f *FSM) IsWriteCommandRegistered(op string) bool {
	_, ok := f.writeHandlers[op]
	return ok
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM) Apply(l *raft.Log) interface{} {
	cmd := &Command{}
	if err := encoding.DecodeMsgPack(l.Data, cmd); err != nil {
		f.log.Error(fmt.Sprintf("decoding message: %v", err))
		return CommandResultError(err).Encode()
	}

	f.log.Debug("apply", "op", cmd.Op, "key", string(cmd.Key))

	// make sure command was registered with RegisterWriteHandler
	fn, ok := f.writeHandlers[cmd.Op]
	if !ok {
		f.log.Debug("apply", "error", "unknown command", "op", string(cmd.Op))
		return CommandResultError(fmt.Errorf("unknown command: %s", cmd.Op)).Encode()
	}

	result := fn(cmd, f.kvStore)
	return result.Encode()
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO: finish
	return nil, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FSM) Restore(reader io.ReadCloser) error {
	// TODO: finish
	return nil
}
