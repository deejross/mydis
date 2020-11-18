package mem

import (
	"bufio"
	"bytes"
	"io"
	"sync"

	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/deejross/mydis/pkg/encoding"
	"github.com/hashicorp/raft"
)

// Cursor implements storage.Cursor.
type Cursor struct {
	*MemoryStore
	p int
}

// NewCursor creates a new Cursor.
func NewCursor(m *MemoryStore) *Cursor {
	return &Cursor{
		MemoryStore: m,
		p:           -1,
	}
}

// Next value.
func (c *Cursor) Next() (key, val []byte) {
	if c.p >= len(c.v)-1 {
		return nil, nil
	}

	c.p++
	k := c.k[c.p]
	v := c.v[c.p]
	return k, v
}

// Prev value.
func (c *Cursor) Prev() (key, val []byte) {
	if c.p == 0 {
		return nil, nil
	}

	c.p--
	k := c.k[c.p]
	v := c.v[c.p]
	return k, v
}

// Seek value.
func (c *Cursor) Seek(k []byte) (key, val []byte) {
	for i, b := range c.k {
		if bytes.HasPrefix(b, k) {
			c.p = i
			return c.k[i], c.v[i]
		}
	}

	return nil, nil
}

// Transaction implements storage.Transaction.
type Transaction struct {
	*MemoryStore
}

// Get a value.
func (t *Transaction) Get(key []byte) ([]byte, error) {
	return t.Get(key)
}

// Set a value.
func (t *Transaction) Set(key, val []byte) error {
	return t.Set(key, val)
}

// Delete a value.
func (t *Transaction) Delete(key []byte) error {
	return t.Delete(key)
}

// Commit noop.
func (t *Transaction) Commit() error {
	return nil
}

// Rollback noop.
func (t *Transaction) Rollback() error {
	return nil
}

// MemoryStore only storage for unit testing.
type MemoryStore struct {
	k  [][]byte
	v  [][]byte
	mu sync.Mutex
}

// NewMemoryStore creates a new Memory object.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		k: [][]byte{},
		v: [][]byte{},
	}
}

// Close gracefully closes the DB connection.
func (m *MemoryStore) Close() error {
	return nil
}

// FirstIndex returns the first known index from the Raft log.
func (m *MemoryStore) FirstIndex() (uint64, error) {
	return 0, nil
}

// LastIndex returns the last known index from the Raft log.
func (m *MemoryStore) LastIndex() (uint64, error) {
	index := uint64(len(m.k))
	if index > 0 {
		// offset the length but don't underflow the integer
		index--
	}
	return index, nil
}

// GetLog is used to retrieve a log from memory at a given index.
func (m *MemoryStore) GetLog(index uint64, out *raft.Log) error {
	if index < 0 || index+1 > uint64(len(m.v)) {
		return raft.ErrLogNotFound
	}

	value := m.v[index]
	return encoding.DecodeMsgPack(value, out)
}

// StoreLog is used to store a single Raft log.
func (m *MemoryStore) StoreLog(log *raft.Log) error {
	return m.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store multiple Raft logs.
func (m *MemoryStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		key := encoding.Uint64ToBytes(log.Index)
		val, err := encoding.EncodeMsgPack(log)
		if err != nil {
			return err
		}

		m.k = append(m.k, key)
		m.v = append(m.v, val.Bytes())
	}

	return nil
}

// DeleteRange is used to delete logs within a given range, inclusively.
func (m *MemoryStore) DeleteRange(min, max uint64) error {
	for i := max; i >= min; i-- {
		m.delete(int(i))
	}

	return nil
}

// Set is used to set a key/value set on the KV bucket.
func (m *MemoryStore) Set(k, v []byte) error {
	for i, b := range m.k {
		if bytes.Equal(b, k) {
			m.v[i] = v
			return nil
		}
	}

	m.k = append(m.k, k)
	m.v = append(m.v, v)

	return nil
}

// Get is used to retrieve a value from the KV bucket.
func (m *MemoryStore) Get(k []byte) ([]byte, error) {
	for i, b := range m.k {
		if bytes.Equal(b, k) {
			return m.v[i], nil
		}
	}

	return nil, nil
}

// Delete is used to delete a value from the KV bucket.
func (m *MemoryStore) Delete(k []byte) error {
	for i, b := range m.k {
		if bytes.Equal(b, k) {
			m.delete(i)
			return nil
		}
	}

	return nil
}

func (m *MemoryStore) delete(i int) {
	m.k = append(m.k[:i], m.k[i+1:]...)
	m.v = append(m.v[:i], m.v[i+1:]...)
}

// Update a value in the KV bucket within a read/write transaction.
func (m *MemoryStore) Update(fn func(txn storage.Transaction) error) error {
	txn := &Transaction{m}
	return fn(txn)
}

// View a value in the KV bucket within a read transaction.
func (m *MemoryStore) View(fn func(txn storage.Transaction) error) error {
	txn := &Transaction{m}
	return fn(txn)
}

// Cursor returns an iterator used for searching and discovering keys.
func (m *MemoryStore) Cursor(fn func(c storage.Cursor)) error {
	fn(NewCursor(m))
	return nil
}

// SetUint64 is like Set, but for uint64 values.
func (m *MemoryStore) SetUint64(key []byte, u uint64) error {
	return m.Set(key, encoding.Uint64ToBytes(u))
}

// GetUint64 is like Get, but for uint64 values.
func (m *MemoryStore) GetUint64(key []byte) (uint64, error) {
	val, err := m.Get(key)
	if err != nil || val == nil {
		return 0, err
	}

	return encoding.BytesToUint64(val), nil
}

// Sync noop.
func (m *MemoryStore) Sync() error {
	return nil
}

// Snapshot writes a snapshot of the store to an io.Writer.
func (m *MemoryStore) Snapshot(w io.WriteCloser) error {
	for i, b := range m.k {
		w.Write(b)
		w.Write([]byte{0})
		w.Write(m.v[i])
		w.Write([]byte{0})
	}

	return nil
}

// Restore a snapshot.
func (m *MemoryStore) Restore(r io.ReadCloser) error {
	m.k = [][]byte{}
	m.v = [][]byte{}

	for {
		k, err := bufio.NewReader(r).ReadBytes(byte(0))
		if err == io.EOF || k == nil {
			return nil
		} else if err != nil {
			return err
		}

		v, err := bufio.NewReader(r).ReadBytes(byte(0))
		if err == io.EOF || k == nil {
			return io.ErrUnexpectedEOF
		} else if err != nil {
			return err
		}

		m.k = append(m.k, k)
		m.v = append(m.v, v)
	}
}
