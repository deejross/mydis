package storage

import (
	"io"

	"github.com/hashicorp/raft"
)

// Cursor allows keys to be discovered.
type Cursor interface {
	// Next gets the next key.
	Next() (key, val []byte)

	// Prev gets the previous key.
	Prev() (key, val []byte)

	// Seek to the given key.
	Seek(k []byte) (key, val []byte)
}

// Transaction allows multiple read/writes for atomic operations.
type Transaction interface {
	// Get a value by key.
	Get(key []byte) ([]byte, error)

	// Set a value by key.
	Set(key, val []byte) error

	// Delete a key.
	Delete(key []byte) error

	// Commit the transaction.
	Commit() error

	// Rollback the transaction.
	Rollback() error
}

// Store is the interface implemented in storage backends.
type Store interface {
	raft.LogStore
	raft.StableStore

	// Get a value by key.
	Get(key []byte) ([]byte, error)

	// Set a value by key.
	Set(key, val []byte) error

	// Delete a key.
	Delete(key []byte) error

	// Update a value within a read/write transaction.
	Update(fn func(txn Transaction) error) error

	// View a value within a read-only transaction.
	View(fn func(txn Transaction) error) error

	// Cursor returns a key iterator to allow for searching and discovering keys.
	Cursor(fn func(c Cursor)) error

	// Close the store.
	Close() error

	// Snapshot writes a snapshot of the store to an io.WriteCloser.
	Snapshot(w io.WriteCloser) error

	// Restore a snapshot from an io.ReadCloser.
	Restore(r io.ReadCloser) error
}
