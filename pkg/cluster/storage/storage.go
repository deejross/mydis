package storage

import "github.com/hashicorp/raft"

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

	// Close the store.
	Close() error
}
