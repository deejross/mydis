package bbolt

import (
	"errors"
	"fmt"
	"time"

	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/deejross/mydis/pkg/encoding"
	"github.com/deejross/mydis/pkg/logger"
	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

const (
	// Permissions to use on db file when created.
	fileMode = 0600
)

var (
	// logging library.
	log = logger.New("bbolt")

	// Raft logs bucket name.
	bucketLogs = []byte("logs")

	// BucketKV is the bucket name for the KV store.
	bucketKV = []byte("kv")

	// ErrKeyNotFound indicates the given key does not exist.
	ErrKeyNotFound = errors.New("not found")
)

// Config object.
type Config struct {
	// Path to the database file.
	Path string

	// BoltOptions for specific bbolt options.
	BoltOptions *bolt.Options

	// AutoSync causes the database to call fsync after each write.
	// Setting this to false will increase performance at the cost of potential data loss
	// if process is unexpectly stopped and writes have occured since the last fsync.
	AutoSync bool

	// SyncInterval sets how often the database will call fsync.
	// This is only applicable when `AutoSync` is `false`.
	// Defaults to `5 * time.Second`.
	SyncInterval time.Duration
}

// DefaultConfig returns a new Config object with sane defaults.
func DefaultConfig(path string) Config {
	return Config{
		Path:         path,
		AutoSync:     true,
		SyncInterval: 5 * time.Second,
	}
}

// readOnly returns true if BoltOptions say to open the db in readOnly mode.
// This can be useful to tools that want to examine the log.
func (o Config) readOnly() bool {
	return o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// Transaction implements storage.Transaction.
type Transaction struct {
	*bolt.Tx
}

// Get a value.
func (t *Transaction) Get(key []byte) ([]byte, error) {
	bucket := t.Bucket(bucketKV)
	return bucket.Get(key), nil
}

// Set a value.
func (t *Transaction) Set(key, val []byte) error {
	bucket := t.Bucket(bucketKV)
	return bucket.Put(key, val)
}

// Delete a value.
func (t *Transaction) Delete(key []byte) error {
	bucket := t.Bucket(bucketKV)
	return bucket.Delete(key)
}

// BoltStore provides access to bbolt for Raft to store and retrieve log entries.
// It also provides key/value storage and can be used as a LogStore and StableStore.
type BoltStore struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// config the BoltStore was created with.
	config Config

	// closeCh exits the fsync loop when `options.AutoSync` is `false`.
	closeCh chan struct{}
}

// NewBoltStore creates a new BoltStore and prepares it for use as a Raft backend.
func NewBoltStore(options Config) (*BoltStore, error) {
	// attempt to connect to the database
	handle, err := bolt.Open(options.Path, fileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}

	handle.NoSync = !options.AutoSync

	// create the new store
	store := &BoltStore{
		conn:    handle,
		config:  options,
		closeCh: make(chan struct{}),
	}

	// if store was opened readonly, don't create buckets
	if !options.readOnly() {
		// setup the buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}

	return store, nil
}

// initialize creates the buckets and starts the fsync loop (if `AutoSync` is `false`).
func (b *BoltStore) initialize() error {
	// create the buckets
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.CreateBucketIfNotExists(bucketLogs); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(bucketKV); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// if `options.AutoSync` is `false`, setup a sync loop to call fsync at the desired interval
	if !b.config.AutoSync {
		go func() {
			for {
				select {
				case <-b.closeCh:
					return
				case <-time.After(b.config.SyncInterval):
					if err := b.conn.Sync(); err != nil {
						log.Error(fmt.Sprintf("unable to fsync: %v", err))
					}
				}
			}
		}()
	}

	return nil
}

// DB gets the underlying bbolt DB connection.
func (b *BoltStore) DB() *bbolt.DB {
	return b.conn
}

// Close gracefully closes the DB connection.
func (b *BoltStore) Close() error {
	if !b.config.AutoSync {
		if err := b.conn.Sync(); err != nil {
			return err
		}

		b.closeCh <- struct{}{}
	}

	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(bucketLogs).Cursor()
	first, _ := cursor.First()
	if first == nil {
		return 0, nil
	}

	return encoding.BytesToUint64(first), nil
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(bucketLogs).Cursor()
	last, _ := cursor.Last()
	if last == nil {
		return 0, nil
	}

	return encoding.BytesToUint64(last), nil
}

// GetLog is used to retrieve a log from bbolt at a given index.
func (b *BoltStore) GetLog(index uint64, out *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(bucketLogs)
	value := bucket.Get(encoding.Uint64ToBytes(index))
	if value == nil {
		return raft.ErrLogNotFound
	}

	return encoding.DecodeMsgPack(value, out)
}

// StoreLog is used to store a single Raft log.
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store multiple Raft logs.
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := encoding.Uint64ToBytes(log.Index)
		val, err := encoding.EncodeMsgPack(log)
		if err != nil {
			return err
		}

		bucket := tx.Bucket(bucketLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range, inclusively.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := encoding.Uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(bucketLogs).Cursor()
	for k, _ := cursor.Seek(minKey); k != nil; k, _ = cursor.Next() {
		// handle out-of-range loops
		if encoding.BytesToUint64(k) > max {
			break
		}

		// delete in-range log index
		if err := cursor.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Set is used to set a key/value set on the KV bucket.
func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(bucketKV)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the KV bucket.
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(bucketKV)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

// Delete is used to delete a value from the KV bucket.
func (b *BoltStore) Delete(k []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(bucketKV)
	if err := bucket.Delete(k); err != nil {
		return err
	}

	return tx.Commit()
}

// Update a value in the KV bucket within a read/write transaction.
func (b *BoltStore) Update(fn func(txn storage.Transaction) error) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txn := &Transaction{tx}
	return fn(txn)
}

// SetUint64 is like Set, but for uint64 values.
func (b *BoltStore) SetUint64(key []byte, u uint64) error {
	return b.Set(key, encoding.Uint64ToBytes(u))
}

// GetUint64 is like Get, but for uint64 values.
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}

	return encoding.BytesToUint64(val), nil
}

// Sync performs an fsync call. This is only applicable when `options.AutoSync` is `false`
// and a manual fsync is require outside of the `options.SyncInterval` window.
func (b *BoltStore) Sync() error {
	return b.conn.Sync()
}
