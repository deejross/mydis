package bbolt

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestBoltStoreImplements(t *testing.T) {
	var store interface{} = &BoltStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatal("BoltStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatal("BoltStore does not implement raft.LogStore")
	}
}

func TestNewBoltStore(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	_, err := os.Stat(store.config.Path)
	require.NoError(t, err)

	require.NoError(t, store.Close())

	db, err := bbolt.Open(store.config.Path, fileMode, nil)
	require.NoError(t, err)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	_, err = tx.CreateBucket(bucketLogs)
	require.Equal(t, bbolt.ErrBucketExists, err)

	_, err = tx.CreateBucket(bucketKV)
	require.Equal(t, bbolt.ErrBucketExists, err)
}

func TestFirstIndex(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	index, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), index)

	// create a mock Raft log
	logs := []*raft.Log{
		raftLog(1, "log1"),
		raftLog(2, "log2"),
		raftLog(3, "log3"),
	}
	require.NoError(t, store.StoreLogs(logs))

	// fetch the first Raft index
	index, err = store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
}

func TestLastIndex(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	index, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), index)

	// create a mock Raft log
	logs := []*raft.Log{
		raftLog(1, "log1"),
		raftLog(2, "log2"),
		raftLog(3, "log3"),
	}
	require.NoError(t, store.StoreLogs(logs))

	// fetch the last Raft index
	index, err = store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), index)
}

func TestGetLog(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	log := &raft.Log{}
	require.Error(t, raft.ErrLogNotFound, store.GetLog(1, log))

	// create a mock Raft log
	logs := []*raft.Log{
		raftLog(1, "log1"),
		raftLog(2, "log2"),
		raftLog(3, "log3"),
	}
	require.NoError(t, store.StoreLogs(logs))
	require.NoError(t, store.GetLog(2, log))
	require.Equal(t, logs[1], log)
}

func TestSetLog(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	require.NoError(t, store.StoreLog(log))

	result := &raft.Log{}
	require.NoError(t, store.GetLog(1, result))
	require.Equal(t, log, result)
}

func TestSetLogs(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	logs := []*raft.Log{
		raftLog(1, "log1"),
		raftLog(2, "log2"),
	}

	require.NoError(t, store.StoreLogs(logs))

	result1, result2 := &raft.Log{}, &raft.Log{}
	require.NoError(t, store.GetLog(1, result1))
	require.Equal(t, logs[0], result1)
	require.NoError(t, store.GetLog(2, result2))
	require.Equal(t, logs[1], result2)
}

func TestDeleteRange(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	// create a mock Raft log
	logs := []*raft.Log{
		raftLog(1, "log1"),
		raftLog(2, "log2"),
		raftLog(3, "log3"),
	}

	require.NoError(t, store.StoreLogs(logs))
	require.NoError(t, store.DeleteRange(1, 2))
	require.Equal(t, raft.ErrLogNotFound, store.GetLog(1, &raft.Log{}))
	require.Equal(t, raft.ErrLogNotFound, store.GetLog(2, &raft.Log{}))
}

func TestGet(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	_, err := store.Get([]byte("bad"))
	require.Equal(t, ErrKeyNotFound, err)

	k, v := []byte("hello"), []byte("world")
	require.NoError(t, store.Set(k, v))

	val, err := store.Get(k)
	require.NoError(t, err)
	require.Equal(t, v, val)
}

func TestSetGetUint64(t *testing.T) {
	store := newBoltStore(t)
	defer store.Close()
	defer os.Remove(store.config.Path)

	_, err := store.GetUint64([]byte("bad"))
	require.Equal(t, ErrKeyNotFound, err)

	k, v := []byte("hello"), uint64(42)
	require.NoError(t, store.SetUint64(k, v))

	val, err := store.GetUint64(k)
	require.NoError(t, err)
	require.Equal(t, v, val)
}

func newBoltStore(t testing.TB) *BoltStore {
	tempFile, err := ioutil.TempFile("", "bbolt")
	require.NoError(t, err)

	conf := DefaultConfig(tempFile.Name())
	store, err := NewBoltStore(conf)
	require.NoError(t, err)
	require.Equal(t, tempFile.Name(), store.config.Path)

	return store
}

func raftLog(index uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: index,
	}
}
