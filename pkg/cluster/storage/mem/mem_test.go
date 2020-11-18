package mem

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBoltStoreImplements(t *testing.T) {
	var store interface{} = &MemoryStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatal("MemStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatal("MemStore does not implement raft.LogStore")
	}
}

func TestFirstIndex(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

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
	require.Equal(t, uint64(0), index)
}

func TestLastIndex(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

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
	require.Equal(t, uint64(2), index)
}

func TestGetLog(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

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
	require.Equal(t, logs[2], log)
}

func TestSetLog(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	require.NoError(t, store.StoreLog(log))

	result := &raft.Log{}
	require.NoError(t, store.GetLog(0, result))
	require.Equal(t, log, result)
}

func TestSetLogs(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	logs := []*raft.Log{
		raftLog(1, "log1"),
		raftLog(2, "log2"),
	}

	require.NoError(t, store.StoreLogs(logs))

	result1, result2 := &raft.Log{}, &raft.Log{}
	require.NoError(t, store.GetLog(0, result1))
	require.Equal(t, logs[0], result1)
	require.NoError(t, store.GetLog(1, result2))
	require.Equal(t, logs[1], result2)
}

func TestDeleteRange(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

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
	store := NewMemoryStore()
	defer store.Close()

	v, err := store.Get([]byte("bad"))
	require.Nil(t, err)
	require.Nil(t, v)

	k, v := []byte("hello"), []byte("world")
	require.NoError(t, store.Set(k, v))

	val, err := store.Get(k)
	require.NoError(t, err)
	require.Equal(t, v, val)
}

func TestSetGetUint64(t *testing.T) {
	store := NewMemoryStore()
	defer store.Close()

	v, err := store.GetUint64([]byte("bad"))
	require.Nil(t, err)
	require.Zero(t, v)

	k, v := []byte("hello"), uint64(42)
	require.NoError(t, store.SetUint64(k, v))

	val, err := store.GetUint64(k)
	require.NoError(t, err)
	require.Equal(t, v, val)
}

func raftLog(index uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: index,
	}
}
