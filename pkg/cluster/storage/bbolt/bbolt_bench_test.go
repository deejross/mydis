package bbolt

import (
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkFirstIndex(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkLastIndex(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.LastIndex(b, store)
}

func BenchmarkGetLog(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.GetLog(b, store)
}

func BenchmarkStoreLog(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.StoreLog(b, store)
}

func BenchmarkStoreLogs(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkDeleteRange(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkSet(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.Set(b, store)
}

func BenchmarkGet(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.Get(b, store)
}

func BenchmarkSetUint64(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.SetUint64(b, store)
}

func BenchmarkGetUint64(b *testing.B) {
	store := newBoltStore(b)
	defer store.Close()
	defer os.Remove(store.config.Path)

	raftbench.GetUint64(b, store)
}
