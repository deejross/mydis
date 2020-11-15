package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/stretchr/testify/require"
)

func TestNewCluster(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dir3 := t.TempDir()

	cluster1, err := NewCluster(&Config{DataDir: dir1, BindAddress: "127.0.0.1", HTTPPort: "2380", RedisPort: "2381", RaftPort: "2382"})
	require.NoError(t, err)
	require.NotNil(t, cluster1)
	require.NotNil(t, cluster1.raft)
	require.NotNil(t, cluster1.logStore)
	require.NotNil(t, cluster1.kvStore)

	time.Sleep(2 * time.Second)

	require.True(t, cluster1.IsLeader())

	cluster2, err := NewCluster(&Config{DataDir: dir2, BindAddress: "127.0.0.1", HTTPPort: "3380", RedisPort: "3381", RaftPort: "3382", Join: "127.0.0.1:2382"})
	require.NoError(t, err)
	require.NotNil(t, cluster2)
	require.NotNil(t, cluster2.raft)
	require.NotNil(t, cluster2.logStore)
	require.NotNil(t, cluster2.kvStore)

	time.Sleep(2 * time.Second)

	cluster3, err := NewCluster(&Config{DataDir: dir3, BindAddress: "127.0.0.1", HTTPPort: "4380", RedisPort: "4381", RaftPort: "4382", Join: "127.0.0.1:3382"})
	require.NoError(t, err)
	require.NotNil(t, cluster3)
	require.NotNil(t, cluster3.raft)
	require.NotNil(t, cluster3.logStore)
	require.NotNil(t, cluster3.kvStore)

	time.Sleep(2 * time.Second)

	require.True(t, cluster1.IsLeader() || cluster2.IsLeader() || cluster3.IsLeader())
	f := cluster1.raft.GetConfiguration()
	require.NoError(t, f.Error())
	require.Len(t, f.Configuration().Servers, 3)
	require.Equal(t, "127.0.0.1:2382", string(cluster1.raft.Leader()))
	require.Equal(t, "127.0.0.1:2382", string(cluster2.raft.Leader()))
	require.Equal(t, "127.0.0.1:2382", string(cluster3.raft.Leader()))

	t.Run("kvSet", func(t *testing.T) {
		setHandler := func(cmd *fsm.Command, db storage.Store) *fsm.CommandResult {
			if cmd.Key == nil || len(cmd.Key) == 0 {
				return fsm.CommandResultError(fmt.Errorf("key cannot be empty"))
			}
			if cmd.Args == nil || len(cmd.Args) != 1 {
				return fsm.CommandResultError(fmt.Errorf("must have only one argument for value"))
			}

			result := fsm.CommandResultError(db.Set(cmd.Key, cmd.Args[0]))
			if result.Error() != nil {
				return result
			}

			// ensure the value was actually written to local storage
			b, err := db.Get(cmd.Key)
			if err != nil {
				return fsm.CommandResultError(err)
			}

			if b == nil {
				return fsm.CommandResultError(fmt.Errorf("set was successful, but could not get written value"))
			}

			return fsm.CommandResultError(nil)
		}

		cluster1.RegisterHandler("SET", setHandler, true)
		cluster2.RegisterHandler("SET", setHandler, true)
		cluster3.RegisterHandler("SET", setHandler, true)

		cmd := &fsm.Command{
			Op:  "SET",
			Key: []byte("unit-test"),
			Args: [][]byte{
				[]byte("the-value"),
			},
		}

		result := cluster2.Command(cmd)
		require.NoError(t, result.Error())
	})

	t.Run("kvGet", func(t *testing.T) {
		getHandler := func(cmd *fsm.Command, db storage.Store) *fsm.CommandResult {
			if cmd.Key == nil || len(cmd.Key) == 0 {
				return fsm.CommandResultError(fmt.Errorf("key cannot be empty"))
			}

			b, err := db.Get(cmd.Key)
			if err != nil {
				return fsm.CommandResultError(err)
			}

			return fsm.CommandResultBytes(b)
		}

		cluster1.RegisterHandler("GET", getHandler, false)
		cluster2.RegisterHandler("GET", getHandler, false)
		cluster3.RegisterHandler("GET", getHandler, false)

		cmd := &fsm.Command{
			Op:  "GET",
			Key: []byte("unit-test"),
		}

		// check that the value was written to the leader
		result := cluster1.Command(cmd)
		require.NoError(t, result.Error())
		require.NotEmpty(t, result.Val)
		require.Equal(t, []byte("the-value"), result.Val[0])

		// use ReadConsistencyLeader to validate the consistency mode
		cmd.ReadConsistencyMode = fsm.ConsistencyStrong
		result = cluster2.Command(cmd)
		require.NoError(t, result.Error())
		require.NotEmpty(t, result.Val)
		require.Equal(t, []byte("the-value"), result.Val[0])

		// use ReadConsistencyEventual and wait for replication to finish
		cmd.ReadConsistencyMode = fsm.ConsistencyEventual
		time.Sleep(100 * time.Millisecond)
		result = cluster3.Command(cmd)
		require.NoError(t, result.Error())
		require.NotEmpty(t, result.Val)
		require.Equal(t, []byte("the-value"), result.Val[0])
	})

	cluster1.Shutdown()
	cluster2.Shutdown()
	cluster3.Shutdown()
}
