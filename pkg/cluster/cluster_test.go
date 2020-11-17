package cluster

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/stretchr/testify/require"
)

func GetTestCluster(t *testing.T, dirs ...string) []*Cluster {
	if dirs == nil || len(dirs) == 0 {
		require.FailNow(t, "must provide at least one data directory")
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	instances := []*Cluster{}
	address := "127.0.0.1"
	basePort := 2380

	for i, d := range dirs {
		wg.Add(1)

		go func(i int, d string) {
			defer wg.Done()

			httpPort := basePort + i*1000
			redisPort := basePort + i*1000 + 1
			raftPort := basePort + i*1000 + 2
			joinAddr := ""

			if i > 0 {
				// always join the previously created instance
				//joinAddr = address + ":" + strconv.Itoa(basePort+(i-1)*1000+2)
				joinAddr = address + ":" + strconv.Itoa(basePort+2)
			}

			instance, err := NewCluster(&Config{
				DataDir:     d,
				BindAddress: address,
				HTTPPort:    strconv.Itoa(httpPort),
				RedisPort:   strconv.Itoa(redisPort),
				RaftPort:    strconv.Itoa(raftPort),
				Join:        joinAddr,
			})

			require.NoError(t, err)
			require.NotNil(t, instance)
			require.NotNil(t, instance.raft)
			require.NotNil(t, instance.logStore)
			require.NotNil(t, instance.kvStore)

			t.Log(fmt.Sprintf("created instance: %s with join address: %s", address+":"+strconv.Itoa(raftPort), joinAddr))

			mu.Lock()
			instances = append(instances, instance)
			mu.Unlock()
		}(i, d)

		time.Sleep(time.Second)
	}

	// wait for the cluster to form
	wg.Wait()
	time.Sleep(5 * time.Second)

	// determine the leader
	var leader *Cluster
	for _, instance := range instances {
		if instance.IsLeader() {
			require.Nil(t, leader)
			leader = instance
		}
	}
	require.NotNil(t, leader)

	// ensure the cluster was formed with all expected instances
	raftServers, err := leader.Instances()
	require.NoError(t, err)
	require.Len(t, raftServers, len(instances))

	// ensure all instances report the same leader address
	leaderAddr := leader.LeaderAddress()
	for _, instance := range instances {
		require.Equal(t, leaderAddr, instance.LeaderAddress())
	}

	return instances
}

func TestNewCluster(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dir3 := t.TempDir()

	instances := GetTestCluster(t, dir1, dir2, dir3)

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

		for _, instance := range instances {
			instance.RegisterHandler("SET", setHandler, true)
		}

		cmd := &fsm.Command{
			Op:  "SET",
			Key: []byte("unit-test"),
			Args: [][]byte{
				[]byte("the-value"),
			},
		}

		result := instances[1].Command(cmd)
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

		for _, instance := range instances {
			instance.RegisterHandler("GET", getHandler, false)
		}

		cmd := &fsm.Command{
			Op:  "GET",
			Key: []byte("unit-test"),
		}

		// check that the value was written to the leader
		result := instances[0].Command(cmd)
		require.NoError(t, result.Error())
		require.NotEmpty(t, result.Val)
		require.Equal(t, []byte("the-value"), result.Val[0])

		// use ReadConsistencyLeader to validate the consistency mode
		cmd.ReadConsistencyMode = fsm.ConsistencyStrong
		result = instances[1].Command(cmd)
		require.NoError(t, result.Error())
		require.NotEmpty(t, result.Val)
		require.Equal(t, []byte("the-value"), result.Val[0])

		// use ReadConsistencyEventual and wait for replication to finish
		cmd.ReadConsistencyMode = fsm.ConsistencyEventual
		time.Sleep(100 * time.Millisecond)
		result = instances[2].Command(cmd)
		require.NoError(t, result.Error())
		require.NotEmpty(t, result.Val)
		require.Equal(t, []byte("the-value"), result.Val[0])
	})

	for _, instance := range instances {
		instance.Shutdown()
	}

	// wait for a couple of seconds for all instances to shut down
	time.Sleep(2 * time.Second)

	// attempt to rebuild the cluster
	t.Run("rebuildCluster", func(t *testing.T) {
		instances := GetTestCluster(t, dir1, dir2, dir3)

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

			for _, instance := range instances {
				instance.RegisterHandler("GET", getHandler, false)
			}

			cmd := &fsm.Command{
				Op:  "GET",
				Key: []byte("unit-test"),
			}

			// check that the value still exists after rebuilding the cluster
			result := instances[0].Command(cmd)
			require.NoError(t, result.Error())
			require.NotEmpty(t, result.Val)
			require.Equal(t, []byte("the-value"), result.Val[0])
		})

		for _, instance := range instances {
			instance.Shutdown()
		}
	})
}
