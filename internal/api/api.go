package api

import (
	"fmt"
	"strconv"

	"github.com/deejross/mydis/pkg/cluster"
	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
)

// RegisterHandlers registers all known fsm.CommandHandlers.
func RegisterHandlers(c *cluster.Cluster) {
	// read commands
	c.RegisterHandler("GET", cmdGet, false)
	// TODO:
	// DBSIZE
	// ECHO
	// EXISTS
	// GETRANGE
	// HEXISTS
	// HGET
	// HGETALL
	// HKEYS
	// HLEN
	// HMGET
	// HSTRLEN
	// INFO
	// KEYS
	// LINDEX
	// LLEN
	// LPOS
	// LRANGE
	// MGET
	// PING
	// PSUBSCRIBE
	// PTTL
	// PUNSUBSCRIBE
	// RANDOMKEY
	// ROLE
	// SINTER
	// SINTERSTORE
	// SISMEMBER
	// SMISMEMBER
	// SMEMBERS
	// SRANDMEMBER
	// STRLEN
	// SUBSCRIBE
	// TIME
	// UNSUBSCRIBE
	// UNWATCH
	// WATCH
	// SCAN
	// SSCAN
	// HSCAN

	// write commands
	c.RegisterHandler("APPEND", cmdAppend, true)
	// TODO:
	// BLPOP
	// BRPOP
	// BRPOPLPUSH / BLMOVE
	// DECR
	// DECRBY
	// DEL
	// EXPIRE
	// EXPIREAT
	// FLUSHALL / FLUSHDB
	// GETSET
	// HDEL
	// HINCRBY
	// HINCRBYFLOAT
	// HMSET
	// HSET
	// HSETNX
	// INCR
	// INCRBY
	// INCRBYFLOAT
	// LINSERT
	// LPOP
	// LPUSH
	// LPUSHX
	// LREM
	// LSET
	// LTRIM
	// MSET
	// MSETNX
	// PERSIST
	// PEXPIRE
	// PEXPIREAT
	// PSETEX
	// PUBLISH
	// RENAME
	// RENAMENX
	// RPOP
	// RPOPLPUSH
	// LMOVE
	// RPUSH
	// RPUSHX
	// SADD
	// SCARD
	// SDIFF
	// SDIFFSTORE
	// SET
	// SETNX
	// SETRANGE
	// SMOVE
	// SORT
	// SPOP
	// SREM
	// SUNION
	// SUNIONSTORE

	// unsupported commands
	// BGREWRITEAOF
	// BGSAVE
	// BITCOUNT
	// BITFIELD
	// BITOP
	// BITPOS
	// BZPOPMIN
	// BZPOPMAX
	// CLIENT *
	// CLUSTER *
	// COMMAND *
	// CONFIG *
	// DEBUG *
	// DISCARD
	// DUMP
	// EVAL
	// EVALSHA
	// EXEC
	// GEO*
	// GITBIT
	// HELLO
	// LOLWUT
	// LASTSAVE
	// MEMORY *
	// MIGRATE
	// MODULE *
	// MONITOR
	// MOVE
	// OBJECT
	// PF*
	// PUBSUB
	// QUIT
	// READONLY
	// READWRITE
	// RESET
	// RESTORE
	// SAVE
	// SCRIPT *
	// SELECT
	// SETBIT
	// SHUTDOWN
	// SLAVEOF
	// REPLICAOF
	// SLOWLOG
	// STRALGO
	// SWAPDB
	// SYNC
	// PSYNC
	// UNLINK
	// WAIT
	// Z*
	// X*
	// LATENCY *
}

func cmdAppend(cmd *fsm.Command, db storage.Store) *fsm.CommandResult {
	newLen := ""

	err := db.Update(func(t storage.Transaction) error {
		b, err := t.Get(cmd.Key)
		if err != nil {
			return err
		}

		if b == nil {
			b = []byte{}
		}

		if cmd.Args == nil || len(cmd.Args) != 1 {
			return fmt.Errorf("wrong number of arguments for 'append' command")
		}

		b = append(b, cmd.Args[0]...)

		if err := t.Set(cmd.Key, b); err != nil {
			return err
		}

		newLen = strconv.Itoa(len(b))
		return t.Commit()
	})

	if err != nil {
		return fsm.CommandResultError(err)
	}

	return fsm.CommandResultString(newLen)
}

func cmdGet(cmd *fsm.Command, db storage.Store) *fsm.CommandResult {
	val, err := db.Get(cmd.Key)
	if err != nil {
		return fsm.CommandResultError(err)
	}

	return fsm.CommandResultBytes(val)
}
