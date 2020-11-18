package api

import "github.com/deejross/mydis/pkg/cluster"

// RegisterHandlers registers all known fsm.CommandHandlers.
func RegisterHandlers(c *cluster.Cluster) {
	// read commands
	c.RegisterHandler("GET", cmdGet, false)
	c.RegisterHandler("KEYS", cmdKeys, false)
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
	c.RegisterHandler("SET", cmdSet, true)
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
