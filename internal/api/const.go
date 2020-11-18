package api

import (
	"fmt"

	"github.com/deejross/mydis/pkg/logger"
)

const (
	// MaxDatabaseIndex is the max database index allowed.
	MaxDatabaseIndex = 16383

	// MaxKeySize is the max key size allowed.
	MaxKeySize = 1024

	// MaxHashFieldSize is the max size for a hash field.
	MaxHashFieldSize = 1024

	// MaxZSetMemberSize is the ZSet max member size.
	MaxZSetMemberSize = 1024

	// MaxSetMemberSize is the Set max member size.
	MaxSetMemberSize = 1024

	// MaxValueSize is the max size for values.
	MaxValueSize = 1024 * 1024 * 1024

	// TypeNone is a value with no type.
	TypeNone byte = 0

	// TypeString is the string type.
	TypeString byte = 1

	// TypeHash is the hash type.
	TypeHash byte = 2

	// TypeHSize is the hash size type.
	TypeHSize byte = 3

	// TypeList is the list type.
	TypeList byte = 4

	// TypeLMeta is the list metadata type.
	TypeLMeta byte = 5

	// TypeZSet is the sorted set type.
	TypeZSet byte = 6

	// TypeZSize = is the sorted set size type.
	TypeZSize byte = 7

	// TypeZScore is the sorted set score type.
	TypeZScore byte = 8

	// TypeSet is the unordered set type.
	TypeSet byte = 9

	// MaxDataType is the maximum data type index allowed.
	MaxDataType byte = 100

	// TypeExpireMeta is the expiration metadata type.
	TypeExpireMeta byte = 101

	// TypeExpireTime is the expiration time type.
	TypeExpireTime byte = 102

	// MaxMetaType is the maximum metadata type index allowed.
	MaxMetaType byte = 200

	// TypeMeta is the general metadata type.
	TypeMeta byte = 201

	// ValueMetaVersion1 is version 1 of the value metadata format.
	ValueMetaVersion1 byte = 1
)

// TypeName is the map of types to name strings.
var TypeName = map[byte]string{
	TypeString:     "string",
	TypeHash:       "hash",
	TypeHSize:      "hsize",
	TypeList:       "list",
	TypeLMeta:      " lmeta",
	TypeZSet:       "zset",
	TypeZSize:      "zsize",
	TypeZScore:     "zscore",
	TypeSet:        "set",
	TypeExpireMeta: "expmeta",
	TypeExpireTime: "exptime",
}

var (
	// ErrKeySize is returned when the key size is invalid.
	ErrKeySize = fmt.Errorf("invalid key size")

	// ErrScoreMissed is returned when there is a ZSET score miss.
	ErrScoreMissed = fmt.Errorf("zset score miss")

	// ErrValueSize is returned when the value size is invalid.
	ErrValueSize = fmt.Errorf("invalid value size")

	// ErrWrongNumArgs is returned when the number of arguments passed to a command is incorrect.
	ErrWrongNumArgs = fmt.Errorf("wrong number of arguments for operation")

	// ErrWrongType is returned when performing an operation on a key with an unexpected type.
	ErrWrongType = fmt.Errorf("operation against a key holding the wrong kind of value")
)

var log = logger.New("internal/api")
