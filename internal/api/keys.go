package api

import (
	"bytes"
	"fmt"
	"time"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/deejross/mydis/pkg/encoding"
)

// Key object. DBIndex allows for multi-tenancy, MetaType will be `0` for
// client-facing keys. This allows us to store metadata keys alongside the
// parent key.
type Key struct {
	DBIndex  uint16
	MetaType byte
	Name     []byte
}

// ParseKey parses the key into a DB index and key name.
func ParseKey(key []byte) (*Key, error) {
	if len(key) < 4 {
		return nil, ErrKeySize
	}

	k := &Key{
		DBIndex:  encoding.BytesToUint16(key[0:2]),
		MetaType: key[2],
		Name:     key[3:],
	}

	return k, nil
}

// KeyFromCommand creates a Key from a Command.
func KeyFromCommand(cmd *fsm.Command) *Key {
	return &Key{
		DBIndex:  cmd.DBIndex,
		MetaType: TypeNone,
		Name:     cmd.Key,
	}
}

// ToBytes formats the Key into a byte slice for use as a storage key.
func (k *Key) ToBytes() []byte {
	buf := &bytes.Buffer{}
	buf.Write(encoding.Uint16ToBytes(k.DBIndex))
	buf.WriteByte(k.MetaType)
	buf.Write(k.Name)
	return buf.Bytes()
}

// Value object wrapped with required metadata.
type Value struct {
	Version  byte
	DataType byte
	Created  uint32
	Updated  uint32
	Expires  uint32
	Contents []byte
}

// WrapValue creates a new Value with existing content.
func WrapValue(content []byte, dataType byte) *Value {
	return &Value{
		Contents: content,
		DataType: dataType,
	}
}

// ParseValue parses the value into a metadata/value object.
func ParseValue(val []byte) (*Value, error) {
	if len(val) < 14 {
		return nil, ErrValueSize
	}

	if val[0] != ValueMetaVersion1 {
		return nil, fmt.Errorf("unknown value metadata version: %s", string(val[0]))
	}

	v := &Value{
		Version:  val[0],
		DataType: val[1],
		Created:  encoding.BytesToUint32(val[2:6]),
		Updated:  encoding.BytesToUint32(val[6:10]),
		Expires:  encoding.BytesToUint32(val[10:14]),
	}

	if len(val) > 14 {
		v.Contents = val[14:]
	}

	return v, nil
}

// ToBytes formats the Value to a byte string.
func (v *Value) ToBytes() []byte {
	if v.Version == TypeNone {
		v.Version = ValueMetaVersion1
	}

	v.Updated = uint32(time.Now().Unix())
	if v.Created == 0 {
		v.Created = v.Updated
	}

	if v.Contents == nil {
		v.Contents = []byte{}
	}

	buf := &bytes.Buffer{}
	buf.WriteByte(v.Version)
	buf.WriteByte(v.DataType)
	buf.Write(encoding.Uint32ToBytes(v.Created))
	buf.Write(encoding.Uint32ToBytes(v.Updated))
	buf.Write(encoding.Uint32ToBytes(v.Expires))
	buf.Write(v.Contents)
	return buf.Bytes()
}

// Expired determins if the Value has expired.
func (v *Value) Expired() bool {
	if v.Expires > 0 {
		expires := time.Unix(int64(v.Expires), 0)
		if expires.Before(time.Now()) {
			return true
		}
	}

	return false
}

// GetValue gets a Value from the given Command and validates it is the correct type and has not expired.
func GetValue(db storage.Store, cmd *fsm.Command, dataType byte) (*Value, error) {
	k := KeyFromCommand(cmd)
	b, err := db.Get(k.ToBytes())
	if err != nil {
		return nil, err
	}

	var val *Value
	if b == nil {
		val = WrapValue(nil, dataType)
	} else {
		val, err = ParseValue(b)
		if err != nil {
			return nil, err
		}
	}

	if dataType == TypeNone || val.DataType != dataType {
		return val, ErrWrongType
	}

	if val.Expired() {
		return WrapValue(nil, dataType), nil
	}

	return val, nil
}

// SetValue sets a Value from the given Command.
func SetValue(db storage.Store, cmd *fsm.Command, val *Value) error {
	k := KeyFromCommand(cmd)
	return db.Set(k.ToBytes(), val.ToBytes())
}

// DeleteValue deletes a Value from the store.
func DeleteValue(db storage.Store, cmd *fsm.Command) error {
	k := KeyFromCommand(cmd)
	return db.Delete(k.ToBytes())
}

// Keys returns all known keys.
func cmdKeys(db storage.Store, cmd *fsm.Command) *fsm.CommandResult {
	keys := [][]byte{}

	db.Cursor(func(c storage.Cursor) {
		for {
			k, v := c.Next()
			if k == nil {
				break
			}

			key, err := ParseKey(k)
			if err != nil {
				log.Error("unable to parse key name: %v", err)
				continue
			}

			if key.DBIndex != cmd.DBIndex || key.MetaType != TypeNone {
				break
			}

			val, err := ParseValue(v)
			if err != nil {
				log.Error("unable to parse value for key: %s: %v", string(key.Name), err)
				continue
			}

			if val.Expired() {
				continue
			}

			keys = append(keys, key.Name)
		}
	})

	return &fsm.CommandResult{
		Val: keys,
	}
}
