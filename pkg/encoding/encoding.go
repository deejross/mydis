package encoding

import (
	"bytes"
	"encoding/binary"

	"github.com/hashicorp/go-msgpack/codec"
)

// DecodeMsgPack reverses the EncodeMsgPack operation.
func DecodeMsgPack(b []byte, out interface{}) error {
	buf := bytes.NewBuffer(b)
	handle := codec.MsgpackHandle{}
	decoder := codec.NewDecoder(buf, &handle)
	return decoder.Decode(out)
}

// EncodeMsgPack writes an object to a MsgPack-encoded `*bytes.Buffer`.
func EncodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(in)
	return buf, err
}

// BytesToUint64 converts a byte slice to an unsigned 64-bit integer.
func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Uint64ToBytes convers an unsigned 64-bit integer to a byte slice.
func Uint64ToBytes(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}
