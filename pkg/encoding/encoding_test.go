package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMsgPack(t *testing.T) {
	b := []byte("test message")
	buf, err := EncodeMsgPack(b)
	t.Log(buf.Bytes())
	require.NoError(t, err)

	decodedB := []byte{}
	err = DecodeMsgPack(buf.Bytes(), &decodedB)
	require.NoError(t, err)
	require.Equal(t, b, decodedB)
}

func TestUint64(t *testing.T) {
	u := uint64(42)
	b := Uint64ToBytes(u)
	decodedU := BytesToUint64(b)
	require.Equal(t, u, decodedU)
}
