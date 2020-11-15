package fsm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandResultString(t *testing.T) {
	r := CommandResultString("testing")
	require.NotEqual(t, "testing", r.Encode())

	b := r.Encode()
	r = DecodeCommandResult(b)
	require.NoError(t, r.Error())
	require.Len(t, r.Val, 1)
	require.Equal(t, "testing", string(r.Val[0]))
}

func TestCommandResultStrings(t *testing.T) {
	r := CommandResultStrings([]string{"t1", "t2", "t3"})
	require.NotEqual(t, "testing", r.Encode())

	b := r.Encode()
	r = DecodeCommandResult(b)
	require.NoError(t, r.Error())
	require.Len(t, r.Val, 3)

	bs := [][]byte{[]byte("t1"), []byte("t2"), []byte("t3")}
	require.ElementsMatch(t, bs, r.Val)
}

func TestCommandResultError(t *testing.T) {
	r := CommandResultError(fmt.Errorf("bad thing happened"))
	require.NotEqual(t, "bad thing happened", r.Encode())

	b := r.Encode()
	r = DecodeCommandResult(b)
	require.Error(t, r.Error())
	require.Equal(t, "bad thing happened", r.Err)
}
