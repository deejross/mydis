package api

import (
	"strconv"
	"testing"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/stretchr/testify/require"
)

func TestGetEmpty(t *testing.T) {
	val, err := GetValue(db, &fsm.Command{Key: []byte("does-not-exist")}, TypeString)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Nil(t, val.Contents)
}

func TestSet(t *testing.T) {
	cmd := &fsm.Command{Key: strKey, Args: [][]byte{testVal}}
	result := cmdSet(db, cmd)
	require.NoError(t, result.Error())
	require.Equal(t, "OK", result.ValString())
}

func TestGet(t *testing.T) {
	cmd := &fsm.Command{Key: strKey}
	result := cmdGet(db, cmd)
	require.NoError(t, result.Error())
	require.Len(t, result.Val, 1)
	require.Equal(t, testVal, result.Val[0])
}

func TestAppend(t *testing.T) {
	appenedVal := append(testVal, []byte(" stuff")...)

	cmd := &fsm.Command{Key: strKey, Args: [][]byte{[]byte(" stuff")}}
	result := cmdAppend(db, cmd)
	require.NoError(t, result.Error())
	require.Len(t, result.Val, 1)
	require.Equal(t, strconv.Itoa(len(appenedVal)), result.ValString())

	cmd = &fsm.Command{Key: strKey}
	result = cmdGet(db, cmd)
	require.NoError(t, result.Error())
	require.Len(t, result.Val, 1)
	require.Equal(t, appenedVal, result.Val[0])
}
