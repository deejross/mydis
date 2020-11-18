package api

import (
	"os"
	"testing"
	"time"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
	"github.com/deejross/mydis/pkg/cluster/storage/mem"
	"github.com/stretchr/testify/require"
)

var (
	db      storage.Store
	strKey  = []byte("string-type")
	testVal = []byte("testing")
)

func TestMain(m *testing.M) {
	db = mem.NewMemoryStore()

	code := m.Run()
	os.Exit(code)
}

func TestGetValueEmpty(t *testing.T) {
	val, err := GetValue(db, &fsm.Command{Key: []byte("does-not-exist")}, TypeString)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Nil(t, val.Contents)
}

func TestSetValue(t *testing.T) {
	cmd := &fsm.Command{Key: strKey}
	err := SetValue(db, cmd, WrapValue(testVal, TypeString))
	require.NoError(t, err)
}

func TestGetValue(t *testing.T) {
	cmd := &fsm.Command{Key: strKey}
	val, err := GetValue(db, cmd, TypeString)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotNil(t, val.Contents)
	require.Equal(t, testVal, val.Contents)
}

func TestGetValueWrongType(t *testing.T) {
	cmd := &fsm.Command{Key: strKey}
	val, err := GetValue(db, cmd, TypeList)
	require.Equal(t, ErrWrongType, err)
	require.NotNil(t, val)
}

func TestKeys(t *testing.T) {
	// TODO: this is broken
	result := cmdKeys(db, &fsm.Command{})
	require.NoError(t, result.Error())
	require.Len(t, result.Val, 1)
	require.Equal(t, testVal, result.Val[0])
}

func TestExpiration(t *testing.T) {
	cmd := &fsm.Command{Key: strKey}
	val, err := GetValue(db, cmd, TypeString)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotNil(t, val.Contents)
	require.Equal(t, testVal, val.Contents)

	// make expiration in the future
	val.Expires = uint32(time.Now().Add(10 * time.Second).Unix())
	err = SetValue(db, cmd, val)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotNil(t, val.Contents)
	require.Equal(t, testVal, val.Contents)

	// make sure it hasn't expired yet
	val, err = GetValue(db, cmd, TypeString)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotNil(t, val.Contents)
	require.Equal(t, testVal, val.Contents)

	// make expiration in the past
	val.Expires = uint32(time.Now().Add(-10 * time.Second).Unix())
	err = SetValue(db, cmd, val)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.NotNil(t, val.Contents)
	require.Equal(t, testVal, val.Contents)

	// make sure it has expired
	val, err = GetValue(db, cmd, TypeString)
	require.Nil(t, err)
	require.NotNil(t, val)
	require.Nil(t, val.Contents)
}

func TestDeleteValue(t *testing.T) {
	cmd := &fsm.Command{Key: strKey}
	err := DeleteValue(db, cmd)
	require.NoError(t, err)

	val, err := GetValue(db, cmd, TypeString)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Nil(t, val.Contents)
}
