package api

import (
	"strconv"

	"github.com/deejross/mydis/pkg/cluster/fsm"
	"github.com/deejross/mydis/pkg/cluster/storage"
)

func cmdAppend(db storage.Store, cmd *fsm.Command) *fsm.CommandResult {
	newLen := ""

	err := db.Update(func(t storage.Transaction) error {
		if cmd.Args == nil || len(cmd.Args) != 1 {
			return ErrWrongNumArgs
		}

		val, err := GetValue(db, cmd, TypeString)
		if err != nil {
			return err
		}

		if val.Contents == nil {
			val.Contents = []byte{}
		}

		val.Contents = append(val.Contents, cmd.Args[0]...)

		if err := SetValue(db, cmd, val); err != nil {
			return err
		}

		if val.Contents == nil {
			newLen = "0"
		} else {
			newLen = strconv.Itoa(len(val.Contents))
		}

		return t.Commit()
	})

	if err != nil {
		return fsm.CommandResultError(err)
	}

	return fsm.CommandResultString(newLen)
}

func cmdGet(db storage.Store, cmd *fsm.Command) *fsm.CommandResult {
	val, err := GetValue(db, cmd, TypeString)
	if err != nil {
		return fsm.CommandResultError(err)
	}

	return fsm.CommandResultBytes(val.Contents)
}

func cmdSet(db storage.Store, cmd *fsm.Command) *fsm.CommandResult {
	if cmd.Args == nil || len(cmd.Args) != 1 {
		return fsm.CommandResultError(ErrWrongNumArgs)
	}

	if err := SetValue(db, cmd, WrapValue(cmd.Args[0], TypeString)); err != nil {
		return fsm.CommandResultError(err)
	}

	return fsm.CommandResultString("OK")
}
