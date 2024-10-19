package database

import (
	"github.com/Tuanzi-bug/TuanKV/datastruct/dict"
	"github.com/Tuanzi-bug/TuanKV/redis/interface/redis"
)

type DB struct {
	data *dict.SimpleDict
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine
