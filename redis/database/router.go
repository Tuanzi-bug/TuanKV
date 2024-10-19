package database

type command struct {
	name     string
	executor ExecFunc
	//// prepare returns related keys command
	//prepare PreFunc
	//// undo generates undo-log before command actually executed, in case the command needs to be rolled back
	//undo UndoFunc
	// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
	// for example: the arity of `get` is 2, `mget` is -2
	arity int
	flags int
	extra *commandExtra
}
type commandExtra struct {
	signs    []string
	firstKey int
	lastKey  int
	keyStep  int
}
