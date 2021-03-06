package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type DoneArgs struct {
	TaskType int // 0 means a map is done, 1 means a reduce is done
	Id       int // which task are you
}

type DoneReply struct {
	MapDone    int
	ReduceDone int
}

type TaskArgs struct {
	// not sure what arg should be used
	Key string
}

type TaskReply struct {
	M, R       int
	TaskType   int    // 0 means map task, 1 means reduce task, -1 means all is done and worker should exit
	Id         int    // identifier
	InputFiles string // files for map or reduce
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
