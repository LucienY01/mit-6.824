package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Reply struct {
	Act       int // indicate the action. 0: work on tasks; 1: wait; 2: all jobs done, please exit.
	MR        int // indicate the type of tasks: map or reduce.
	Filenames []string
	IMap      int
	NReduce   int
	IReduce   int
}

const (
	actWork = iota
	actWait
	actExit
)

const (
	workMap = iota
	workReduce
)

type ArgsMapDone struct {
	NumberMap int
	Filenames []string
}

type ArgsReduceDone struct {
	I int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
