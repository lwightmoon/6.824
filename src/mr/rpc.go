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

const (
	mapPhase    = "map"
	reducePhase = "reduce"
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

type TaskArgs struct {
	Phase string
}
type TaskReply struct {
	File    string
	Phase   string
	Index   int
	NReduce int
	Nfiles  int
}

type FinishArgs struct {
	Index int
	Phase string
}

type FinishReply struct {
	Ok bool
}

type OkArgs struct {
	Phase string
}

type OkReply struct {
	Ok bool
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
