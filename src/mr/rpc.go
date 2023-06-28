package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskReply struct {
	Task    Task
	Done    bool
	NReduce int
}

type MapCallbackArgs struct {
	FileName  string
	LocalFile []string
}
type MapCallbackReply struct {
	Done bool
}

type ReduceCallbackArgs struct {
	FileName string
}
type ReduceCallbackReply struct {
	Done bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
