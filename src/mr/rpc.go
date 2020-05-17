package mr

// RPC definitions.

import "os"
import "strconv"

type ReplyType int
const (
	MAP ReplyType = iota
	REDUCE
	ACK
	CLOSE
)

type RequestType int
const (
	ASK_NEW_TASK RequestType = iota
	MAP_TASK_COMPLETE
	REDUCE_TASK_COMPLETE
)

type Task struct {
	Type     ReplyType
	Id       int
	FileList []string
}

type RequestArgs struct {
	Type            RequestType
	CompletedTaskId int
	TaskResult      []string
}

type RequestReply struct {
	NewTask Task
	NumReduce int
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
