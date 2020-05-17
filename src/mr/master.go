package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Master struct {
	TaskChain chan Task
	MapCompleted map[int]bool
	IntermediateFiles map[int][]string
	ReduceCompleted map[int]bool
	NumMap int
	NumReduce int
	cond sync.Cond
}

// master wait task execution result for 10s
// after that, assume that the worker has died
func (m* Master) TaskTimeOut(task Task) {
	countDown := time.NewTimer(time.Second * 10)
	select {
	case <- countDown.C:
		m.cond.L.Lock()
		if task.Type == MAP && !m.MapCompleted[task.Id] {
			m.TaskChain <- task
			m.cond.Broadcast()
		}
		if task.Type == REDUCE && !m.ReduceCompleted[task.Id] {
			m.TaskChain <- task
			m.cond.Broadcast()
		}
		m.cond.L.Unlock()
	}
}

func (m* Master) GetAvailableTask() *Task {
	var task *Task
	m.cond.L.Lock()
	// if there is any pending task
	for len(m.TaskChain) == 0 && len(m.ReduceCompleted) != m.NumReduce {
		m.cond.Wait()
	}

	if len(m.TaskChain) > 0 {
		newTask := <- m.TaskChain
		task = &newTask
	}
	m.cond.L.Unlock()
	return task
}

// Request handler
func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {
	reqType := args.Type
	switch reqType {
	case ASK_NEW_TASK:
		newTask := m.GetAvailableTask()
		if newTask != nil {
			reply.NewTask = *newTask
			reply.NumReduce = m.NumReduce
			go m.TaskTimeOut(*newTask)
		} else {
			// all tasks are done
			reply.NewTask = Task{CLOSE, 0, nil}
		}
	case MAP_TASK_COMPLETE:
		m.cond.L.Lock()
		m.MapCompleted[args.CompletedTaskId] = true
		for idx, fileName := range args.TaskResult {
			m.IntermediateFiles[idx] = append(m.IntermediateFiles[idx], fileName)
		}
		// check if all map tasks have finished
		if len(m.MapCompleted) == m.NumMap {
			// create reduce tasks
			for reduceTaskId, fileList := range m.IntermediateFiles {
				m.TaskChain <- Task{REDUCE, reduceTaskId, fileList}
				m.cond.Broadcast()
			}
		}
		m.cond.L.Unlock()
		reply.NewTask = Task{ACK, 0, nil}
	case REDUCE_TASK_COMPLETE:
		m.cond.L.Lock()
		m.ReduceCompleted[args.CompletedTaskId] = true
		m.cond.L.Unlock()
		reply.NewTask = Task{ACK, 0, nil}
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.cond.L.Lock()
	ret := len(m.ReduceCompleted) == m.NumReduce
	m.cond.L.Unlock()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NumMap = len(files)
	m.NumReduce = nReduce
	if m.NumMap > m.NumReduce {
		m.TaskChain = make(chan Task, m.NumMap)
	} else {
		m.TaskChain = make(chan Task, m.NumReduce)
	}
	m.ReduceCompleted = make(map[int]bool)
	m.MapCompleted = make(map[int]bool)
	m.IntermediateFiles = make(map[int][]string)

	// create all map tasks
	for idx, fileName := range files {
		m.TaskChain <- Task {MAP, idx, []string{fileName}}
	}

	m.cond = *sync.NewCond(new(sync.Mutex))

	m.server()
	return &m
}
