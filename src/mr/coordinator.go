package mr

import (
	"log"
	"net"
	"sync"
	"time"
)
import "os"
import "net/rpc"
import "net/http"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type TaskStatus int
type TaskType int
type TaskPhase int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	MapPhase TaskPhase = iota
	ReducePhase
)

const (
	UnHandle TaskStatus = iota
	Handling
	Handled
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Filename string
}

type Coordinator struct {
	// Your definitions here.
	MapTaskMap        map[string]*Task
	ReduceTaskMap     map[string]*Task
	nReduce           int
	RWLock            sync.Mutex
	Phase             chan TaskPhase
	HandlingTaskQueue map[string]chan struct{}
	UnHandleTaskQueue chan Task
	Finish            chan bool
}

const Timeout = 10 * time.Second

// GetTask get task rpc
func (c *Coordinator) GetTask(_, reply *TaskReply) error {
	task := <-c.UnHandleTaskQueue
	reply.Task = task
	reply.NReduce = c.nReduce
	c.RWLock.Lock()
	defer c.RWLock.Unlock()
	if task.Type == MapTask {
		c.MapTaskMap[task.Filename].Status = Handling
	} else {
		c.ReduceTaskMap[task.Filename].Status = Handling
	}
	c.HandlingTaskQueue[task.Filename] = make(chan struct{})
	go c.CheckTimeoutTask(task)
	return nil
}

//MapTaskSuccess map task rpc callback
func (c *Coordinator) MapTaskSuccess(args *MapCallbackArgs, reply *MapCallbackReply) error {
	c.RWLock.Lock()
	key := args.FileName
	task := c.MapTaskMap[key]
	task.Status = Handled
	for _, filename := range args.LocalFile {
		c.ReduceTaskMap[filename] = &Task{
			Filename: filename,
			Status:   UnHandle,
			Type:     ReduceTask,
		}
	}
	mapFinish := true
	for _, task := range c.MapTaskMap {
		if task.Status != Handled {
			mapFinish = false
			break
		}
	}
	c.HandlingTaskQueue[args.FileName] <- struct{}{}
	if mapFinish {
		for _, task := range c.ReduceTaskMap {
			c.UnHandleTaskQueue <- Task{
				Filename: task.Filename,
				Status:   UnHandle,
				Type:     ReduceTask,
			}
		}
	}
	c.RWLock.Unlock()
	return nil
}

// ReduceTaskSuccess reduce task callback
func (c *Coordinator) ReduceTaskSuccess(args *ReduceCallbackArgs, reply *ReduceCallbackReply) error {
	c.RWLock.Lock()
	defer c.RWLock.Unlock()

	key := args.FileName
	c.ReduceTaskMap[key].Status = Handled

	finish := true
	for _, task := range c.ReduceTaskMap {
		if task.Status != Handled {
			finish = false
			break
		}
	}
	c.HandlingTaskQueue[args.FileName] <- struct{}{}
	if finish {
		c.Finish <- true
	}
	return nil
}

// CheckTimeoutTask check timeout task when receive getTask request
func (c *Coordinator) CheckTimeoutTask(task Task) {

	timeout := time.After(Timeout)

	select {
	case <-c.HandlingTaskQueue[task.Filename]:
		c.RWLock.Lock()
		if task.Type == MapTask {
			c.MapTaskMap[task.Filename].Status = Handled
		} else {
			c.ReduceTaskMap[task.Filename].Status = Handled
		}
		c.RWLock.Unlock()
	case <-timeout:
		c.RWLock.Lock()
		if task.Type == MapTask {
			c.MapTaskMap[task.Filename].Status = UnHandle
		} else {
			c.ReduceTaskMap[task.Filename].Status = UnHandle
		}
		c.RWLock.Unlock()
		c.UnHandleTaskQueue <- Task{
			Filename: task.Filename,
			Type:     task.Type,
			Status:   UnHandle,
		}
	}
}

// initializeTask initialize the mapTask and add into the unHandled task queue
func (c *Coordinator) initializeTask(files []string) {
	for _, file := range files {
		c.MapTaskMap[file] = &Task{
			Filename: file,
			Type:     MapTask,
			Status:   UnHandle,
		}
		c.HandlingTaskQueue[file] = make(chan struct{})
		c.UnHandleTaskQueue <- Task{
			Filename: file,
			Type:     MapTask,
			Status:   UnHandle,
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	<-c.Finish
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:           nReduce,
		MapTaskMap:        make(map[string]*Task),
		ReduceTaskMap:     make(map[string]*Task),
		HandlingTaskQueue: make(map[string]chan struct{}),
		UnHandleTaskQueue: make(chan Task, len(files)+nReduce),
		Finish:            make(chan bool),
	}
	c.initializeTask(files)
	//c.CheckTimeoutTask()
	c.server()
	return &c
}
