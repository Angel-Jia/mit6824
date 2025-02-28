package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"sync/atomic"
	// "runtime"
	// _ "net/http/pprof"
)

const (
	TaskMap    = "Map"
	TaskReduce = "Reduce"
	TaskWait   = "Wait"
)

type TaskInfo struct {
	TaskType string
	Finished bool
	Assigned bool
	TaskIdx  int
}

type TaskRunningInfo struct {
	MapInputFilePath string
	Deadline         int64
	WorkerId         int
	TaskIdx          int
	Finished         bool
	reAssigned       bool
	TaskUUID         string
}

type Coordinator struct {
	// Your definitions here.
	TasksQueue        map[string]TaskInfo
	TasksRunningQueue []TaskRunningInfo
	Stage             string
	nReduce           int
	nMap              int

	mu       sync.Mutex
	channel  chan bool
	Finished int32
	TasksRemaining int32
}

func TempMapTaskOutputFileName(workerId int, taskIdx int, reduceIdx int) string {
	return fmt.Sprintf("temp-map-%d-%d-%d", workerId, taskIdx, reduceIdx)
}

func MapTaskOutputFileName(taskIdx int, reduceIdx int) string {
	return fmt.Sprintf("map-out-%d-%d", taskIdx, reduceIdx)
}

func TempReduceTaskOutputFileName(workerId int, taskIdx int) string {
	return fmt.Sprintf("temp-reduce-%d-%d", workerId, taskIdx)
}

func ReduceTaskOutputFileName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

func mergeMapTaskOutput(workerId int, taskIdx int, nReduce int) {
	for i := 0; i < nReduce; i++ {
		file_name := TempMapTaskOutputFileName(workerId, taskIdx, i)
		new_file_name := MapTaskOutputFileName(taskIdx, i)
		err := os.Rename(file_name, new_file_name)
		if err != nil {
			panic(fmt.Errorf("fatal error: %v", err))

		}
	}
}

func mergeReduceTaskOutput(workerId int, taskIdx int, nReduce int) {
	file_name := TempReduceTaskOutputFileName(workerId, taskIdx)
	new_file_name := ReduceTaskOutputFileName(taskIdx)
	err := os.Rename(file_name, new_file_name)
	if err != nil {
		panic(fmt.Errorf("fatal error: %v", err))
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	if atomic.LoadInt32(&c.Finished) > 0 {
		reply.AllFinished = true
		return nil
	}
	reply.TaskType = TaskWait
	c.mu.Lock()
	defer c.mu.Unlock()
	for file_path, taskInfo := range c.TasksQueue {
		if !taskInfo.Assigned {

			uuid := uuid.New().String()
			reply.TaskType = taskInfo.TaskType
			reply.TaskIdx = taskInfo.TaskIdx
			reply.TaskFilePath = file_path
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			reply.TaskUUID = uuid

			c.TasksRunningQueue = append(c.TasksRunningQueue, TaskRunningInfo{
				MapInputFilePath: file_path,
				Deadline:         time.Now().Unix() + 10,
				WorkerId:         args.WorkerId,
				TaskIdx:          taskInfo.TaskIdx,
				Finished:         false,
				reAssigned:       false,
				TaskUUID:         uuid,
			})

			taskInfo.Assigned = true
			c.TasksQueue[file_path] = taskInfo
			return nil
		}
	}
	return nil
}

func (c *Coordinator) CheckTasksTimeout() {
	for {
		c.mu.Lock()
		timeNow := time.Now().Unix()
		for i := range c.TasksRunningQueue {
			runningTask := c.TasksRunningQueue[i]
			if runningTask.Deadline < timeNow && !runningTask.Finished && !runningTask.reAssigned {
				if taskInfo, ok := c.TasksQueue[runningTask.MapInputFilePath]; ok && !taskInfo.Finished {
					taskInfo.Assigned = false
					c.TasksQueue[runningTask.MapInputFilePath] = taskInfo
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) SendTaskResult(result *TaskResult, reply *ExampleReply) error {
	if result.TaskType != c.Stage {
		return fmt.Errorf("task type error")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.TasksRunningQueue {
		runningTask := c.TasksRunningQueue[i]
		if runningTask.TaskUUID == result.TaskUUID && !runningTask.Finished {
			c.TasksRunningQueue[i].Finished = true
			inputFilePath := runningTask.MapInputFilePath
			taskInfo, ok := c.TasksQueue[inputFilePath]
			if !ok {
				return fmt.Errorf("task not found")
			}
			if !taskInfo.Finished {
				taskInfo.Finished = true
				c.TasksQueue[inputFilePath] = taskInfo
				c.TasksRemaining--
				if c.TasksRemaining < 0{
					panic("fatal error: tasks remaining < 0")
				}
				if c.TasksRemaining == 0 {
					c.channel <- true
				}

				if c.Stage == TaskMap {
					go mergeMapTaskOutput(result.WorkerId, result.TaskIdx, c.nReduce)
				} else if c.Stage == TaskReduce {
					go mergeReduceTaskOutput(result.WorkerId, result.TaskIdx, c.nReduce)
				}
			}
			return nil
		}
	}
	return nil
}

func (c *Coordinator) main() {
	// go func() { http.ListenAndServe("localhost:6060", nil) }()
    // runtime.SetMutexProfileFraction(1)
    // runtime.SetBlockProfileRate(1)
	go c.CheckTasksTimeout()
	for {
		<-c.channel
		if atomic.LoadInt32(&c.TasksRemaining) > 0 {
			continue
		}
		if c.Stage == TaskMap {
			c.mu.Lock()
			tasksQueue := map[string]TaskInfo{}
			for idx := 0; idx < c.nReduce; idx++ {
				tasksQueue[strconv.Itoa(idx)] = TaskInfo{
					TaskType: TaskReduce,
					Finished: false,
					Assigned: false,
					TaskIdx:  idx,
				}
			}
			c.TasksQueue = tasksQueue
			c.TasksRunningQueue = []TaskRunningInfo{}
			c.Stage = TaskReduce
			c.TasksRemaining = int32(len(c.TasksQueue))
			c.mu.Unlock()
		} else {
			atomic.AddInt32(&c.Finished, 1)
			break
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return atomic.LoadInt32(&c.Finished) > 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.TasksQueue = make(map[string]TaskInfo)
	c.Stage = TaskMap
	c.nReduce = nReduce

	for idx, f := range files {
		c.TasksQueue[f] = TaskInfo{
			TaskType: c.Stage,
			Finished: false,
			Assigned: false,
			TaskIdx:  idx,
		}
	}
	c.TasksRunningQueue = []TaskRunningInfo{}
	c.channel = make(chan bool, 10)
	c.Finished = 0
	c.TasksRemaining = int32(len(files))
	c.nMap = len(files)

	go c.main()

	c.server()
	return &c
}
