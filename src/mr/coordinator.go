package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"fmt"
	"sync"
	"time"
)

const (
	TaskMap    = "Map"
	TaskReduce = "Reduce"
)

type TaskInfo struct {
	TaskType       string
	Finished       bool
	Assigned       bool
	TaskIdx        int
}

type TaskRunningInfo struct {
	MapInputFilePath  string
	Deadline       int64
	WorkerId       int
	TaskIdx         int
	Finished       bool
	reAssigned     bool
}

type Coordinator struct {
	// Your definitions here.
	TasksQueue        map[string]TaskInfo
	TasksRunningQueue []TaskRunningInfo
	Stage             string
	nReduce           int

	mu       sync.Mutex
	channel  chan bool
	Finished bool
}


func TempMapTaskOutputFileName(workerId int, taskIdx int, reduceIdx int) string {
	return fmt.Sprintf("temp-map-%d-%d-%d", workerId, taskIdx, reduceIdx)
}

func MapTaskOutputFileName(taskIdx int, reduceIdx int) string {
	return fmt.Sprintf("map-%d-%d", taskIdx, reduceIdx)
}

func TempReduceTaskOutputFileName(workerId int, taskIdx int) string {
	return fmt.Sprintf("temp-reduce-%d-%d-%d", workerId, taskIdx)
}

func ReduceTaskOutputFileName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

func mergeMapTaskOutput(workerId int, taskIdx int, nReduce int){
	for i := 0; i < nReduce; i++{
		file_name := TempMapTaskOutputFileName(workerId, taskIdx, i)
		new_file_name := MapTaskOutputFileName(taskIdx, i)
		err := os.Rename(file_name, new_file_name)
		if err != nil {
			panic(fmt.Errorf("Fatal error: %v", err))
			
		}
	}
}

func mergeReduceTaskOutput(workerId int, taskIdx int, nReduce int){
	file_name := TempReduceTaskOutputFileName(workerId, taskIdx)
	new_file_name := ReduceTaskOutputFileName(taskIdx)
	err := os.Rename(file_name, new_file_name)
	if err != nil {
		panic(fmt.Errorf("Fatal error: %v", err))
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
	c.mu.Lock()
	for file_path, taskInfo := range c.TasksQueue {
		if !taskInfo.Assigned {

			reply.TaskType = taskInfo.TaskType
			reply.TaskIdx = taskInfo.TaskIdx
			reply.TaskFilePath = file_path

			c.TasksRunningQueue = append(c.TasksRunningQueue, TaskRunningInfo{
				MapInputFilePath:  file_path,
				Deadline:       time.Now().Unix() + 10,
				WorkerId:       args.WorkerId,
				TaskIdx:        taskInfo.TaskIdx,
				Finished:       false,
				reAssigned:     false,
			})
			return nil
		}
	}
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) CheckTasksTimeout() {
	for {
		if len(c.TasksRunningQueue) > 0 {
			c.mu.Lock()
			for i := range c.TasksRunningQueue {
				runningTask := c.TasksRunningQueue[i]
				if runningTask.Deadline > time.Now().Unix() && !runningTask.Finished && !runningTask.reAssigned {
					if taskInfo, ok := c.TasksQueue[runningTask.MapInputFilePath]; ok {
						taskInfo.Assigned = false
						c.TasksQueue[runningTask.MapInputFilePath] = taskInfo
					}
				}
			}
			c.mu.Unlock()
		}
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) SendTaskResult(result TaskResult) {
	if result.TaskType != c.Stage {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.TasksRunningQueue {
		runningTask := c.TasksRunningQueue[i]
		if runningTask.TaskIdx == result.TaskIdx && runningTask.WorkerId == result.WorkerId && !runningTask.Finished {
			c.TasksRunningQueue[i].Finished = true
			inputFilePath := runningTask.MapInputFilePath
			taskInfo, ok := c.TasksQueue[inputFilePath]
			if !ok {
				log.Fatal("Task not found: ", inputFilePath)
				return
			}
			if !taskInfo.Finished {
				taskInfo.Finished = true
				c.TasksQueue[inputFilePath] = taskInfo

				if c.Stage == TaskMap {
					go mergeMapTaskOutput(result.WorkerId, result.TaskIdx, c.nReduce)
				}else if c.Stage == TaskReduce {
					go mergeReduceTaskOutput(result.WorkerId, result.TaskIdx, c.nReduce)
				}
				c.channel <- true
			}
			return
		}
	}
}

func (c *Coordinator) main() {
	go c.CheckTasksTimeout()
	for {
		<-c.channel
		allFinished := true
		c.mu.Lock()
		for _, taskInfo := range c.TasksQueue {
			if !taskInfo.Finished {
				allFinished = false
				break
			}
		}
		c.mu.Unlock()
		if !allFinished {
			continue
		}
		if c.Stage == TaskMap {
			c.mu.Lock()
			tasksQueue := map[string]TaskInfo{}
			for idx, _ := range c.TasksQueue {
				tasksQueue[string(idx)] = TaskInfo{
					TaskType:       TaskReduce,
					Finished:       false,
					Assigned:       false,
				}
			}
			c.TasksQueue = tasksQueue
			c.TasksRunningQueue = []TaskRunningInfo{}
			c.Stage = TaskReduce
			c.mu.Unlock()
		} else {
			c.Finished = true
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Finished
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
			TaskType:       c.Stage,
			Finished:       false,
			Assigned:       false,
			TaskIdx:        idx,
		}
	}
	c.TasksRunningQueue = []TaskRunningInfo{}
	c.channel = make(chan bool)
	c.Finished = false

	go c.main()

	c.server()
	return &c
}
