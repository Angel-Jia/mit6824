package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeTempMapTaskOutputFile(pid int, taskIdx int, nReduce int, m map[int][]KeyValue) error {
	for i := 0; i < nReduce; i++ {
		f, err := os.Create(TempMapTaskOutputFileName(pid, taskIdx, i))
		if err != nil {
			fmt.Println("Fatal error:", err)
			return err
		}
		for j := range m[i] {
			fmt.Fprintf(f, "%v %v\n", m[i][j].Key, m[i][j].Value)
		}
		f.Close()
	}
	return nil
}

func reduceTaskReadInputFile(nMap int, taskIdx int) ([]KeyValue, error) {
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		fileContent, err := os.ReadFile(MapTaskOutputFileName(i, taskIdx))
		if err != nil {
			fmt.Println("Fatal error in worker: ", err)
			return []KeyValue{}, err
		}
		fileContentLines := strings.Split(string(fileContent), "\n")
		for i := 0; i < len(fileContentLines); i++ {
			line := strings.TrimSpace(fileContentLines[i])
			if len(line) == 0 {continue}
			idx := strings.Index(line, " ")
			if idx == -1 {continue}
			key := line[:idx]
			value := line[idx+1:]
			kvs = append(kvs, KeyValue{Key: key, Value: value})
		}
	}
	return kvs, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	pid := os.Getpid()
	fmt.Println("worker pid: ", pid , " start!")
	defer fmt.Println("worker pid: ", pid , " end!")

	retryCount := 0

	for {
		args := ApplyTaskArgs{WorkerId: pid}
		reply := ApplyTaskReply{}

		// send the RPC request, wait for the reply.
		ret := call("Coordinator.ApplyTask", &args, &reply)
		if !ret {
			retryCount += 1
			if retryCount >= 2 {
				break
			}
			time.Sleep(time.Millisecond * 50)
			continue
		}

		retryCount = 0
		if reply.AllFinished {
			break
		}

		if reply.TaskType == TaskMap {
			//读取文件
			fileContent, err := os.ReadFile(reply.TaskFilePath)
			if err != nil {
				fmt.Println(err)
				continue
			}

			kva := mapf(reply.TaskFilePath, string(fileContent))

			kvaMap := map[int][]KeyValue{}
			for i := range kva {
				key := kva[i].Key
				hashKey := ihash(key) % reply.NReduce
				kvaMap[hashKey] = append(kvaMap[hashKey], kva[i])
			}
			err = writeTempMapTaskOutputFile(os.Getpid(), reply.TaskIdx, reply.NReduce, kvaMap)
			if err != nil {
				fmt.Println("Fatal error in worker: ", err)
				continue
			}
			result := TaskResult{TaskType: reply.TaskType, TaskUUID: reply.TaskUUID, WorkerId: pid, TaskIdx: reply.TaskIdx, InputFilePath: reply.TaskFilePath}
			reply := ExampleReply{}
			call("Coordinator.SendTaskResult", &result, &reply)
		} else if reply.TaskType == TaskReduce {
			kvs, err := reduceTaskReadInputFile(reply.NMap, reply.TaskIdx)
			if err != nil {
				fmt.Println("Fatal error in worker: ", err)
				continue
			}
			sort.Slice(kvs, func(i, j int) bool {
				return kvs[i].Key < kvs[j].Key
			})
			outFile, err := os.Create(TempReduceTaskOutputFileName(pid, reply.TaskIdx))
			if err != nil {
				fmt.Println("Fatal error in worker: ", err)
				continue

			}
			left, right := 0, 0
			for ; left < len(kvs) && right < len(kvs);{
				right = left + 1
				values := []string{kvs[left].Value}
				for ; right < len(kvs) && kvs[right].Key == kvs[left].Key; right++ {
					values = append(values, kvs[right].Value)
				}
				ret := reducef(kvs[left].Key, values)
				fmt.Fprintf(outFile, "%v %v\n", kvs[left].Key, ret)
				left = right
			}
			outFile.Close()
			result := TaskResult{TaskType: reply.TaskType, TaskUUID: reply.TaskUUID, WorkerId: pid, TaskIdx: reply.TaskIdx, InputFilePath: reply.TaskFilePath}
			reply := ExampleReply{}
			call("Coordinator.SendTaskResult", &result, &reply)
		} else if reply.TaskType == TaskWait {
			time.Sleep(time.Millisecond * 100)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
