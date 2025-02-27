package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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
			fmt.Fprintf(f, "%v\n", m[i][j].Key)
		}
		f.Close()
	}
	return nil
}

func reduceTaskReadInputFile(nMap int, taskIdx int) ([]string, error) {
	var kvs []string
	for i := 0; i < nMap; i++ {
		fileContent, err := os.ReadFile(MapTaskOutputFileName(i, taskIdx))
		if err != nil {
			fmt.Println("Fatal error in worker: ", err)
			return []string{}, err
		}
		fileContent_lines := strings.Split(string(fileContent), "\n")
		if fileContent_lines[len(fileContent_lines)-1] == "" {
			fileContent_lines = fileContent_lines[:len(fileContent_lines)-1]
		}
		kvs = append(kvs, fileContent_lines...)
	}
	return kvs, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		pid := os.Getpid()
		args := ApplyTaskArgs{WorkerId: pid}
		reply := ApplyTaskReply{}

		// send the RPC request, wait for the reply.
		ret := call("Coordinator.ApplyTask", &args, &reply)
		if !ret {
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

			m := map[int][]KeyValue{}
			for i := range kva {
				value := kva[i].Key
				hash_value := ihash(value) % reply.NReduce
				m[hash_value] = append(m[hash_value], kva[i])
			}
			err = writeTempMapTaskOutputFile(os.Getpid(), reply.TaskIdx, reply.NReduce, m)
			if err != nil {
				fmt.Println("Fatal error in worker: ", err)
				continue
			}
			result := TaskResult{TaskType: reply.TaskType, TaskUUID: reply.TaskUUID, WorkerId: pid, TaskIdx: reply.TaskIdx, InputFilePath: reply.TaskFilePath}
			reply := ExampleReply{}
			call("Coordinator.SendTaskResult", &result, &reply)
		} else if reply.TaskType == TaskReduce {
			vs, err := reduceTaskReadInputFile(reply.NMap, reply.TaskIdx)
			if err != nil {
				fmt.Println("Fatal error in worker: ", err)
				continue
			}
			sort.Slice(vs, func(i, j int) bool {
				return vs[i] < vs[j]
			})
			outFile, err := os.Create(TempReduceTaskOutputFileName(pid, reply.TaskIdx))
			if err != nil {
				fmt.Println("Fatal error in worker: ", err)
				continue
			}
			left, right := 0, 0
			for ; left < len(vs) && right < len(vs);{
				right = left + 1
				for ; right < len(vs) && vs[right] == vs[left]; right++ {}
				num := reducef(vs[left], vs[left:right])
				fmt.Fprintf(outFile, "%v %v\n", vs[left], num)
				left = right
			}
			outFile.Close()
			result := TaskResult{TaskType: reply.TaskType, TaskUUID: reply.TaskUUID, WorkerId: pid, TaskIdx: reply.TaskIdx, InputFilePath: reply.TaskFilePath}
			reply := ExampleReply{}
			call("Coordinator.SendTaskResult", &result, &reply)
		} else {
			break
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
