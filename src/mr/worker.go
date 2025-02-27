package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		pid := os.Getpid()
		args := ApplyTaskArgs{WorkerId: pid}
		reply := ApplyTaskReply{}

		// send the RPC request, wait for the reply.
		call("Coordinator.ApplyTask", &args, &reply)
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
				hash_value := ihash(value) % reply.nReduce
				m[hash_value] = append(m[hash_value], kva[i])
			}
			for i := 0; i < reply.nReduce; i++ {
				f, err := os.Create(fmt.Sprintf("mr-%d-%d-%d", pid, reply.TaskId, i))
				if err != nil {
					fmt.Println(err)
					break
				}
				for j := range m[i] {
					fmt.Fprintf(f, "%v %v\n", m[i][j].Key, m[i][j].Value)
				}
				f.Close()
			}
		}else if reply.TaskType == TaskReduce {
			
		}else{
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
