package mr

import (
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	taskType, mapTask, reduceTask, M, R := GetTask()
	intermediate := []KeyValue{}

	if taskType == 0 { // start the map process
		for _, filename := range mapTask.inputFiles {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}
		// TODO: write intermediate kv pairs into R local files

		_ = MapTaskDone() // tell master I've done the map task
	} else { // start the reduce process
		// TODO: read  intermediate kv pairs from M local files
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			occurence := reducef(intermediate[i].Key, values)

			// TODO: write result into the output file

			i = j
		}

		//ofile.Close()
		_ = ReduceTaskDone() // tell master I've done the reduce task
	}

}

func MapTaskDone() error {
	args := DoneArgs{}
	reply := DoneReply{}
	args.TaskType = 0
	call("Master.TaskDone", &args, &reply)
	return nil
}

func ReduceTaskDone() error {
	args := DoneArgs{}
	reply := DoneReply{}
	args.TaskType = 1
	call("Master.TaskDone", &args, &reply)
	return nil
}

func GetTask() (int, MapTask, ReduceTask, int, int) {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Master.GiveMeTask", &args, &reply)
	mapTask := MapTask{}
	reduceTask := ReduceTask{}
	if reply.taskType == -1 { // mapreduce is completed
		os.Exit(0) // exit with code 0
	} else if reply.taskType == 0 { // it should be a map task
		mapTask.ID = reply.id
		mapTask.state = 0 // initial state is idle...I guess?
		mapTask.inputFiles = reply.inputFiles
	} else {
		reduceTask.ID = reply.id
		reduceTask.state = 0 // initial state is idle...I guess?
		reduceTask.inputFiles = reply.inputFiles
	}
	return reply.taskType, mapTask, reduceTask, reply.M, reply.R
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
