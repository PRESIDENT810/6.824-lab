package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	for { // if returned task is -1 type, then the process exits
		taskType, mapTask, reduceTask, M, R := GetTask()
		intermediate := []KeyValue{}

		if taskType == 0 { // start the map process
			for _, filename := range mapTask.inputFiles {
				file, err := os.Open(filename)
				if err != nil {
					pwd, _ := os.Getwd()
					fmt.Printf("Current path: %s\n", pwd)
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
			buckets := make([][]KeyValue, R) // R buckets to store kv pairs, each bucket corresponds to a file
			for _, kv := range intermediate {
				bucketIdx := ihash(kv.Key) % R
				buckets[bucketIdx] = append(buckets[bucketIdx], kv)
			}
			for i := 0; i < R; i++ {
				// create a temp file to ensure nobody observes partially written files
				//tmpfile, err := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d-temp", mapTask.ID, i))
				tmpfile, _ := os.Create(fmt.Sprintf("mr-%d-%d-temp", mapTask.ID, i))
				enc := json.NewEncoder(tmpfile)
				for _, kv := range buckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
			}
			for i := 0; i < R; i++ {
				// rename the temp file when the map task is executed successfully
				err := os.Rename(fmt.Sprintf("mr-%d-%d-temp", mapTask.ID, i), fmt.Sprintf("mr-%d-%d", mapTask.ID, i))
				if err != nil {
					log.Fatal(err)
				}
			}
			_ = MapTaskDone(&mapTask) // tell master I've done the map task
		} else { // start the reduce process
			for i := 0; i < M; i++ {
				ifilename := fmt.Sprintf("mr-%d-%d", i, reduceTask.ID)
				ifile, err := os.Open(ifilename)
				if err != nil {
					log.Fatalf("cannot open %v", ifilename)
				}
				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			//tmpfile, _ := ioutil.TempFile("", fmt.Sprintf("mr-out-%d-temp", reduceTask.ID))
			tmpfile, _ := os.Create(fmt.Sprintf("mr-out-%d-temp", reduceTask.ID))
			i := 0
			sort.Sort(ByKey(intermediate))
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
				fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, occurence)
				i = j
			}
			// atomic rename output file
			err := os.Rename(fmt.Sprintf("mr-out-%d-temp", reduceTask.ID), fmt.Sprintf("mr-out-%d", reduceTask.ID))
			if err != nil {
				log.Fatal(err)
			}
			tmpfile.Close()
			_ = ReduceTaskDone(&reduceTask) // tell master I've done the reduce task
		}
	}
}

func MapTaskDone(mapTask *MapTask) error {
	args := DoneArgs{}
	args.Id = mapTask.ID
	args.TaskType = 0
	reply := DoneReply{}
	call("Master.TaskDone", &args, &reply)
	return nil
}

func ReduceTaskDone(reduceTask *ReduceTask) error {
	args := DoneArgs{}
	args.Id = reduceTask.ID
	args.TaskType = 1
	reply := DoneReply{}
	call("Master.TaskDone", &args, &reply)
	return nil
}

func GetTask() (int, MapTask, ReduceTask, int, int) {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Master.GiveTask", &args, &reply)
	mapTask := MapTask{}
	reduceTask := ReduceTask{}
	if reply.TaskType == -1 { // mapreduce is completed
		os.Exit(0) // exit with code 0
	} else if reply.TaskType == 0 { // it should be a map task
		mapTask.ID = reply.Id
		mapTask.state = 0 // initial state is idle...I guess?
		mapTask.inputFiles = strings.Split(reply.InputFiles, ";")
	} else {
		reduceTask.ID = reply.Id
		reduceTask.state = 0 // initial state is idle...I guess?
		reduceTask.inputFiles = strings.Split(reply.InputFiles, ";")
	}
	return reply.TaskType, mapTask, reduceTask, reply.M, reply.R
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
