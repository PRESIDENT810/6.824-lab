package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	M, R                int  // number of assigned map workers & reduce workers
	mapDone, reduceDone int  // number of completed map workers & reduce workers
	mapAssigned         int  // how many map task already given
	reduceAssigned      int  // how many reduce task already given
	finished            bool // whether mapreduce process is done
}

// Your code here -- RPC handlers for the worker to call.

func (master *Master) TaskDone(doneArgs *DoneArgs, doneReply *DoneReply) error {
	if doneArgs.TaskType == 0 { // a map task is done
		master.mapDone++
	} else { // a reduce task is done
		master.reduceDone++
	}
	return nil
}

//
// give a worker a task
//
func (master *Master) GiveMeTask(taskArgs *TaskArgs, taskReply *TaskReply) error {
	// TODO: master server is concurrent, so don't forget to lock shit!!!
	// TODO: 他妈的应该怎么设计容错
	if master.mapAssigned == master.M && master.mapDone != master.M { // all map tasks assigned but some still running
		for {
			time.Sleep(10)                  // sleep and wait for a while
			if master.mapDone != master.M { // if all map tasks are done, then break
				break
			}
		}
	}

	if master.mapDone == master.M { // all map tasks done
		taskReply.taskType = 1 // you are a reduce task
		taskReply.id = master.reduceAssigned
		master.reduceAssigned++
		taskReply.inputFiles = GiveReduceFiles(taskReply.id)
		taskReply.M = master.M
		taskReply.R = master.R
	} else { // still some map tasks need to be assigned
		taskReply.taskType = 0
		taskReply.id = master.mapAssigned
		master.mapAssigned++
		taskReply.inputFiles = GiveMapFiles(taskReply.id)
		taskReply.M = master.M
		taskReply.R = master.R
	}
	return nil
}

func GiveMapFiles(id int) []string {
	// TODO: find filenames according to the map task's id
	return []string{}
}

func GiveReduceFiles(id int) []string {
	// TODO: find filenames according to the reduce task's id
	return []string{}
}

//
// start a thread that listens for RPCs from worker.go
//
func (master *Master) server() {
	rpc.Register(master)
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
func (master *Master) Done() bool {
	// Your code here.
	return master.reduceDone == master.R
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{8, nReduce, 0, 0, 0, 0, false}
	// Your code here.

	m.server()
	return &m
}
