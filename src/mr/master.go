package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	M, R                int        // number of assigned map workers & reduce workers
	mapDone, reduceDone int        // number of completed map workers & reduce workers
	mapAssigned         int        // how many map task already given
	reduceAssigned      int        // how many reduce task already given
	finished            bool       // whether mapreduce process is done
	mu                  sync.Mutex // mutex lock that ensures thread safety
	mapStatus           []int      // 0 means idle, 1 means in progress, 2 means completed
	reduceStatus        []int      // 0 means idle, 1 means in progress, 2 means completed
}

// Your code here -- RPC handlers for the worker to call.

//
// give a worker a task
//
func (master *Master) GiveTask(taskArgs *TaskArgs, taskReply *TaskReply) error {
	if master.mapAssigned == master.M && master.mapDone != master.M { // all map tasks assigned but some still running
		for {
			time.Sleep(1000000000)          // sleep and wait for a while
			if master.mapDone != master.M { // if all map tasks are done, then break
				break
			}
		}
	}
	if master.reduceDone >= master.R {
		taskReply.TaskType = -1 // mapreduce is done, and workers should quit
		return nil
	}

	if master.mapDone == master.M { // all map tasks done
		taskReply.TaskType = 1                 // give a reduce task
		taskReply.Id = master.nextReduceTask() // find a idle task and give
		taskReply.InputFiles = GiveReduceFiles(taskReply.Id, master.M)
		taskReply.M = master.M
		taskReply.R = master.R
	} else { // still some map tasks need to be assigned
		taskReply.TaskType = 0              // give a map task
		taskReply.Id = master.nextMapTask() // find a idle task and give
		taskReply.InputFiles = GiveMapFiles(taskReply.Id, master.M)
		taskReply.M = master.M
		taskReply.R = master.R
	}
	// TODO: add a timer to check whether this task is done in 10 seconds
	return nil
}

//
// when map/reduce tasks are done, worker should report to master that it has done its job
// if the task status is idle, then master thinks this task is fucked and re-assigned it to other workers
// so under this circumstance, the task's status should not be updated to "completed"
//
func (master *Master) TaskDone(doneArgs *DoneArgs, doneReply *DoneReply) error {
	master.mu.Lock()
	if doneArgs.TaskType == 0 { // a map task is done
		if master.mapStatus[doneArgs.Id] == 1 { // only update to "completed" for in progress tasks
			master.mapStatus[doneArgs.Id] = 2 // make this map task's status completed
			master.mapDone++
		}
	} else { // a reduce task is done
		if master.reduceStatus[doneArgs.Id] == 1 { // only update to "completed" for in progress tasks
			master.reduceStatus[doneArgs.Id] = 2 // make this reduce task's status completed
			master.reduceDone++
		}
	}
	master.mu.Unlock()
	doneReply.MapDone = master.mapDone
	doneReply.ReduceDone = master.reduceDone
	return nil
}

//
// if a task doesn't report done in 10s, then we think this task is fucked,
// and reset its status to "idle"
//
func (master *Master) faultHandler(taskType int, taskId int) error {

	return nil
}

//
// give next map task according to master's map task status
//
func (master *Master) nextMapTask() int {
	master.mu.Lock()
	logMaster(master)
	for i := 0; i < master.M; i++ {
		if master.mapStatus[i] == 0 { // find an idle map task
			master.mapStatus[i] = 1 // make it in progress
			master.mapAssigned++    // one more assigned map task
			master.mu.Unlock()
			return i
		}
	}
	master.mu.Unlock()
	panic("No more map task to give!")
}

//
// give next reduce task according to master's reduce task status
//
func (master *Master) nextReduceTask() int {
	master.mu.Lock()
	logMaster(master)
	for i := 0; i < master.R; i++ {
		if master.reduceStatus[i] == 0 { // find an idle reduce task
			master.reduceStatus[i] = 1 // make it in progress
			master.reduceAssigned++    // one more assigned reduce task
			master.mu.Unlock()
			return i
		}
	}
	master.mu.Unlock()
	panic("No more reduce task to give!")
}

//
// log how many map and reduce tasks are done
//
func logMaster(master *Master) {
	fmt.Printf("mapDone: %d, reduceDone: %d\n", master.mapDone, master.reduceDone)
}

//
// give the input files for map tasks according to its task id
//
func GiveMapFiles(id int, M int) string {
	filelist := []string{"../pg-being_ernest.txt",
		"../pg-dorian_gray.txt",
		"../pg-frankenstein.txt",
		"../pg-grimm.txt",
		"../pg-huckleberry_finn.txt",
		"../pg-metamorphosis.txt",
		"../pg-sherlock_holmes.txt",
		"../pg-tom_sawyer.txt"}
	return filelist[id]
}

//
// give the input files for reduce tasks according to its task id
//
func GiveReduceFiles(id int, M int) string {
	files := ""
	for i := 0; i < M; i++ {
		files += fmt.Sprintf("mr-%d-%d;", i, id)
	}
	return files
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
	nMap := 8

	m := Master{nMap, nReduce, 0, 0, 0, 0, false, sync.Mutex{}, make([]int, nMap), make([]int, nReduce)}
	// Your code here.

	m.server()
	return &m
}
