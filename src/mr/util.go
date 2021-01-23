package mr

type MapTask struct {
	state      int      // idle (0), in-progress (1) or completed (2)
	ID         int      // which map task are you
	inputFiles []string // the file to read
}

type ReduceTask struct {
	state      int      // idle (0), in-progress (1) or completed (2)
	ID         int      // which reduce task are you
	inputFiles []string // the file to read
}
