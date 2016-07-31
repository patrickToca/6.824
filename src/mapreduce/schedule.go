package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	tasks := make(chan int)
	for t := 0; t < ntasks; t++ {
		go func(t int) {
			tasks <- t
		}(t)
	}

	// To signal to the for loop to break when all tasks are completed
	quit := make(chan bool)
	task := make(chan int, 1)
	tasksCompleted := 0
	go func() {
		for {
			select {
			case <-task:
				tasksCompleted++
				if tasksCompleted == ntasks {
					quit <- true
				}
			default:
			}
		}
	}()

Loop:
	for {
		select {
		case <-quit:
			break Loop
		case t := <-tasks:
			go func(t int) {
				w := <-mr.registerChannel
				arg := new(DoTaskArgs)
				arg.JobName = mr.jobName
				arg.Phase = phase
				arg.TaskNumber = t
				arg.File = mr.files[t]
				arg.NumOtherPhase = nios

				ok := call(w, "Worker.DoTask", arg, new(struct{}))
				if !ok {
					// If the task didn't complete successfully, we put it back to the tasks channel so that it can be taken up by another worker.
					tasks <- t
				} else {
					// If we complete a task successfully we send over a signal to the other goroutine which mantains a count of the tasks completed.
					task <- t
				}
				mr.registerChannel <- w
			}(t)
		}
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
