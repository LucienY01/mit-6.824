package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var debbug = false

type Coordinator struct {
	mapTasks       []mapTask
	unstartedMaps  map[int]struct{}
	undoneMapTasks map[int]struct{}
	mapTimerDone   []chan int

	reduceTasks       [][]string
	unstartedReduces  map[int]struct{}
	undoneReduceTasks map[int]struct{}
	reduceTimerDone   []chan int
	nReduce           int

	sync.RWMutex
}

type mapTask struct {
	filename string
	done     bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Task(noneArgs *int, reply *Reply) error {

	c.Lock()
	defer c.Unlock()
	if len(c.undoneMapTasks) != 0 {
		if len(c.unstartedMaps) == 0 {
			log.Print("master: some map task is started but undone.")
			reply.Act = actWait
			return nil
		}

		// randomly choose one unstarted map task and return
		for i := range c.unstartedMaps {
			reply.Act = actWork
			reply.MR = workMap
			reply.Filenames = []string{c.mapTasks[i].filename}
			reply.IMap = i
			reply.NReduce = c.nReduce

			// Wait for 10 senconds. If timeout, re-issue the map task.
			delete(c.unstartedMaps, i)
			i := i
			go func() {
				t := time.NewTimer(10 * time.Second)
				select {
				case <-t.C:
					c.Lock()
					c.unstartedMaps[i] = struct{}{}
					log.Print("master: map timeout, re-issue the reduce task.")
					c.Unlock()
					return

				case <-c.mapTimerDone[i]:
					return
				}
			}()
			return nil
		}
	}

	// assign reduce tasks
	if len(c.undoneReduceTasks) != 0 {
		if len(c.unstartedReduces) == 0 {
			log.Print("master: some reduce task is started but undone.")
			reply.Act = actWait
			return nil
		}

		// randomly choose one of undone reduce tasks
		for iReduce := range c.unstartedReduces {
			reply.Act = actWork
			reply.MR = workReduce
			if debbug {
				log.Printf("master: c.reduceTasks[iReduce]: %v", c.reduceTasks[iReduce])
			}
			reply.Filenames = make([]string, len(c.reduceTasks[iReduce]))
			copied := copy(reply.Filenames, c.reduceTasks[iReduce])
			if debbug {
				log.Printf("master: copy %v filenames to reply, copied filenames: %v", copied, reply.Filenames)
			}
			reply.IReduce = iReduce

			// Wait for 10 senconds. If timeout, re-issue the reduce task.
			delete(c.unstartedReduces, iReduce)
			iReduce := iReduce
			go func() {
				t := time.NewTimer(10 * time.Second)
				select {
				case <-t.C:
					c.Lock()
					c.unstartedReduces[iReduce] = struct{}{}
					c.Unlock()
					log.Print("master: reduce timeout, re-issue the reduce task.")

				case <-c.reduceTimerDone[iReduce]:
					return
				}
			}()
			return nil

		}
	}

	// all jobs done, ask worker to exit
	reply.Act = actExit
	return nil
}

func (c *Coordinator) MapDone(args *ArgsMapDone, noneReply *int) error {
	// The map task has already been done.
	c.Lock()
	defer c.Unlock()
	if _, undone := c.undoneMapTasks[args.NumberMap]; !undone {
		return nil
	}

	close(c.mapTimerDone[args.NumberMap])

	c.mapTasks[args.NumberMap].done = true
	delete(c.undoneMapTasks, args.NumberMap)

	if debbug {
		log.Printf("master: before append: %v", c.reduceTasks)
	}
	for iReduce, filename := range args.Filenames {
		c.reduceTasks[iReduce] = append(c.reduceTasks[iReduce], filename)
		c.unstartedReduces[iReduce] = struct{}{}
		c.undoneReduceTasks[iReduce] = struct{}{}
	}
	if debbug {
		log.Printf("master: append a reduce file: %v, after append: %v", args.Filenames, c.reduceTasks)
	}

	return nil
}

func (c *Coordinator) ReduceDone(r *ArgsReduceDone, noneReply *int) error {
	// The reduce task has already been done, so do nothing.
	c.Lock()
	defer c.Unlock()
	if _, undone := c.undoneReduceTasks[r.I]; !undone {
		return nil
	}

	close(c.reduceTimerDone[r.I])
	delete(c.undoneReduceTasks, r.I)

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false

	c.RLock()
	if len(c.undoneMapTasks) == 0 && len(c.undoneReduceTasks) == 0 {
		ret = true
		log.Print("master: all jobs is done, master will exit.")
	}
	c.RUnlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:       make([]mapTask, len(files)),
		unstartedMaps:  make(map[int]struct{}),
		undoneMapTasks: make(map[int]struct{}),
		mapTimerDone:   make([]chan int, len(files)),

		reduceTasks:       make([][]string, nReduce),
		unstartedReduces:  make(map[int]struct{}),
		undoneReduceTasks: make(map[int]struct{}),
		reduceTimerDone:   make([]chan int, nReduce),
		nReduce:           nReduce,
	}

	for i, f := range files {
		c.mapTasks[i].filename = f
		c.mapTasks[i].done = false
		c.unstartedMaps[i] = struct{}{}
		c.undoneMapTasks[i] = struct{}{}
		c.mapTimerDone[i] = make(chan int)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTimerDone[i] = make(chan int)
	}

	c.server()
	return &c
}
