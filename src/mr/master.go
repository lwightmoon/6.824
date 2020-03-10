package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskState struct {
	file    string
	start   int64
	ok      bool
	running bool
}
type Master struct {
	// Your definitions here.
	lock     *sync.Mutex
	mTasks   []*taskState
	mReduces []*taskState
	mapOk    bool
	reduceOk bool
	nReduce  int
	nFiles   int
	stop     chan struct{}
}

func (m *Master) isTasksOk(tasks []*taskState) bool {
	var ok bool
	for _, task := range tasks {
		if !task.ok {
			ok = false
			break
		}
	}
	ok = true
	return ok
}

func (m *Master) monitorTask() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.mapOk {
		now := time.Now().Unix()
		var ok = true
		for _, task := range m.mTasks {
			if !task.ok && (task.running && now-task.start > 10) {
				task.running = false
				ok = false
				continue
			}
			if !task.ok {
				ok = false
			}
		}
		m.mapOk = ok
	}

	if !m.reduceOk {
		now := time.Now().Unix()
		var ok = true
		for _, task := range m.mReduces {
			if !task.ok && (task.running && now-task.start > 10) {
				task.running = false
				ok = false
				continue
			}
			if !task.ok {
				ok = false
			}
		}
		m.reduceOk = ok
	}
}

func (m *Master) getRunnableTask(tasks []*taskState, phase string) (rep *TaskReply) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.reduceOk {
		rep = &TaskReply{
			Finished: true,
		}
		return
	}
	for i, task := range tasks {
		if !task.running && !task.ok {
			rep = &TaskReply{
				File:    task.file,
				Phase:   phase,
				Index:   i,
				NReduce: m.nReduce,
				Nfiles:  m.nFiles,
			}
			task.running = true
			task.start = time.Now().Unix()

			return rep
		}
	}
	return rep
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTasks(args *TaskArgs, reply *TaskReply) error {
	switch args.Phase {
	case mapPhase:
		ret := m.getRunnableTask(m.mTasks, mapPhase)
		if ret != nil {
			reply.File = ret.File
			reply.Phase = ret.Phase
			reply.Index = ret.Index
			reply.NReduce = ret.NReduce
			reply.Nfiles = ret.Nfiles
		} else {
			reply.File = ""
		}
	case reducePhase:
		ret := m.getRunnableTask(m.mReduces, reducePhase)
		if ret != nil {
			reply.File = ret.File
			reply.Phase = ret.Phase
			reply.Index = ret.Index
			reply.NReduce = ret.NReduce
			reply.Nfiles = ret.Nfiles
		} else {
			reply.Nfiles = 0
		}

	default:
		return errors.New("task arg err")
	}
	return nil
}

func (m *Master) FinishTask(args *FinishArgs, reply *FinishReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch args.Phase {
	case mapPhase:
		task := m.mTasks[args.Index]
		task.ok = true
	case reducePhase:
		rTask := m.mReduces[args.Index]
		rTask.ok = true
	default:
		return errors.New("finish arg err")
	}
	return nil
}

func (m *Master) Ok(args *OkArgs, reply *OkReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch args.Phase {
	case mapPhase:
		reply.Ok = m.mapOk
	case reducePhase:
		reply.Ok = m.reduceOk
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
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
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.lock.Lock()
	defer m.lock.Unlock()
	ret = m.reduceOk
	if ret {
		close(m.stop)
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.lock = &sync.Mutex{}
	m.nReduce = nReduce
	m.nFiles = len(files)

	reduces := make([]*taskState, 0)
	for i := 0; i < nReduce; i++ {
		reduces = append(reduces, &taskState{})
	}
	maps := make([]*taskState, 0)
	for i := 0; i < len(files); i++ {
		maps = append(maps, &taskState{
			file: files[i],
		})
	}
	m.mReduces = reduces
	m.mTasks = maps
	m.stop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for {
			select {

			case <-ticker.C:
				m.monitorTask()
				log.Printf("mapok:%v,reduceok:%v", m.mapOk, m.reduceOk)
			case <-m.stop:
				return
			}

		}
	}()
	m.server()
	return &m
}
