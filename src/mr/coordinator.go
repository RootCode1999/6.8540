package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type JobMsg struct {
	response *JobResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

type Coordinator struct {
	files         []string
	nReduce       int
	nMap          int
	Schedulephase SchedulePhase
	tasks         []Task
	JobCh         chan JobMsg
	reportCh      chan reportMsg
	doneCh        chan struct{}
}

func (c *Coordinator) JobHeartbeat(request *JobRequest, response *JobResponse) error {
	msg := JobMsg{response, make(chan struct{})}
	c.JobCh <- msg
	log.Printf("work%v JobRequest Send\n", request.WorkId)
	<-msg.ok
	log.Printf("work%v JobHeartbeat Finish\n", request.WorkId)
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	log.Printf("Report Finish\n")
	return nil
}

func (c *Coordinator) initPhase(phaseType SchedulePhase) {
	log.Printf("init %v\n", phaseType)
	if phaseType == MapPhase {
		c.Schedulephase = MapPhase
		c.tasks = make([]Task, len(c.files))
		for index, file := range c.files {
			c.tasks[index] = Task{
				fileName: file,
				id:       index,
				status:   Waiting,
			}
		}
	} else if phaseType == ReducePhase {
		c.Schedulephase = ReducePhase
		c.tasks = make([]Task, c.nReduce)
		for i := 0; i < c.nReduce; i++ {
			c.tasks[i] = Task{
				id:     i,
				status: Waiting,
			}
		}
	} else if phaseType == CompletePhase {
		c.Schedulephase = CompletePhase
		c.doneCh <- struct{}{}
	} else {
		panic(fmt.Sprintf("unexpected SchedulePhase %v", phaseType))
	}
	for _, task := range c.tasks {
		log.Printf("task:%v\n", task)
	}
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	<-c.doneCh
	return true
}

const maxRunTime = time.Second * 10

func (c *Coordinator) changPhase(response *JobResponse) bool {
	log.Printf("start changPhase")
	allTaskFineshed, hasNewTask := true, false
	for id, task := range c.tasks {
		switch task.status {
		case Waiting:
			allTaskFineshed = false
			hasNewTask = true
			if c.Schedulephase == MapPhase {
				response.JobType = MapJob
				response.Id = id
			} else if c.Schedulephase == ReducePhase {
				response.JobType = ReduceJob
				response.Id = id
			}
			response.NMap = c.nMap
			response.NReduce = c.nReduce
			response.FilePath = task.fileName
			c.tasks[id].status = Working
			c.tasks[id].startTime = time.Now()
			log.Printf("select task %v\n", task)
		case Working:
			allTaskFineshed = false
			hasNewTask = false
			if time.Now().Sub(task.startTime) > maxRunTime {
				hasNewTask = true
				c.tasks[id].startTime = time.Now()
				if c.Schedulephase == MapPhase {
					response.JobType = MapJob
					response.Id = id
				} else if c.Schedulephase == ReducePhase {
					response.JobType = ReduceJob
					response.Id = id
				}
				response.NMap = c.nMap
				response.NReduce = c.nReduce
				response.FilePath = task.fileName
			}
		}
		if hasNewTask {
			log.Printf("Break change phase %v", task.fileName)
			break
		}
	}
	for _, task := range c.tasks {
		if task.status != Finished {
			allTaskFineshed = false
		}
	}
	if hasNewTask == false {
		response.JobType = WaitJob
	}
	return allTaskFineshed
}

func (c *Coordinator) process() {
	c.initPhase(MapPhase)
	for {
		select {
		case msg := <-c.JobCh:
			log.Printf("msg:%v\n", msg.response)
			log.Printf("c.Schedulephase:%v\n", c.Schedulephase)
			if c.Schedulephase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if c.changPhase(msg.response) {
				if c.Schedulephase == MapPhase {
					c.initPhase(ReducePhase)
					c.changPhase(msg.response)
					log.Printf("Coordinator: MapPhase finished, start ReducePhase \n")
				} else if c.Schedulephase == ReducePhase {
					c.initPhase(CompletePhase)
					msg.response.JobType = CompleteJob
					log.Printf("Coordinator: ReducePhase finished, Congratulations \n")
				} else {
					panic(fmt.Sprintf("Wrong Phase(CompletePhase)"))
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			log.Printf("msg:%v\n", msg.request)
			if msg.request.Phase == c.Schedulephase {
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				taskId := msg.request.Id
				c.tasks[taskId].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		nReduce:  nReduce,
		nMap:     len(files),
		JobCh:    make(chan JobMsg),
		reportCh: make(chan reportMsg),
		doneCh:   make(chan struct{}, 1),
	}
	logFile, _ := os.OpenFile("/home/lky/school/mit/6.5840/src/main/terminal.txt",
		os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	log.SetOutput(logFile)
	log.Printf("start\n")
	c.server()
	go c.process()
	return &c
}
