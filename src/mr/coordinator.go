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

const (
	MaxTaskRunInterval = time.Second * 10
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

// A laziest, worker-stateless, channel-based implementation of Coordinator
type Coordinator struct {
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase	// 阶段
	tasks   []Task			// 任务

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		// 接受心跳，一般是请求执行任务，Coordinator 会通过 Response 下发任务。
		case msg := <-c.heartbeatCh:
			// 如果当前是已完成状态，就回包告知已完成
			if c.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			// 如果当前阶段的任务均已完成，就进入下一阶段
			} else if c.selectTask(msg.response) {
				switch c.phase {
				// 如果当前是 Map 阶段
				case MapPhase:
					log.Printf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
					// 进入 reduce 状态
					c.initReducePhase()
					c.selectTask(msg.response)
				// 如果当前是 Reduce 阶段
				case ReducePhase:
					log.Printf("Coordinator: %v finished, Congratulations \n", ReducePhase)
					c.initCompletePhase()
					msg.response.JobType = CompleteJob
				// 如果当前是 Complete 阶段
				case CompletePhase:
					panic(fmt.Sprintf("Coordinator: enter unexpected branch"))
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			msg.ok <- struct{}{}
		// 接受上报
		case msg := <-c.reportCh:
			// 修改任务状态
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				c.tasks[msg.request.Id].status = Finished
			}
			// 回包
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewJob := true, false

	// 遍历任务列表
	for id, task := range c.tasks {
		// 检查任务状态
		switch task.status {
		// 如果是初始化状态，则需要下发给 worker
		case Idle:
			allFinished, hasNewJob = false, true
			// 更新任务状态
			c.tasks[id].status, c.tasks[id].startTime = Working, time.Now()
			// 获取 reducer 数目 / reducer ID
			response.NReduce, response.Id = c.nReduce, id
			// 下发任务
			if c.phase == MapPhase {
				response.JobType, response.FilePath = MapJob, c.files[id]
			} else {
				response.JobType, response.NMap = ReduceJob, c.nMap
			}
		// 如果是执行中的状态
		case Working:
			allFinished = false
			// 如果任务已经超时，就重新下发任务
			if time.Now().Sub(task.startTime) > MaxTaskRunInterval {
				hasNewJob = true
				// 更新下发时间
				c.tasks[id].startTime = time.Now()
				// 获取 reducer 数目 / reducer ID
				response.NReduce, response.Id = c.nReduce, id
				// 下发任务
				if c.phase == MapPhase {
					response.JobType, response.FilePath = MapJob, c.files[id]
				} else {
					response.JobType, response.NMap = ReduceJob, c.nMap
				}
			}
		case Finished:
		}
		if hasNewJob {
			break
		}
	}

	// 如果当前阶段(map/reduce)还有其它未完成的 worker ，就告知其等待
	if !hasNewJob {
		response.JobType = WaitJob
	}

	// 如果当前阶段所有 worker 均已完成，就进入下一阶段
	return allFinished
}

func (c *Coordinator) initMapPhase() {
	// 设置当前为 Map 阶段
	c.phase = MapPhase
	// 为每个 file 创建一个 Map 任务
	c.tasks = make([]Task, len(c.files))
	for index, file := range c.files {
		c.tasks[index] = Task{
			fileName: file,		// 文件名
			id:       index,	// 任务 ID
			status:   Idle,		// 任务状态 init
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	name := coordinatorSock()
	os.Remove(name)
	l, e := net.Listen("unix", name)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	<-c.doneCh
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}, 1),
	}
	c.server()
	go c.schedule()
	return &c
}
