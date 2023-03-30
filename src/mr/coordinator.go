package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var lock sync.Mutex

type Coordinator struct {
	TaskId            int            // 用于生成task的特殊id
	CurPhase          Phase          // 目前整个框架应该处于什么任务阶段
	MapTaskChannel    chan *Task     //并发安全的传递传递或接受map类型的Task
	ReduceTaskChannel chan *Task     //并发安全的传递传递或接受reduce类型的Task
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	MapNum            int            // 传入的参数决定需要多少个map
	taskMetaHolder    TaskMetaHolder //存放着待分配的任务
	files             []string       //任务文件
}

//存放对应桶里面的元数据索引

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

//维护任务的状态

type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time //开始时间
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// 启动一个线程来监听来自 worker.go 的 RPC
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

// main/mr/coordinator.go 定期调用 Done() 以查看整个作业是否已完成
func (c *Coordinator) Done() bool {
	lock.Lock()
	defer lock.Unlock()
	if c.CurPhase == AllDone {
		fmt.Println("所有任务都完成了，master将退出！ ！")
		return true
	}
	return false
}

// 创建一个协调器。 main/mr/coordinator.go 调用这个函数。
// nReduce 是要使用的 reduce 任务的数量。
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		CurPhase: MapPhase,
		//初始化为有缓冲即异步的通道
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		ReducerNum:        nReduce,
		MapNum:            len(files),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	//Coordinator制作map任务，在一开始程序运行的时候就执行
	c.makeMapTasks(files)
	c.server()

	go c.CrashSearch() //开启协程去探测
	return &c
}

// 将Map任务放到Map管道中，taskMetaInfo放到taskMetaHolder中。
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		id := c.generateTaskId()
		//构建任务
		task := Task{
			TaskId:     id,
			TaskType:   MapTask,
			ReducerNum: c.ReducerNum,
			FileInput:  []string{file},
		}

		//保存当前任务状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, //当前任务刚被创建完属于被放在队列等待消费的状态
			TaskAdr: &task,
		}
		//将元信息保存
		c.taskMetaHolder.setTaskInfo(&taskMetaInfo)
		fmt.Println("正在生成map任务 :", &task)
		c.MapTaskChannel <- &task
	}
}

// 将reduce任务放到reduce管道中，taskMetaInfo放到taskMetaHolder中。
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		//构建任务
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileInput: selectReduceName(i),
		}

		//保存当前任务状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, //当前任务刚被创建完属于被放在队列等待消费的状态
			TaskAdr: &task,
		}
		//将元信息保存
		c.taskMetaHolder.setTaskInfo(&taskMetaInfo)
		fmt.Println("正在生成reduce任务 :", &task)
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(i int) []string {
	var s []string
	//返回对应于当前目录的根路径名。如果当前目录可以通过多个路径（由于符号链接）到达，Getwd 可能会返回其中任何一个。
	//相当于把我们持久化的那些文件直接遍历，然后找到符合前面我们持久化文件的名字就属于reduce
	path, _ := os.Getwd()
	//把这些路径的文件全部读出来
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") &&
			strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			s = append(s, file.Name())
		}
	}
	return s
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// 将元信息保存到map中去
func (t TaskMetaHolder) setTaskInfo(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	err, _ := t.MetaMap[taskId]
	if err != nil {
		fmt.Printf("元信息已经包含%v的任务\n", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

// rpc被调用方法，被worker调用申请任务，多个worker可能会同时调用这个游戏来获取任务，所以记得加锁
func (c *Coordinator) GetTask(args *TaskArgs, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	fmt.Println("Master从worker那里得到一个请求:")
	//我们先要当前mapreduce当前的任务状态，换而言之就是看map任务都执行完没有，没有就进入reduce阶段
	switch c.CurPhase {
	case MapPhase: //如果通道里还有任务或者还有map任务没有执行完，那么分配给worker map任务
		{
			if len(c.MapTaskChannel) > 0 {
				//由于传过来的是地址，那就再取一次指针拿到具体的数据，然后将通道里面的任务输出，同样需要指针处理因为我们
				//希望任务从始至终就处理对应的拿一个
				*reply = *<-c.MapTaskChannel
				//这里其实就是多做一次健壮，就怕任务在被执行还没出通道
				if !c.taskMetaHolder.judgeTaskState(reply.TaskId) {
					fmt.Printf("任务id[ %d ]正在被别的线程执行\n", reply.TaskId)
				}
			} else { //通道没任务，那么此时就在等所有map任务执行完
				reply.TaskType = WaitingTask
				//检查任务完成情况，返回true则全部完成，就进入下一个阶段
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil //就是没有任务可申请喽
			}
		}
	case ReducePhase: //如果正在执行reduce阶段，这里我们对应的worker还没有实现处理reduce的方法
		{
			if len(c.ReduceTaskChannel) > 0 {
				//由于传过来的是地址，那就再取一次指针拿到具体的数据，然后将通道里面的任务输出，同样需要指针处理因为我们
				//希望任务从始至终就处理对应的拿一个
				*reply = *<-c.ReduceTaskChannel
				//这里其实就是多做一次健壮，就怕任务在被执行还没出通道
				if !c.taskMetaHolder.judgeTaskState(reply.TaskId) {
					fmt.Printf("任务id[ %d ]正在被别的线程执行\n", reply.TaskId)
				}
			} else { //通道没任务，那么此时就在等所有reduce任务执行完
				reply.TaskType = WaitingTask
				//检查任务完成情况，返回true则全部完成，就进入下一个阶段
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil //就是没有任务可申请喽
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		panic("mapReduce可没有这么多状态哦")
	}

	return nil
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeTaskState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now() //任务开始
	return true
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAdr.TaskType == MapTask {
			// 判断任务是否完成,下同
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	fmt.Printf("map任务完成了（完成/总数） %d/%d, reduce任务完成了（完成/总数） %d/%d \n",
		mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false
}

// 更新当前master的阶段
func (c *Coordinator) toNextPhase() {
	if c.CurPhase == MapPhase {
		//如果是map阶段全部任务完成，那么就该进入下一个reduce阶段
		c.makeReduceTasks()
		c.CurPhase = ReducePhase
	} else if c.CurPhase == ReducePhase {
		c.CurPhase = AllDone
	}
}

// 标记当前任务的完成状态
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//防止从另一个worker返回的重复工作
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("map任务[%d]正在执行.\n", args.TaskId)
		} else {
			fmt.Printf("map任务[%d]已经完成 ! ! !\n", args.TaskId)
		}
		break
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//防止从另一个worker返回的重复工作
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("reduce任务[%d]正在执行.\n", args.TaskId)
		} else {
			fmt.Printf("reduce任务[%d]已经完成 ! ! !\n", args.TaskId)
		}
		break

	default:
		panic("任务类型未定义 ! ! !")
	}
	return nil
}

// 探测某个worker下线进行更换的容灾操作
func (c *Coordinator) CrashSearch() {
	for {
		time.Sleep(time.Second * 3) //确保master的其他方法能够拿到锁，要不然探测器会疯狂的获取锁导致其他方法失败
		lock.Lock()
		if c.CurPhase == AllDone { //所有任务都完成了还检测啥啊
			lock.Unlock()
			break
		}

		for _, taskInfo := range c.taskMetaHolder.MetaMap {
			if taskInfo.state == Working { //如果正在工作那么看看执行时间
				fmt.Println("任务[", taskInfo.TaskAdr.TaskId, "] 执行了: ", time.Since(taskInfo.StartTime), "s秒")
			}
			//如果超时大于10秒了进行切换worker
			if taskInfo.state == Working && time.Since(taskInfo.StartTime) > 10*time.Second {
				fmt.Println("任务[", taskInfo.TaskAdr.TaskId, "] 超时")
				switch taskInfo.TaskAdr.TaskType {
				case MapTask: //将任务重新放回对应的通道中，并更新相应状态
					{
						c.MapTaskChannel <- taskInfo.TaskAdr
						taskInfo.state = Waiting
					}
				case ReduceTask:
					{
						c.ReduceTaskChannel <- taskInfo.TaskAdr
						taskInfo.state = Waiting
					}
				}
			}
		}
		lock.Unlock()
	}
}
