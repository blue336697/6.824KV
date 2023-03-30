package mr

//
// RPC 定义。记得把所有名字都大写。
//

import "os"
import "strconv"

//
// 示例展示如何声明参数并回复 RPC。
//

type Task struct {
	TaskType   TaskType // 任务类型判断到底是map还是reduce
	TaskId     int      // 任务的id
	ReducerNum int      // 传入的reducer的数量，用于hash
	FileInput  []string // 输入文件的切片，map一个文件对应一个文件名字，reduce是对应多个temp中间值文件
}
type TaskArgs struct{}
type TaskType int
type Phase int
type State int

// 任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask // WaitingTask任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask    // exit
)

// master阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// 任务状态
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// 在此处添加您的 RPC 定义。

// 在 /var/tmp 中为协调器创建一个独特的 UNIX 域套接字名称。
// 无法使用当前目录，因为 Athena AFS 不支持 UNIX 域套接字。
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
