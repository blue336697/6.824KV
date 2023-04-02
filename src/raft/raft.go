package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//
//	create a new Raft server.
//
// rf.Start(command interface{}) (index, term, isleader)
//
//	start agreement on a new log entry
//
// rf.GetState() (term, isLeader)
//
//	ask a Raft for its current term, and whether it thinks it is leader
//
// ApplyMsg
//
//	每次将新条目提交到日志时，每个 Raft 对等方都应向同一服务器中的服务（或测试人员）发送 ApplyMsg。
//	当每个 Raft 对等体意识到连续的日志条目被提交时，对等方应通过传递给 Make（） 的 applyCh 向同一服务器上的服务（或测试器）发送 ApplyMsg。
//	将“命令有效”设置为 true 以指示 ApplyMsg 包含新提交的日志条目。
//	在 2D 部分中，您需要在 applyCh 上发送其他类型的消息（例如快照），但对于这些其他用途，请将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Status 节点的角色
type Status int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

const (

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 100
	MinVoteTime  = 75

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 35
	AppliedSleep   = 15
)

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

// 实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu        sync.Mutex          // 锁定以保护对该节点状态的共享访问
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 端点
	persister *Persister          // 保持此对等方持久状态的对象
	me        int                 // 该节点在 peers[] 中的索引
	dead      int32               // 由 Kill() 设置
	// Your data here (2A, 2B, 2C).
	// 查看论文的图 2，了解 Raft 服务器必须保持的状态。
	currentTerm int        // 服务器最后知道的任期号（初始化为 0，持续递增）
	votedFor    int        // 在当前获得选票的候选人（投给某个人）的 Id(初始化为 -1，表示没有投票给任何人)
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	//服务器常修改的状态
	commitIndex      int // 已知的最大的已经被提交的日志条目的索引值（初始化为 0，持续递增）
	lastAppliedIndex int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	//leader 常修改的状态
	//nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	nextIndex  []int // 对于每个服务器，需要发送给他的下一个日志条目的索引值（初始化为 leader 最后日志索引加 1）
	matchIndex []int // 对于每个服务器，已经复制给他的日志的最高索引值（初始化为 0，持续递增）

	//自定义状态
	status     Status    // 节点的角色（状态）
	votedNum   int       //记录当前投给多少个节点
	votedTimer time.Time //计时器

	applyChan chan ApplyMsg // 用于提交日志的通道（2B）

	lastIncludeIndex int //快照中最后一个日志条目的索引值(2D)
	lastIncludeTerm  int //快照中最后一个日志条目的任期号(2D)
}

// 存放log的结构体
type LogEntry struct {
	Term    int
	Command interface{}
}

/* --------------------------------------------------------RPC参数部分----------------------------------------------------*/
// 示例 RequestVote RPC 参数结构
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// 示例 RequestVote RPC 回复结构。
// 如果竞选者任期比自己的任期还短，那就不投票，返回false
// 如果当前节点的votedFor为空，且竞选者的日志条目跟收到者的一样新则把票投给该竞选者
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       // 当前任期号，以便于候选人去更新自己过期的任期号
	VoteGranted bool      // 候选人赢得了此张选票时为真
	VoteState   VoteState //投票状态
}

// AppendEntriesArgs由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // leader 的任期号
	LeaderId     int        // 发送请求的领导者的 Id，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值，那么就是len（logs）+1的长度，初始化为leader.nextIndex - 1
	PrevLogTerm  int        // PrevLogIndex 条目的任期号，初始化为leader.currentTerm
	Entries      []LogEntry // 需要存储的日志条目（可能为空即为心跳连接，但不能为 null）
	LeaderCommit int        // 领导者已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term        int                // 当前任期号，以便于领导者去更新自己过期的任期号
	Success     bool               // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志才会进行追加，不匹配返回false
	AppState    AppendEntriesState // 追加日志的状态
	UpNextIndex int                //节点更新请求的nextIndex中的索引值
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
}

type InstallSnapshotReply struct {
	Term int
}

// 服务或测试人员想要创建一个 Raft 服务器。所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// 这个服务器的端口是 peers[me]。所有服务器的 peers[] 数组都具有相同的顺序。
// persister 是此服务器保存其持久状态的地方，并且最初还保存最近保存的状态（如果有）。
// applyCh 是测试人员或服务期望 Raft 发送 ApplyMsg 消息的通道。
// Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()

	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votedNum = 0

	//日志
	rf.commitIndex = 0
	rf.lastAppliedIndex = 0
	//快照
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh // 通知 applyCh 有新的日志条目被提交,2B
	rf.mu.Unlock()

	// 从崩溃前持续存在的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastAppliedIndex = rf.lastIncludeIndex
	}

	// 启动 ticker goroutine
	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

/* ----------------------------------------------------ticker部分----------------------------------------------------*/
// 对ticker进行分类，因为选举、日志追加、心跳、日志快照都是每个节点都会进行ticker，只不过是接收者或者发送者的区别而已
// 选举ticker
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		// 时间过期发起选举
		// 此处的流程为每次每次votedTimer如果小于在sleep睡眠之前定义的时间，就代表没有votedTimer没被更新为最新的时间，则发起选举
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			rf.mu.Lock()
			// 转变状态，将票投给自己
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.votedNum = 1
			rf.currentTerm += 1
			rf.persist()
			//挨个发送选举请求
			rf.sendElection()
			rf.votedTimer = time.Now()

			rf.mu.Unlock()
		}
	}
}

// 作为leader发送心跳
func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// 提交日志ticker
func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastAppliedIndex >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastAppliedIndex < rf.commitIndex && rf.lastAppliedIndex < rf.getLastIndex() {
			rf.lastAppliedIndex += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastAppliedIndex,
				Command:       rf.restoreLog(rf.lastAppliedIndex).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}

}

/* ----------------------------------------------选举RPC----------------------------------------------------*/
// 将 RequestVote RPC 发送到服务器的示例代码。
// server 是 rf.peers[] 中目标服务器的索引。
// 期望 args 中的 RPC 参数。用 RPC 回复填写回复，所以调用者应该通过 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须与处理函数中声明的参数类型相同（包括它们是否为指针）。
//
// labrpc 包模拟有损网络，其中服务器可能无法访问，并且请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果回复在超时间隔内到达，则 Call() 返回 true；
// 否则 Call() 返回 false。因此 Call() 可能暂时不会返回。
// 错误的返回可能是由死服务器、无法访问的活动服务器、丢失的请求或丢失的回复引起的。
//
// Call() 保证返回（可能在延迟之后），除非服务器端的处理函数不返回。因此，无需围绕 Call() 实现您自己的超时。
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// 如果您在使 RPC 工作时遇到问题，请检查您是否已将通过 RPC 传递的结构中的所有字段名称大写，并且调用者使用 & 传递回复结构的地址，而不是结构本身。
func (rf *Raft) sendElection() {
	// 对自身以外的节点发送选举请求
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)
			//如果请求被处理
			if res == true {
				rf.mu.Lock()
				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.status = Follower
					rf.votedFor = -1
					rf.votedNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}
				// 返回结果正确判断是否大于一半节点同意,如果投票数大于一半，成为leader
				if reply.VoteGranted == true && rf.currentTerm == args.Term {
					rf.votedNum += 1
					if rf.votedNum >= len(rf.peers)/2+1 {
						rf.status = Leader
						rf.votedFor = -1
						rf.votedNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.votedTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

// RequestVote RPC 处理程序。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果别的节点的任期号比自己的小，那么就说明出现网路分区，那么就将自己的任期号回复给他，并且不投票给他
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//如果别的节点的任期号比自己的大，那么就需要重置自己的状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.votedNum = 0
		rf.persist()
	}

	// 根据选举约束，我们要限制这两个条件
	// 1、args.LastLogTerm < lastLogTerm是因为选举时应该首先看term，只要term大的，才代表存活在raft中越久
	// 2、判断日志的最后一个index是否是最新的前提应该是要在这两个节点任期是否是相同的情况下，判断哪个数据更完整
	// 判断日志是否冲突，或者已经投过票，并且投的还不是发起投票的节点，并且两节点的状态一样
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) ||
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { // paper中的第二个条件votedFor is null

		// 满足以上两个其中一个都返回false，不给予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		//一切正常，给予投票
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}
}

/*--------------------------------------日志增量RPC--------------------------------------*/
// leader向其他节点发送心跳建立请求
func (rf *Raft) leaderAppendEntries() {
	//遍历节点的索引
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		//开始使用协程向其他节点发送日志RPC
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}
			//installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
			// 同时要注意的是比快照还小时，已经算是比较落后
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}
			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			//如果有要更新的日志，那么就进行添加
			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			//发送RPC并接收结果
			res := rf.sendAppendEntries(server, &args, &reply)
			//如果节点能进行结果返回，那么对结果进行处理
			if res == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//再次检查
				if rf.status != Leader {
					return
				}
				//如果返回的任期大于自身的任期，那么自己就不是最新的了，需要转换为Follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.votedNum = 0
					rf.persist()
					rf.votedTimer = time.Now()
					return
				}
				//如果追加成功，那么就更新自身的nextIndex和matchIndex
				if reply.Success {
					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 这里就是为了将日志进行筛选，精准的找到commitIndex
					//外层遍历下标是否满足,从快照最后开始反向进行
					for index := rf.getLastIndex(); index >= rf.lastIncludeIndex+1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						// 大于一半，且因为是从后往前，一定会大于原本commitIndex
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}

					}
				} else {
					//如果追加失败，冲突的地方不为-1，则进行更新
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}
		}(index)
	}
}

// AppendEntries 接收建立心跳、同步日志的RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果当前结点的任期号大于接收到的任期号，说明出现了分区，返回当前节点的任期号
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	//如果一切正常，就将当前结点的状态变为follower，并更新相应的状态
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.votedNum = 0
	rf.status = Follower
	rf.persist()
	rf.votedTimer = time.Now()

	//对返回的reply进行赋值，即返回自己的信息作为应答
	reply.Term = args.Term
	reply.Success = true
	reply.UpNextIndex = -1

	//对比任期号后就开始处理出现冲突的情况
	// 首先要保证自身len(rf)大于0否则数组越界
	// 1、 如果preLogIndex的大于当前日志的最大的下标说明跟随者缺失日志，拒绝附加日志
	// 2、 如果preLog处的任期和preLogIndex处的任期和preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志

	// 自身的快照Index比发过来的prevLogIndex还大，所以返回冲突的下标加1(原因是冲突的下标用来更新nextIndex，nextIndex比Prev大1
	// 返回冲突下标的目的是为了减少RPC请求次数
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}
	// 如果自身最后的快照日志比prev小说明中间有缺失日志，such 3、4、5、6、7 返回的开头为6、7，而自身到4，缺失5
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex()
		return
	} else if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
		for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
			if rf.restoreLogTerm(index) != tempTerm {
				reply.UpNextIndex = index + 1
				break
			}
		}
		return
	}

	// 如果一切正常，进行日志的截取
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	//持久化
	rf.persist()
	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// commitIndex取leaderCommit与last new entry最小值的原因是，虽然应该更新到leaderCommit，但是new entry的下标更小
	// 则说明日志不存在，更新commit的目的是为了applied log，这样会导致日志日志下标溢出
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	return
}

/*--------------------------------------日志快照RPC--------------------------------------*/
// 该服务表示，它已经创建了一个快照，其中包含所有信息，包括索引。
//这意味着服务不再需要通过（包括）该索引进行日志。Raft 现在应该尽可能修剪其日志。

// index代表是快照apply应用的index,即在快照中最高的日志条目的下标
// 而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果自身快照点大于index说明不需要安装；如果传入的下标大于自身的最新提交，说明没被提交不能安装快照，
	if index <= rf.lastIncludeIndex || index > rf.commitIndex {
		return
	}
	//开始更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	//更新快照下标和任期
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs

	//当apply了以后，快照进行重置
	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastAppliedIndex {
		rf.lastAppliedIndex = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.status != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.votedNum = 0
			rf.persist()
			rf.votedTimer = time.Now()
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.votedNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastAppliedIndex {
		rf.lastAppliedIndex = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyChan <- msg

}

// 服务想要切换到快照。仅当 Raft 没有更新的信息时，才这样做，因为它在 applyCh 上传达快照。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

/*------------------------------------其他---------------------------------------*/
// 使用 Raft 的服务（例如 kv 服务器）想要就下一个要附加到 Raft 日志的命令达成协议。
// 如果此服务器不是领导者，则返回 false。否则启动协议并立即返回。
// 无法保证此命令将永远提交到 Raft 日志，因为领导者可能会失败或失去选举。
// 即使 Raft 实例已经被杀死，这个函数也应该优雅地返回。
//
// 第一个返回值是命令在提交时将出现的索引。第二个返回值是当前术语。如果此服务器认为它是领导者，则第三个返回值为真。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).

	if rf.killed() {
		return index, term, false
	}

	if rf.status != Leader {
		return index, term, false
	} else {
		isLeader = true
		index = rf.getLastIndex() + 1
		term = rf.currentTerm
		//初始化日志，并对日志进行追加
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		return index, term, isLeader
	}

}

// 返回 currentTerm 以及该服务器是否认为它是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
	// Your code here (2A).
}

/*------------------------------------持久化（编码解码）---------------------------------------*/
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		log.Fatal("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

func (rf *Raft) persistData() []byte {
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	return data
}

// 测试人员不会在每次测试后停止由 Raft 创建的 goroutine，但它会调用 Kill() 方法。
//您的代码可以使用 killed() 检查是否已调用 Kill()。使用 atomic 避免了对锁的需要。

// 问题是长时间运行的 goroutines 会占用内存并且可能会消耗 CPU 时间，可能会导致后面的测试失败并产生令人困惑的调试输出。
// 任何具有长时间运行循环的 goroutine 都应该调用 killed() 来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
