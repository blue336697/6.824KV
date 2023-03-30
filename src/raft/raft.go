package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"math/rand"
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
//	each time a new entry is committed to the log, each Raft peer
//	should send an ApplyMsg to the service (or tester)
//	in the same server.
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// HeartBeatTimeout 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

// 节点投票状态
const (
	Normal VoteState = iota //投票过程正常
	Killed                  //Raft节点已终止
	Expire                  //投票(消息\竞选者）过期
	Voted                   //本Term内已经投过票

)

// 节点追加日志状态
const (
	AppNormal    AppendEntriesState = iota // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	AppRepeat                              // 追加重复 (2B
	AppCommitted                           // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B

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
	status         Status        // 节点的角色（状态）
	electionTimer  time.Duration // 选举超时时间
	heartbeatTimer *time.Ticker  //每个节点的计时器，用于心跳超时

	applyChan chan ApplyMsg // 用于提交日志的通道（2B）
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

	rf.applyChan = applyCh // 通知 applyCh 有新的日志条目被提交,2B

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastAppliedIndex = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.electionTimer = time.Duration(rand.Intn(200)+150) * time.Millisecond // 随机产生选举时间150~300ms
	rf.heartbeatTimer = time.NewTicker(rf.electionTimer)                    // 心跳定时器

	// 从崩溃前持续存在的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// 启动 ticker goroutine 开始选举
	go rf.ticker()

	return rf
}

// 如果这个对等方最近没有收到心跳，则 ticker go 例程将开始新的选举
func (rf *Raft) ticker() {
	for rf.killed() == false {

		//您在此处的代码用于检查是否应开始领导者选举并使用 time.Sleep() 随机化休眠时间。
		select {
		case <-rf.heartbeatTimer.C: // 查看心跳计时器的状态
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.status {
			case Follower:
				rf.status = Candidate
				fallthrough
			case Candidate:
				// 开始选举，初始化自己的任期号，投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1 //统计投票数
				rf.persist()
				//每轮选举都要重置选举超时时间
				rf.electionTimer = time.Duration(150+rand.Intn(200)) * time.Millisecond // 300~600ms
				rf.heartbeatTimer.Reset(rf.electionTimer)                               // 重置心跳计时器

				// 对自身以外的节点进行选举
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					// 向其他服务器发送选举请求
					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs),
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums) // 异步发送选举请求
				}
			case Leader:
				//进行心跳同步，日志同步
				appendNums := 1                           //统计同步成功的服务器数量
				rf.heartbeatTimer.Reset(HeartBeatTimeout) // 重置心跳计时器
				//构造msg
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					appendEntriesReply := AppendEntriesReply{}
					//将当前leader的最新日志条目附带给当前遍历的节点，nextIndex表示的是需要给这个节点发送的下一条日志的索引，
					//减1的意思就是当前leader全部的且没有最新的日志情况下
					// 如果nextIndex[i]长度不等于rf.logs,代表与leader的log entries不一致，需要附带过去
					appendEntriesArgs.Entries = rf.logs[rf.nextIndex[i]-1:]
					//有日志的情况下，将同步过的索引设置进去
					if rf.nextIndex[i] > 0 {
						appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
					}
					//已经有同步过的日志时，那么就要将对应任期号设置进去
					if appendEntriesArgs.PrevLogIndex > 0 {
						appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex-1].Term
					}
					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
				}
			}
			rf.mu.Unlock()
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//由于网络分区，请求投票的人的term的比自己的还小，不给予投票
	if args.Term < rf.currentTerm {
		return false
	}
	switch reply.VoteState {
	//消息如果过期了有两种情况：1.是本身的term过期了比节点还小 2.节点日志的条目数落后于节点
	case Expire:
		rf.status = Follower //变成跟随者
		rf.heartbeatTimer.Reset(rf.electionTimer)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
	case Normal, Voted:
		//根据是否投票成功，更新投票数
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
			*voteNums++
		}
		//如果投票数大于一半，成为leader
		if *voteNums >= (len(rf.peers)/2)+1 {
			*voteNums = 0
			if rf.status == Leader {
				return ok
			}
			//如果之前不是leader，初始化nextIndex和matchIndex
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.heartbeatTimer.Reset(HeartBeatTimeout)
		}
	case Killed:
		return false
	}
	return ok
}

// 示例 RequestVote RPC 处理程序。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果当前结点的状态已经停止了，那么就设置回应的消息,并且不会投票给这个节点
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//如果别的节点的任期号比自己的小，那么就说明出现网路分区，那么就将自己的任期号回复给他，并且不投票给他
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//如果别的节点的任期号比自己的大，那么就需要重置自己的状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
	}

	//如果当前没有投票
	if rf.votedFor == -1 {

		lastLogTerm := 0
		//如果当前日志是有记录的，那么就要进行同步
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		//根据选举约束，我们要限制这两个条件
		// 1、args.LastLogTerm < lastLogTerm是因为选举时应该首先看term，只要term大的，才代表存活在raft中越久
		// 2、判断日志的最后一个index是否是最新的前提应该是要在这两个节点任期是否是相同的情况下，判断哪个数据更完整
		//如果不满足这两个条件，那么就不投票给他
		if args.LastLogTerm < lastLogTerm ||
			(len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)) {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.persist()
			return
		}
		//以上都满足，那么就投票给发请求过来的节点
		rf.votedFor = args.CandidateId

		reply.VoteState = Normal
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()

		rf.heartbeatTimer.Reset(rf.electionTimer)
	} else {
		//如果当前已经投票了，那么此时存在两种情况，一是投给了自己，二是投给了别人
		//更新自己的状态
		reply.VoteState = Voted
		reply.VoteGranted = false

		//如果投给自己直接返回
		if rf.votedFor != args.CandidateId {
			return
		} else {
			//投给别人，那么就要更新自己的状态
			rf.status = Follower
		}
		//重置选举超时时间
		rf.heartbeatTimer.Reset(rf.electionTimer)
	}
	return
}

// 向其他节点发送心跳建立请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {
	if rf.killed() {
		return
	}
	//如果append失败应该不断的重试 ,直到这个log成功的被提交
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	//必须在加在这里否则加载前面retry时进入时，RPC也需要一个锁，但是又获取不到，因为锁已经被加上了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.AppState {
	//目标节点崩溃
	case AppKilled:
		return
	// 目标节点正常返回,2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好，后续有要求进行修改即可
	//这里就对应日志增量正常并且apply到通道了
	case AppNormal:
		// 2B需要判断返回的节点是否超过半数commit，才能将自身commit
		if reply.Success && reply.Term == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
			*appendNums++
		}

		// 如果发送给server这个节点的索引大于日志量，说明返回的值已经大过了自身数组
		if rf.nextIndex[server] > len(rf.logs)+1 {
			return
		}
		//如果server这个节点对应的索引小于日志量，那么就更新索引
		rf.nextIndex[server] += len(args.Entries)
		//检查是否有超过半数的节点commit了
		if *appendNums > len(rf.peers)/2 {
			// 保证幂等性，不会提交第二次
			*appendNums = 0

			// 如果日志为空或者日志的最后一条的term不等于当前的term，那么就不进行apply
			if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
				return
			}
			// 如果一切顺利leader就开始apply
			for rf.lastAppliedIndex < len(rf.logs) {
				rf.lastAppliedIndex++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastAppliedIndex-1].Command,
					CommandIndex: rf.lastAppliedIndex,
				}
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastAppliedIndex
			}

		}
		return
	//前面说过当状态发生改变时就要进行持久化
	// 目标节点返回这个状态就代表当前leader已经过时
	case AppOutOfDate:
		//所以就要将当前结点变为follower，并把相应的状态进行更新
		rf.status = Follower
		rf.votedFor = -1
		rf.heartbeatTimer.Reset(rf.electionTimer)
		rf.currentTerm = reply.Term
		rf.persist()
	//当日志不匹配或者日志已经提交了的时候
	case Mismatch, AppCommitted:
		//并且回复的任期号大于当前leader的，那么就要变成follower
		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.heartbeatTimer.Reset(rf.electionTimer)
			rf.currentTerm = reply.Term
			rf.persist()
		}
		//记录以下不匹配的server回复过来下次要在那个地方开始更新的节点索引
		rf.nextIndex[server] = reply.UpNextIndex
	}
	return
}

// AppendEntries 接收建立心跳、同步日志的RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果当前结点挂壁了，返回状态信息
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}
	//如果当前结点的任期号大于接收到的任期号，说明出现了分区，返回当前节点的任期号
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//对比任期号后就开始处理出现冲突的情况
	// 首先要保证自身len(rf)大于0否则数组越界
	// 1、 如果preLogIndex的大于当前日志的最大的下标说明跟随者缺失日志，拒绝附加日志
	// 2、 如果preLog处的任期和preLogIndex处的任期和preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志

	//记得试一下不减一的情况
	if args.PrevLogIndex > 0 && (len(rf.logs) < args.PrevLogIndex ||
		rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastAppliedIndex + 1
		return
	}

	// 如果当前节点提交的Index比传过来的还高，说明当前节点的日志已经超前,需返回过去
	if args.PrevLogIndex != -1 && rf.lastAppliedIndex > args.PrevLogIndex {
		reply.AppState = AppCommitted
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastAppliedIndex + 1
		return
	}

	//如果一切正常，就将当前结点的状态变为follower，并更新相应的状态
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.heartbeatTimer.Reset(rf.electionTimer)

	//对返回的reply进行赋值，即返回自己的信息作为应答
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	//如果一切正常，那么进行追加日志
	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}
	//持久化
	rf.persist()
	//进行apply
	for rf.lastAppliedIndex < args.LeaderCommit {
		rf.lastAppliedIndex++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastAppliedIndex,
			Command:      rf.logs[rf.lastAppliedIndex-1].Command,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastAppliedIndex
	}
	return
}

// 返回 currentTerm 以及该服务器是否认为它是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	term = rf.currentTerm
	if rf.status == Leader {
		isLeader = true
	} else {
		isLeader = false
	}
	// Your code here (2A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
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
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 使用 Raft 的服务（例如 kv 服务器）想要就下一个要附加到 Raft 日志的命令达成协议。
// 如果此服务器不是领导者，则返回 false。否则启动协议并立即返回。
// 无法保证此命令将永远提交到 Raft 日志，因为领导者可能会失败或失去选举。
// 即使 Raft 实例已经被杀死，这个函数也应该优雅地返回。
//
// 第一个返回值是命令在提交时将出现的索引。第二个返回值是当前术语。如果此服务器认为它是领导者，则第三个返回值为真。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return index, term, false
	}
	isLeader = true

	//初始化日志，并对日志进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.persist()
	return index, term, isLeader
}

// 测试人员不会在每次测试后停止由 Raft 创建的 goroutine，但它会调用 Kill() 方法。
//您的代码可以使用 killed() 检查是否已调用 Kill()。使用 atomic 避免了对锁的需要。

// 问题是长时间运行的 goroutines 会占用内存并且可能会消耗 CPU 时间，可能会导致后面的测试失败并产生令人困惑的调试输出。
// 任何具有长时间运行循环的 goroutine 都应该调用 killed() 来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.heartbeatTimer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
