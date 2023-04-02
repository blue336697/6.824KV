package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// 字段名称必须以大写字母开头，否则 RPC 将中断。
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // 日志转换为快照的临界值

	// Your definitions here.
	seqMap           map[int64]int     //为了确保seq只执行一次	clientId / seqId
	waitChMap        map[int]chan Op   //传递由下层Raft服务的appCh传过来的command	index / chan(Op)，每个index对应一个chan
	kvPersist        map[string]string // 存储持久化的KV键值对	K / V，用来映射底层持久化的数据
	lastIncludeIndex int               // 最后一次快照的日志下标
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{Key: args.Key, ClientId: args.ClientId, SeqId: args.SeqId, OpType: "Get"}
	lastIndex, _, _ := kv.rf.Start(op)
	//获取日志结束位置lastIndex的缓冲chan
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		//从map中删除旧的index
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()
	//设置定时器
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	//这里从通道里获取的op已经是经过中间handler缓存的过得结果了
	case replyOp := <-ch: //从下层Raft服务的appCh中获取command
		if replyOp.ClientId == op.ClientId && replyOp.SeqId == op.SeqId {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, idLeader := kv.rf.GetState()
	if !idLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{Key: args.Key, Value: args.Value, ClientId: args.ClientId, SeqId: args.SeqId, OpType: args.Op}
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	//这里从通道里获取的op已经是经过中间handler缓存的过得结果了
	case replyOp := <-ch:
		if replyOp.ClientId == op.ClientId && replyOp.SeqId == op.SeqId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
	defer timer.Stop()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 这个方法主要就是中转数据，在raft对数据进行commit后再apply通道获取数据并对数据进行类似缓存操作
func (kv *KVServer) handlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh: //获取到apply以后的消息
			if msg.CommandValid { //CommandValid为true就代表有新提交的日志
				if msg.CommandIndex <= kv.lastIncludeIndex {
					return
				}
				index := msg.CommandIndex
				op := msg.Command.(Op) //将记录在日志的指令操作赋值给操作符结构体
				//检查这个客户端是否重复请求，不重复再继续
				if !kv.isDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvPersist[op.Key] = op.Value
					case "Append":
						kv.kvPersist[op.Key] += op.Value
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}
				//如果增加日志后大于临界值那就需要快照
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapShot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapShot)
				}
				kv.getWaitCh(index) <- op
			}
			if msg.SnapshotValid { //如果有新的快照，将其反序列化
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

// 获得raft传回index位置的缓冲chan，跟applychan不是一回事
func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, isExists := kv.waitChMap[index]
	if !isExists {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

// 判断是否为重复请求，是的话就不进行缓存了
func (kv *KVServer) isDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeqId, isExists := kv.seqMap[clientId]
	if !isExists {
		return false
	}
	//如果当前seqId小于等于上次的seqId，说明是之前的请求，重复了
	return seqId <= lastSeqId
}

// 反序列化
func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	}
}

// 序列化，即持久化server的数据
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

// servers[] 包含一组服务器的端口，这些服务器将通过 Raft 协作以形成容错键值服务。
// me 是服务器 [] 中当前服务器的索引。
// KV 服务器应通过底层 Raft 实现存储快照，该实现应调用 persister。
// SaveStateAndSnapshot（） 以原子方式保存 Raft 状态以及快照。
// 当 Raft 保存的状态超过 maxraftstate 字节时，KV 服务器应创建快照，以便允许 Raft 对其日志进行垃圾回收。
// 如果 maxraftstate 为 -1，则无需拍摄快照。
// StartKVServer（） 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutines。
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan Op)
	kv.kvPersist = make(map[string]string)

	kv.lastIncludeIndex = -1

	snapShot := persister.ReadSnapshot()
	if len(snapShot) > 0 {
		kv.DecodeSnapShot(snapShot)
	}

	go kv.handlerLoop() //对这层信息进行处理，可以理解为中转站，对上层与下层数据进行处理、持久化等
	return kv
}
