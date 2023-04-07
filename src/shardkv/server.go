package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	UpConfigLoopInterval = 100 * time.Millisecond // poll configuration period

	GetTimeout          = 500 * time.Millisecond
	AppOrPutTimeout     = 500 * time.Millisecond
	UpConfigTimeout     = 500 * time.Millisecond
	AddShardsTimeout    = 500 * time.Millisecond
	RemoveShardsTimeout = 500 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string

	ClientId int64
	SeqId    int
	OpType   Operation //get、put、append

	ShardId int
	Shard   Shard
	SeqMap  map[int64]int

	UpdateConfig shardctrler.Config //根据分片的更新而更新对应的配置
}

// OpReply 用于在 Op 从 applyCh 到达后唤醒等待的 RPC 调用方
type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

type Shard struct {
	KvMap         map[string]string
	ConfigVersion int //当前分片的版本，每次操作都会更新
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	clients      []*labrpc.ClientEnd //保存RPC发送方的信息
	maxraftstate int                 // 如果日志增长这么大，则快照

	// Your definitions here.
	dead int32 // set by Kill()

	Config     shardctrler.Config // 需要更新的最新的配置
	LastConfig shardctrler.Config // 更新之前的配置，用于比对是否全部更新完了

	seqMap    map[int64]int        //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan OpReply //传递由下层Raft服务的appCh传过来的command	index / chan(Op)，每个index对应一个chan
	//存放已经持久化的shard，这里用于放在快照中以后用来恢复
	shardPersist []Shard // ShardId -> Shard 如果KvMap == nil则说明当前的数据不归当前分片管，

	serverClerk *shardctrler.Clerk // sck 是用于联系分片主服务器的客户端
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	//如果这个分组不存在这个服务器上就返回
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		//KVmap里都是持久化后的分片的KV，如果为空说明raft还没处理好
	} else if kv.shardPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}

	command := Op{
		OpType:   GetType,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
	}
	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	//经过raft层返回的结果再次检查分片
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}

// 添加分片将分片从调用方移动到此服务器
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
	return
}

// applyMsgHandlerLoop 处理applyCh发送过来的ApplyMsg
func (kv *ShardKV) ApplyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid { //如果有新提交的日志
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {
					shardId := key2shard(op.Key)
					if kv.Config.Shards[shardId] != kv.gid { //得到要修改分片的组id，与当前服务器的gid对比
						reply.Err = ErrWrongGroup
					} else if kv.shardPersist[shardId].KvMap == nil { //看是否需要持久化
						// 如果应该存在的切片没有数据那么这个切片就还没到达
						reply.Err = ShardNotArrived
					} else {
						if !kv.isDuplicate(op.ClientId, op.SeqId) {
							kv.seqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardPersist[shardId].KvMap[op.Key] = op.Value
							case GetType:
								//get什么都不做
							case AppendType:
								kv.shardPersist[shardId].KvMap[op.Key] += op.Value
							default:
								log.Fatalf("invalid command type: %v.", op.OpType)
							}
						}
					}

				} else { //其他操作情况，就是请求时分组发过来的对分片的操作，就是对分片的负载均衡请求需要由我们发送给分片控制器
					switch op.OpType {
					case UpConfigType: //对于配置文件的信息
						kv.updateConfigHandler(op)
					case AddShardType:
						// 如果配置号比op的SeqId还低说明不是最新的配置
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						kv.removeShardHandler(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
				}

				//如果增加日志后大于临界值那就需要快照
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapShot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapShot)
				}
				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()
			}
			if msg.SnapshotValid { //如果有新的快照，将其反序列化
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.mu.Lock()
					fmt.Print("11111")
					kv.DecodeSnapShot(msg.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}
		}
	}
}

// ConfigDetectedLoop 配置检测
func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()
	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		//如果遍历导的节点不是leader换下一个
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		// 判断是否把不属于自己的部分给分给别人了
		if !kv.allSent() {
			seqMap := make(map[int64]int)
			for k, v := range kv.seqMap {
				seqMap[k] = v
			}
			//找到标记的分片，并且有最新的配置
			// 将最新配置里不属于自己的分片分给别人
			for shardId, gid := range kv.LastConfig.Shards {
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid &&
					kv.shardPersist[shardId].ConfigVersion < kv.Config.Num {

					sendData := kv.cloneShard(kv.Config.Num, kv.shardPersist[shardId].KvMap)
					args := SendShardArg{
						LastAppliedRequestId: seqMap,
						ShardId:              shardId,
						Shard:                sendData,
						ClientId:             int64(gid),
						RequestId:            kv.Config.Num,
					}
					//得到当前配置中不同分组对应的服务层，根据服务层能够找到客户端
					serverList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serverList))
					for i, name := range serverList {
						servers[i] = kv.makeEnd(name)
					}

					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							// 对自己的共识组内进行add
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)
							if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// 当前配置已配置，轮询下一个配置
		curConfig = kv.Config
		clerk := kv.serverClerk
		kv.mu.Unlock()

		newConfig := clerk.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:       UpConfigType,
			ClientId:     int64(kv.gid),
			SeqId:        newConfig.Num,
			UpdateConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}

}

// 服务器 [] 包含此组中服务器的端口。me 是服务器 [] 中当前服务器的索引。
//
// KV 服务器应通过底层 Raft 实现存储快照，该实现应调用 persister.SaveStateAndSnapshot() 以原子方式保存 Raft 状态以及快照。
//
// 当 Raft 保存的状态超过 maxraftstate 字节时，KV 服务器应创建快照，以便允许 Raft 对其日志进行垃圾回收。
// 如果 maxraftstate 为 -1，则无需拍摄快照。gid 是该组的 GID，用于与分片机交互。
// 将 ctrlers[] 传递给 Shardctrler.MakeClerk()，这样你就可以将 RPC 发送到 shardctrler。
// make_end（servername）将服务器名称从Config.Groups[gid][i]转换为labrpc。
// 可以在其上发送 RPC 的客户端。你将需要它来将 RPC 发送到其他组。
// 查看 client.go 以获取有关如何使用 ctrlers[] 和 make_end() 将 RPC 发送到拥有特定分片的组的示例。
// StartServer() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutines。
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.clients = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan OpReply)
	kv.shardPersist = make([]Shard, shardctrler.NShards)

	kv.serverClerk = shardctrler.MakeClerk(kv.clients) //通过客户端创建出代理对象clerk

	//快照
	snapShot := persister.ReadSnapshot()
	//如果有快照就解码
	if len(snapShot) > 0 {
		kv.DecodeSnapShot(snapShot)
	}
	//创建rf节点
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyMsgHandlerLoop()
	go kv.ConfigDetectedLoop()
	return kv
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 判断是否为重复请求，是的话就不进行缓存了
func (kv *ShardKV) isDuplicate(clientId int64, seqId int) bool {
	lastSeqId, isExists := kv.seqMap[clientId]
	if !isExists {
		return false
	}
	//如果当前seqId小于等于上次的seqId，说明是之前的请求，重复了
	return seqId <= lastSeqId
}

// 获得raft传回index位置的缓冲chan，跟applychan不是一回事
func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	ch, isExists := kv.waitChMap[index]
	if !isExists {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

// 序列化，即持久化server的数据
func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.shardPersist)
	err = e.Encode(kv.seqMap)
	err = e.Encode(kv.maxraftstate)
	err = e.Encode(kv.Config)
	err = e.Encode(kv.LastConfig)
	if err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	data := w.Bytes()
	return data
}

// 反序列化
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	var shardPersist []Shard
	var seqMap map[int64]int
	var maxraftstate int
	var Config, LastConfig shardctrler.Config

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&shardPersist) != nil || d.Decode(&seqMap) != nil ||
		d.Decode(&maxraftstate) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		//如果没有对应序列化内容
		log.Fatalf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	} else {
		kv.shardPersist = shardPersist
		kv.seqMap = seqMap
		kv.maxraftstate = maxraftstate
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}

func (kv *ShardKV) startCommand(command Op, timeout time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	//等到raft
	timer := time.NewTicker(timeout)
	defer timer.Stop()

	select {
	case reply := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		//说明不是这个请求，对应不上了
		if reply.SeqId != command.SeqId || reply.ClientId != command.ClientId {
			// 一种方法是让服务器检测到它已经失去了领导地位，方法是注意到 Start()返回的索引中出现了不同的请求
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return reply.Err

	case <-timer.C:
		return ErrOverTime
	}
}

func (kv *ShardKV) updateConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpdateConfig
	if curConfig.Num >= upConfig.Num {
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			kv.shardPersist[shard].KvMap = make(map[string]string)
			kv.shardPersist[shard].ConfigVersion = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
}

func (kv *ShardKV) addShardHandler(op Op) {
	// 此分片已添加或它是过时的命令
	if kv.shardPersist[op.ShardId].KvMap != nil || op.Shard.ConfigVersion < kv.Config.Num {
		return
	}

	kv.shardPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigVersion, op.Shard.KvMap)

	for clientId, seqId := range op.SeqMap {
		if r, ok := kv.seqMap[clientId]; !ok || r < seqId {
			kv.seqMap[clientId] = seqId
		}
	}
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.Config.Num { //当前这个版本是比较新的，可能对应的分片早没了
		return
	}
	kv.shardPersist[op.ShardId].KvMap = nil
	kv.shardPersist[op.ShardId].ConfigVersion = op.SeqId
}

// 判断当前最新的配置文件中有没有不属于自己的分片
func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardPersist[shard].ConfigVersion < kv.Config.Num {
			return false
		}
	}
	return true
}

// 作为一个服务层，如果只能发送的话就不能接收别人发给属于我们的分片了
func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 判断切片是否都收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardPersist[shard].ConfigVersion < kv.Config.Num {
			return false
		}
	}
	return true
}

// 如果又不属于自己的分片，就进行克隆，克隆的发出去，原本的删除
func (kv *ShardKV) cloneShard(configVersion int, kvMap map[string]string) Shard {
	migrateShard := Shard{
		KvMap:         make(map[string]string),
		ConfigVersion: configVersion,
	}

	for k, v := range kvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}
