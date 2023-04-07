package shardctrler

import (
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num 当前分片对应的配置

	seqMap    map[int64]int   //为了确保seq只执行一次	clientId / seqId（客户端请求的序号）
	waitChMap map[int]chan Op //传递由下层Raft服务的appCh传过来的command	index / chan(Op)，每个index对应一个chan
}

const (
	OverTime = 100

	JoinType  = "Join"
	LeaveType = "Leave"
	MoveType  = "Move"
	QueryType = "Query"

	// 无效 Gid 所有分片都应分配给 GID 零（无效的 GID）。
	InvalidGid = 0
)

type Op struct {
	// Your data here.
	SeqId    int
	ClientId int64
	OpType   string
	/*不同OP需要的参数也不同*/
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
	QueryNum    int // desired config number
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//封装Op，传递给Raft服务层
	op := Op{JoinServers: args.Servers, ClientId: args.ClientId, SeqId: args.SeqId, OpType: JoinType}
	lastIndex, _, _ := sc.rf.Start(op)
	//获取日志结束位置lastIndex的缓冲chan
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex) //从map中删除旧的index
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(OverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.SeqId == op.SeqId && replyOp.ClientId == op.ClientId {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//封装Op，传递给Raft服务层
	op := Op{LeaveGIDs: args.GIDs, ClientId: args.ClientId, SeqId: args.SeqId, OpType: LeaveType}
	lastIndex, _, _ := sc.rf.Start(op)
	//获取日志结束位置lastIndex的缓冲chan
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex) //从map中删除旧的index
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(OverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.SeqId == op.SeqId && replyOp.ClientId == op.ClientId {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//封装Op，传递给Raft服务层
	op := Op{MoveGID: args.GID, MoveShard: args.Shard, ClientId: args.ClientId, SeqId: args.SeqId, OpType: MoveType}
	lastIndex, _, _ := sc.rf.Start(op)
	//获取日志结束位置lastIndex的缓冲chan
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex) //从map中删除旧的index
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(OverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.SeqId == op.SeqId && replyOp.ClientId == op.ClientId {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// 分片器使用具有该编号的配置进行回复。
// 如果该数字为 -1 或大于已知的最大配置编号，则分片控制程序应回复最新配置。
// Query(-1) 的结果应反映分片控制程序在收到 Query(-1) RPC 之前完成处理的每个联接、离开或移动 RPC。
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//封装Op，传递给Raft服务层
	op := Op{QueryNum: args.Num, ClientId: args.ClientId, SeqId: args.SeqId, OpType: QueryType}
	lastIndex, _, _ := sc.rf.Start(op)
	//获取日志结束位置lastIndex的缓冲chan
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex) //从map中删除旧的index
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(OverTime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.SeqId == op.SeqId && replyOp.ClientId == op.ClientId {
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.SeqId
			//如果查询的结果根本不存在，那么就返回最新的配置
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else { //如果查询的结果存在，那么就返回查询的结果
				reply.Config = sc.configs[op.QueryNum]
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

// 这个方法主要就是中转数据，在raft对数据进行commit后再apply通道获取数据并对数据进行类似缓存操作
func (sc *ShardCtrler) handlerLoop() {
	for {
		select {
		case msg := <-sc.applyCh: //获取到apply以后的消息
			if msg.CommandValid { //CommandValid为true就代表有新提交的日志
				index := msg.CommandIndex
				op := msg.Command.(Op) //将记录在日志的指令操作赋值给操作符结构体
				//检查这个客户端是否重复请求，不重复再继续
				if !sc.isDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinType:
						sc.seqMap[op.ClientId] = op.SeqId //更新客户端的请求序列号
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGIDs))
					case MoveType:
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveShard, op.MoveGID))
					}
					sc.seqMap[op.ClientId] = op.SeqId
					sc.mu.Unlock()
				}
				// 将返回的ch返回waitCh
				sc.getWaitCh(index) <- op
			}
		}
	}
}

// 对于接收到raft处理好（持久化请求、日志相关）的join请求再次进行处理（实际业务的处理）
// 分片控制程序应通过创建包含新副本组的新配置来做出反应。
// 新配置应尽可能在整组之间平均划分分片，并应尽可能少地移动分片以实现该目标。
// 如果 GID 不是当前配置的一部分，则分片器应允许重用 GID（即应允许 GID 加入，然后离开，然后再次加入）。
// servers中就是每个group的id对应它下面的所有分片
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	//取出最后一个配置
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	//遍历这个这个配置下的所有分片组，并放到我们的容器中
	for gid, group := range lastConfig.Groups {
		newGroups[gid] = group
	}
	//然后将新的分片组放到容器中
	for gid, group := range servers {
		newGroups[gid] = group
	}
	// GroupMap: groupId -> shards	初始化放分片数量的容器
	groupMap := make(map[int]int)
	for gid := range newGroups {
		groupMap[gid] = 0
	}

	//记录每个分片组的分片数量
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			groupMap[gid]++
		}
	}
	//分片的负载均衡
	//如果没有分片那就不需要负载均衡
	if len(groupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	//有新的那就要负载均衡
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(groupMap, lastConfig.Shards),
		Groups: newGroups,
	}
}

// 分片控制程序应创建一个不包含这些组的新配置，并将这些组的分片分配给其余组。
// 新配置应尽可能平均地在组之间划分分片，并应移动尽可能少的分片以实现该目标。
func (sc *ShardCtrler) LeaveHandler(GIDs []int) *Config {
	// 用set感觉更合适点但是go并没有内置的set..
	leaveMap := make(map[int]bool)
	for _, gid := range GIDs {
		leaveMap[gid] = true
	}

	//取出最后一个配置
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	//遍历这个这个配置下的所有分片组，并放到我们的容器中
	for gid, group := range lastConfig.Groups {
		newGroups[gid] = group
	}
	// 删除对应的gid的值
	for _, gid := range GIDs {
		delete(newGroups, gid)
	}
	// GroupMap: groupId -> shards	初始化放分片数量的容器
	groupMap := make(map[int]int)
	curShards := lastConfig.Shards

	for gid := range newGroups {
		//如果这个分片组不在要删除的分片组中，那就初始化它的分片数量为0
		if !leaveMap[gid] {
			groupMap[gid] = 0
		}
	}

	//记录每个分片组的分片数量
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			// 如果这个组在leaveMap中，则置为0即为删除
			if leaveMap[gid] {
				curShards[shard] = 0
			} else {
				groupMap[gid]++
			}
		}
	}
	//分片的负载均衡
	//如果没有分片那就不需要负载均衡
	if len(groupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	//有新的那就要负载均衡
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(groupMap, curShards),
		Groups: newGroups,
	}
}

// 分片控制程序应创建一个新配置，其中分片分配给组。Move 的目的是让我们测试您的软件。
// 移动后的加入或离开可能会撤消移动，因为加入和离开会重新平衡。
// 就是移动分片位置
func (sc *ShardCtrler) MoveHandler(shard int, gid int) *Config {
	//取出最后一个配置
	lastConfig := sc.configs[len(sc.configs)-1]
	//创建一个新的配置
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	//将分片以及所在的分片组id放到新的配置中的分片中
	for oldShard, oldGID := range lastConfig.Shards {
		newConfig.Shards[oldShard] = oldGID
	}
	//移动完成
	newConfig.Shards[shard] = gid
	//同上
	for oldGID, oldGroup := range lastConfig.Groups {
		newConfig.Groups[oldGID] = oldGroup
	}
	return &newConfig
}

// 负载均衡
// GroupMap : gid -> servers[]
// lastShards : shard -> gid
func (sc *ShardCtrler) loadBalance(groupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(groupMap) //得到有几个组
	avg := NShards / length //平均给每个组分配的分片数量
	//如果不能整除，那就多分配一个
	//取余，得到多出的一个分片，比如 10 % 3 = 1，多出一个分片
	remainder := NShards % length
	sortGIDs := sortGroupShard(groupMap) //排序
	//开始遍历谁需要减负，即谁的分片数量大于平均值
	for i := 0; i < length; i++ {
		target := avg //每个组必须分配的数量
		// 判断这个分组是否需要更多分配，因为在有余数的情况下不可能完全均分，
		// 所以在当前组（前面的）的应该为avg+1，即多分配一个
		if !moreAllocations(length, remainder, i) {
			target = avg + 1
		}
		//在按照组序号排序以后，如果超出负载，进行重新分片，即分给别人
		if groupMap[sortGIDs[i]] > target {
			overLoadGID := sortGIDs[i]
			//得到需要重新分配的分片数量
			changeNum := groupMap[overLoadGID] - target
			//持续的遍历，看那些分组超出负载
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if overLoadGID == gid { //遍历到超出负载的分片组
					//然后将这个要分配给后面的分片进行标记
					lastShards[shard] = InvalidGid
					changeNum--
				}
			}
			//更新分片组的分片数量
			groupMap[overLoadGID] = target
		}
	}

	//现在看谁需要增加，然后将多出来的分给后面的
	for i := 0; i < length; i++ {
		target := avg
		if !moreAllocations(length, remainder, i) { //同理
			target = avg + 1
		}
		//如果分片数量小于平均值，那就说明可以分配给这个分组
		if groupMap[sortGIDs[i]] < target {
			freeGID := sortGIDs[i]
			changeNum := target - groupMap[freeGID]
			//开始遍历
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if InvalidGid == gid { //是我们上面标记的分片就说明要分配
					//将这个分片标记给这个分片组
					lastShards[shard] = freeGID
					changeNum--
				}
			}
			groupMap[freeGID] = target //分配
		}
	}
	return lastShards
}

// 排序 组序号
// GroupMap : groupId -> shard nums
func sortGroupShard(groupMap map[int]int) []int {
	length := len(groupMap)

	gidSlice := make([]int, 0, length)
	//将gid放到切片中
	for gid, _ := range groupMap {
		gidSlice = append(gidSlice, gid)
	}

	//根据每个组的分片数量进行排序，让多的在前面
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			//根据gid得到分片数量，谁大谁在前面
			//例如原始状态： 1->2, 2->3, 3->1	(gids -> shard nums)
			//排序后： 	  2->3, 3->1, 1->2
			if groupMap[gidSlice[j]] < groupMap[gidSlice[j-1]] ||
				(groupMap[gidSlice[j]] == groupMap[gidSlice[j-1]] &&
					gidSlice[j] < gidSlice[j-1]) {

				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

// 尽量按顺序给前面的组多分配
func moreAllocations(length int, remainder int, i int) bool {
	return i < length-remainder
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, isExists := sc.waitChMap[index]
	if !isExists {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

// 判断是否为重复请求，是的话就不进行缓存了
func (sc *ShardCtrler) isDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeqId, isExists := sc.seqMap[clientId]
	if !isExists {
		return false
	}
	//如果当前seqId小于等于上次的seqId，说明是之前的请求，重复了
	return seqId <= lastSeqId
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)
	go sc.handlerLoop()
	return sc
}
