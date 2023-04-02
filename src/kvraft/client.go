package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int   // 客户端的请求序号
	leaderId int   // 当前leader的id
	clientId int64 // 客户端的唯一标识
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

// 获取键的当前值。如果键不存在，则返回 “”。
// 面对所有其他错误，不断尝试。你可以发送一个带有如下代码的 RPC：ok ：= ck.servers[i]。
// Call（“KVServer.Get”， &args， &reply） 参数和回复的类型（包括它们是否是指针）必须与 RPC 处理程序函数参数的声明类型匹配。
// 并且回复必须作为指针传递。
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	severId := ck.leaderId //这个leaderID是我们在所有节点中随机的，如果不对就循环试
	//自旋等待返回结果
	for {
		reply := GetReply{}
		ok := ck.servers[severId].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.leaderId = severId
			return reply.Value
		} else if reply.Err == ErrNoKey {
			ck.leaderId = severId
			return ""
		} else if reply.Err == ErrWrongLeader {
			//如果不是这个leader，就尝试下一个
			severId = (severId + 1) % len(ck.servers)
			continue
		}
		//别的错误，继续尝试
		severId = (severId + 1) % len(ck.servers)
	}

}

// 由放置和追加共享。
// 你可以发送一个带有如下代码的 RPC：ok ：= ck.servers[i]。
// Call（“KVServer.PutAppend”， &args， &reply） args 和 reply 的类型（包括它们是否是指针）必须与 RPC 处理程序函数参数的声明类型匹配。
// 并且回复必须作为指针传递。
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	severId := ck.leaderId //这个leaderID是我们在所有节点中随机的，如果不对就循环试
	for {
		reply := PutAppendReply{}
		ok := ck.servers[severId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.leaderId = severId
			return
		} else if reply.Err == ErrWrongLeader {
			//如果不是这个leader，就尝试下一个
			severId = (severId + 1) % len(ck.servers)
			continue
		}
		//别的错误，继续尝试
		severId = (severId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
