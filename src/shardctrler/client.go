package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	mathrand "math/rand"
)
import "time"
import "crypto/rand"
import "math/big"

const RequsetIntervalTime = 120
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	requestId int
	recentLeaderId int
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
	// Your code here.
	ck.clientId = nrand()
	ck.recentLeaderId = GetRandomServer(len(ck.servers))
	return ck
}

func GetRandomServer(length int) int{
	return mathrand.Intn(length)
}

func (ck *Clerk) Query(num int) Config {
	ck.requestId++
	server := ck.recentLeaderId
	args := &QueryArgs{Num: num,ClientId: ck.clientId,Requestid: ck.requestId}
	// Your code here.

	for {
		reply := QueryReply{}
		ok := ck.servers[server].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server+1)%len(ck.servers)
			continue
		}
		// try each known server.

		if reply.Err == OK {
			ck.recentLeaderId = server
			return reply.Config
		}
		time.Sleep(RequsetIntervalTime * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestId++
	server := ck.recentLeaderId
	args := &JoinArgs{Servers: servers,ClientId: ck.clientId,Requestid: ck.requestId}
	// Your code here.
	for {
		reply := JoinReply{}
		ok := ck.servers[server].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server+1)%len(ck.servers)
			continue
		}
		// try each known server.

		if reply.Err == OK {
			ck.recentLeaderId = server
			return
		}
		time.Sleep(RequsetIntervalTime * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestId++
	server := ck.recentLeaderId
	args := &LeaveArgs{GIDs: gids,ClientId: ck.clientId,Requestid: ck.requestId}
	// Your code here.

	for {
		reply := LeaveReply{}
		ok := ck.servers[server].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server+1)%len(ck.servers)
			continue
		}
		// try each known server.

		if reply.Err == OK {
			ck.recentLeaderId = server
			return
		}
		time.Sleep(RequsetIntervalTime * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestId++
	server := ck.recentLeaderId
	args := &MoveArgs{Shard: shard,GID: gid,ClientId: ck.clientId,Requestid: ck.requestId}
	// Your code here.

	for {
		reply := MoveReply{}
		ok := ck.servers[server].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server+1)%len(ck.servers)
			continue
		}
		// try each known server.

		if reply.Err == OK {
			ck.recentLeaderId = server
			return
		}
		time.Sleep(RequsetIntervalTime * time.Millisecond)
	}
}
