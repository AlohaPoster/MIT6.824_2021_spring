package kvraft

import (
	"6.824/labrpc"
	"log"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

// TODO : Fuck! This order is different from server.me! random
type Clerk struct {
	servers []*labrpc.ClientEnd
	// optimisization of section 8 in extend raft paper
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
	// TODO :possible to replicate ???
	ck.clientId = nrand()
	ck.recentLeaderId = GetRandomServer(len(ck.servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

// TODO : ----------------------------Hint-----------------------------
/*
After calling Start(), your kvservers will need to wait for Raft to complete agreement.
Commands that have been agreed upon arrive on the applyCh.
Your code will need to keep reading applyCh while PutAppend() and Get() handlers submit commands to the Raft log using Start().
Beware of deadlock between the kvserver and its Raft library.

You are allowed to add fields to the Raft ApplyMsg, and to add fields to Raft RPCs such as AppendEntries,
however this should not be necessary for most implementations.

A kvserver should not complete a Get() RPC if it is not part of a majority (so that it does not serve stale data).
A simple solution is to enter every Get() (as well as each Put() and Append()) in the Raft log.
You don't have to implement the optimization for read-only operations that is described in Section 8.

It's best to add locking from the start because the need to avoid deadlocks sometimes affects overall code design.
Check that your code is race-free using go test -race.
 */

func GetRandomServer(length int) int{
	return mathrand.Intn(length)
}

func (ck *Clerk) SendGetToServer(key string, server int) string {
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	reply := GetReply{}
	//DPrintf("[ClientSend GET]From ClientId %d, RequesetId %d, To Server %d, key : %v",ck.clientId, ck.requestId, server, key)
	log.Printf("[ClientSend GET]From ClientId %d, RequesetId %d, To Server %d, key : %v",ck.clientId, ck.requestId, server, key)
	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)

	if !ok || reply.Err == ErrWrongLeader{
		return ck.SendGetToServer(key,(server+1) % len(ck.servers))
	}

	//if reply.Err == ErrWrongLeader {
	//	return ck.SendGetToServer(key,reply.LeaderId)
	//}

	if reply.Err == OK {
		DPrintf("[ClientSend GET SUCCESS]From ClientId %d, RequesetId %d, key : %v, get value :%v",ck.clientId, ck.requestId, key, reply.Value)
		ck.recentLeaderId = server
		return reply.Value
	}

	return ""
}

func (ck *Clerk) Get(key string) string {

	//ck.requestId++
	//return ck.SendGetToServer(key,ck.recentLeaderId)

	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: requestId}

	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			ck.recentLeaderId = server
			//ck.requestId++
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) SendPutAppendToServer(key string,value string, opreation string, server int) {
	args := PutAppendArgs{Key: key, Value: value, Opreation : opreation, ClientId: ck.clientId, RequestId: ck.requestId}
	reply := PutAppendReply{}

	//DPrintf("[ClientSend PUTAPPEND]From ClientId %d, RequesetId %d, To Server %d, key : %v, value : %v, Opreation : %v",ck.clientId, ck.requestId, server, key, value, opreation)
	log.Printf("[ClientSend PUTAPPEND]From ClientId %d, RequesetId %d, To Server %d, key : %v, value : %v, Opreation : %v",ck.clientId, ck.requestId, server, key, value, opreation)
	ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

	if !ok || reply.Err == ErrWrongLeader{
		ck.SendPutAppendToServer(key,value,opreation,(server+1) % len(ck.servers))
	}

	//if reply.Err == ErrWrongLeader {
	//	ck.SendPutAppendToServer(key,value,opreation,reply.LeaderId)
	//}

	if reply.Err == OK {
		DPrintf("[ClientSend PUTAPPEND SUCCESS]From ClientId %d, RequesetId %d, key : %v, value : %v, Opreation : %v",ck.clientId, ck.requestId, key, value, opreation)
		ck.recentLeaderId = server
		return
	}
}

func (ck *Clerk) PutAppend(key string, value string, opreation string) {
	//ck.requestId++
	//ck.SendPutAppendToServer(key,value,opreation,ck.recentLeaderId)
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	for {
		args := PutAppendArgs{Key: key, Value: value, Opreation : opreation, ClientId: ck.clientId, RequestId: requestId}
		reply := PutAppendReply{}

		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader{
			server = (server+1)%len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = server
			//ck.requestId++
			return
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "append")
}
