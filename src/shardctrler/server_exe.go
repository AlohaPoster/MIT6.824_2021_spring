package shardctrler

import (
	"6.824/raft"
)
//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

//type Config struct {
//	Num    int              // config number
//	Shards [NShards]int     // shard -> gid
//	Groups map[int][]string // gid -> servers[]
//}

func (sc *ShardCtrler) ReadRaftApplyCommandLoop(){
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
		}
	}
}

func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg){
	op := message.Command.(Op)

	if message.CommandIndex <= sc.lastSSPointRaftLogIndex {
		return
	}
	//DPrintf("[RaftApplyCommand]Server %d , Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v",sc.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation)
	if !sc.ifRequestDuplicate(op.ClientId, op.RequestId){
		if op.Operation == JoinOp {
			sc.ExecJoinOnController(op)
		}
		if op.Operation == LeaveOp {
			sc.ExecLeaveOnController(op)
		}
		if op.Operation == MoveOp {
			sc.ExecMoveOnController(op)
		}
	}

	if sc.maxraftstate != -1{
		sc.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	sc.SendMessageToWaitChan(op, message.CommandIndex)
}

func (sc *ShardCtrler) SendMessageToWaitChan(op Op, raftIndex int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitApplyCh[raftIndex]
	if exist {
		//DPrintf("[RaftApplyMessageSendToWaitChan-->]Server %d , Send Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v",sc.me, raftIndex, op.ClientId, op.RequestId, op.Operation)
		ch <- op
	}
	return exist
}

func (sc *ShardCtrler) InitNewConfig()Config{
	var tempConfig Config
	if len(sc.configs) == 1 {
		tempConfig = Config{Num:1, Shards: [10]int{}, Groups: map[int][]string{}}
	} else {
		newestConfig := sc.configs[len(sc.configs)-1]
		tempConfig = Config{Num:newestConfig.Num+1, Shards: [10]int{}, Groups: map[int][]string{}}
		for index, gid := range newestConfig.Shards {
			tempConfig.Shards[index] = gid
		}
		for gid, groups := range newestConfig.Groups {
			tempConfig.Groups[gid] = groups
		}
	}
	return tempConfig
}

//func (sc *ShardCtrler) GenerateNewConfig(op Op)Config {
//	//tempConfig := sc.InitNewConfig()
//	tempConfig := Config{}
//	switch op.Operation {
//	case JoinOp :
//		tempConfig = *sc.doJoin(op.Servers_Join)
//		//for gid, servers := range op.Servers_Join {
//		//	tempConfig.Groups[gid] = servers
//		//}
//		//sc.BalanceJoin(&tempConfig,op.Servers_Join)
//	case LeaveOp :
//		tempConfig = *sc.doLeave(op.Gids_Leave)
//		//for _, gid := range op.Gids_Leave {
//		//	delete(tempConfig.Groups, gid)
//		//}
//		//sc.BalanceLeave(&tempConfig,op.Gids_Leave)
//	case MoveOp :
//		//_, exist := tempConfig.Groups[op.Shard_Move]
//		//if exist {
//		//	tempConfig.Shards[op.Shard_Move] = op.Gid_Move
//		//}
//		tempConfig = *sc.doMove(GIDandShard{GID: op.Gid_Move,Shard: op.Shard_Move})
//	}
//	//ShowConfig(tempConfig, op.Operation)
//	return tempConfig
//}

func ShowConfig(config Config, op string){
	DPrintf("=========== Config For op %v", op)
	DPrintf("[ConfigNum]%d",config.Num)
	for index, value := range config.Shards {
		DPrintf("[shards]Shard %d --> gid %d", index,value)
	}
	for gid,servers := range config.Groups {
		DPrintf("[Groups]Gid %d --> servers %v", gid, servers)
	}
}

func (sc *ShardCtrler) BalanceShardToGid(config *Config){
	length := len(config.Groups)
	average := NShards/length
	subNum := NShards - average*length
	avgNum := length - subNum
	// len(config.Groups) - subNum --> average
	// subNum --> average+1
	DPrintf("length %d, average %d",length,average)

	zeroAimGid := 0
	for gid,_ := range config.Groups {
		if gid != 0 {
			zeroAimGid = gid
		}
	}

	// countArray should not be have Gid 0
	for shards, gid := range config.Shards {
		if gid == 0 {
			config.Shards[shards] = zeroAimGid
		}
	}

	// count Every Gid mangement which Shard ???
	countArray := make(map[int][]int)
	for shardIndex,gid := range config.Shards {
		if _, exist := countArray[gid];exist {
			countArray[gid] = append(countArray[gid],shardIndex)
		} else {
			countArray[gid] = make([]int,0)
			countArray[gid] = append(countArray[gid], shardIndex)
		}
	}
	for gid,_ := range config.Groups {
		if _,exist := countArray[gid];!exist {
			countArray[gid] = make([]int,0)
		}
	}




	DPrintf("====countArray=====")
	for gid,shards := range countArray {
		DPrintf("[countarrya]gid %d, shards %v",gid,shards)
	}

	for {
		if ifBalance(average,avgNum,subNum,countArray){
			break
		}
		// make Max Gid One Shard to Min Gid
		maxShardsNum := -1
		maxGid := -1
		minShardsNum := NShards*10
		minGid := -1
		for gid, shardsArray := range countArray {
			if len(shardsArray) >= maxShardsNum {
				maxShardsNum = len(shardsArray)
				maxGid = gid
			}
			if len(shardsArray) <= minShardsNum {
				minShardsNum = len(shardsArray)
				minGid = gid
			}
		}

		fromGid := maxGid
		movedShard := countArray[maxGid][maxShardsNum-1]
		toGid := minGid

		//DPrintf("[Blance]Shard %d from Gid %d ===> Gid %d",movedShard, fromGid,toGid)
		countArray[fromGid] = countArray[fromGid][:maxShardsNum-1]
		countArray[toGid] = append(countArray[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}

}

func ifBalance(average int, avgNum int, subNum int, countArray map[int][]int) bool{
	shouldAvg := 0
	shouldAvgPlus := 0
	for gid, shards := range countArray {
		if len(shards) == average && gid != 0{
			shouldAvg++
		}
		if len(shards) == average+1 && gid != 0{
			shouldAvgPlus++
		}
	}

	return shouldAvg == avgNum && shouldAvgPlus == subNum
}

func (sc *ShardCtrler) ExecQueryOnController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.Num_Query == -1 || op.Num_Query >= len(sc.configs){
		return sc.configs[len(sc.configs) - 1]
	} else {
		return sc.configs[op.Num_Query]
	}

}

func (sc *ShardCtrler) ExecJoinOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, JOIN, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeJoinConfig(op.Servers_Join))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ExecLeaveOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, LEAVE, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeLeaveConfig(op.Gids_Leave))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ExecMoveOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, MOVE, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs,*sc.MakeMoveConfig(op.Shard_Move,op.Gid_Move))
	sc.mu.Unlock()
}


func (sc *ShardCtrler) ifRequestDuplicate(newClientId int64, newRequestId int) bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := sc.lastRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}


