package shardkv

import "6.824/raft"
func (kv *ShardKV) ReadRaftApplyCommandLoop() {
	for message := range kv.applyCh{
		// listen to every command applied by its raft ,delivery to relative RPC Handler
		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}

	}
}

func (kv *ShardKV) ifRequestDuplicate(newClientId int64, newRequestId int, shardNum int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := kv.kvDB[shardNum].ClientRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}

// TODO : all the applied Command Should be execute , except "GET" on ~Leader
// TODO : if a WaitChan is waiting for the execute result, --> server is Leader
func (kv *ShardKV) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)
	//DPrintf("[RaftApplyCommand]Gid %d, Server %d , Op-->Index:%d ,Opreation %v",kv.gid, kv.me, message.CommandIndex, op.Operation)

	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	// newConfig && migrate Command don't contains clientid and requestid
	// nothing about duplicate detection & no waitForChan
	if op.Operation == NEWCONFIGOp {
		kv.ExecuteNewConfigOpOnServer(op)
		if kv.maxraftstate != -1{
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
		}
		return
	}

	if op.Operation == MIGRATESHARDOp {
		kv.ExecuteMigrateShardsOnServer(op)
		if kv.maxraftstate != -1{
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
		}
		kv.SendMessageToWaitChan(op,message.CommandIndex)
		return
	}

	// State Machine (KVServer solute the duplicate problem)
	// duplicate command will not be exed
	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		// execute command
		if op.Operation == PUTOp {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == APPENDOp {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1{
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	// Send message to the chan of op.ClientId
	kv.SendMessageToWaitChan(op,message.CommandIndex)
}

func (kv *ShardKV) SendMessageToWaitChan(op Op, raftIndex int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		//DPrintf("[RaftApplyMessageSendToWaitChan-->]Server %d , Send Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		ch <- op
	}
	return exist
}

func (kv *ShardKV) ExecuteGetOpOnKVDB(op Op) (string, bool){
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	//kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	//if exist {
	//	DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v",op.ClientId, op.RequestId, op.Key, value)
	//} else {
	//	DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!",op.ClientId, op.RequestId, op.Key)
	//}
	//kv.DprintfKVDB()
	return value,exist
}

func (kv *ShardKV) ExecutePutOpOnKVDB(op Op) {

	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	//DPrintf("[ShardNum]%d, len kvDB %d",shardNum,len(kv.kvDB))
	kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	//kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	//DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v",op.ClientId, op.RequestId, op.Key, op.Value)
	//kv.DprintfKVDB()
}

func (kv *ShardKV) ExecuteAppendOpOnKVDB(op Op){
	//if op.IfDuplicate {
	//	return
	//}
	kv.mu.Lock()
	shardNum := key2shard(op.Key)
	value,exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	if exist {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = value + op.Value
	} else {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	}
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
	//kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	//DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v",op.ClientId, op.RequestId, op.Key, op.Value)
	//kv.DprintfKVDB()
}

// TODO ========================= EXE New Config  =========================
func (kv *ShardKV) lockMigratingShard(newShards [NShards]int) {

	oldShards := kv.config.Shards
	for shard := 0;shard < NShards;shard++ {
		// new Shards own to myself
		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			if newShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
		// old Shards not ever belong to myself
		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			if oldShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
	}
}

func (kv *ShardKV) ExecuteNewConfigOpOnServer(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newestConfig := op.Config_NEWCONFIG
	if newestConfig.Num != kv.config.Num+1 {
		return
	}
	// all migrate shard should be finished
	for shard := 0; shard < NShards;shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}
	// should be apply this config
	//if newestConfig.Shards != kv.config.Shards {
	//	DPrintf("[NewConfigNeedMigRate]Gid %d, Server %d, newestConfigNum %d, nowConfigNum %d",kv.gid,kv.me, newestConfig.Num, kv.config.Num)
	//	DPrintf("[OldShards]%v", kv.config.Shards)
	//	DPrintf("[NewShards]%v", newestConfig.Shards)
	//}
	//DPrintf("[ApplyNewConfig]Gid %d, Server %d, newestConfigNum %d",kv.gid,kv.me, newestConfig.Num)
	kv.lockMigratingShard(newestConfig.Shards)
	kv.config = newestConfig
}

// TODO ========================= Exe new Migrate  =========================
func (kv *ShardKV) ExecuteMigrateShardsOnServer(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	//DPrintf("[GotMigrate]Gid %d, fromConfig %d, myConfig %d",kv.gid,op.ConfigNum_MIGRATE,myConfig.Num)
	if op.ConfigNum_MIGRATE != myConfig.Num {
		return
	}
	//DPrintf("[BeginMigrate]Gid %d, fromConfig %d, myConfig %d",kv.gid,op.ConfigNum_MIGRATE,myConfig.Num)
	//DPrintf("[args.SendData]%v", op.MigrateData_MIGRATE)
	//DPrintf("[BeforeExe]-----")
	//kv.DprintfKVDB()
	// apply the MigrateShardData On myselt
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if !kv.migratingShard[shardComponent.ShardIndex] {
			continue
		}
		kv.migratingShard[shardComponent.ShardIndex] = false
		kv.kvDB[shardComponent.ShardIndex] = ShardComponent{ShardIndex: shardComponent.ShardIndex,KVDBOfShard: make(map[string]string),ClientRequestId: make(map[int64]int)}
		// new shard belong to myself

		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			CloneSecondComponentIntoFirstExceptShardIndex(&kv.kvDB[shardComponent.ShardIndex],shardComponent)
		}
	}
	//DPrintf("[AfterExe]-----")
	//kv.DprintfKVDB()

}

func CloneSecondComponentIntoFirstExceptShardIndex (component *ShardComponent, recive ShardComponent) {
	for key,value := range recive.KVDBOfShard {
		component.KVDBOfShard[key] = value
	}
	for clientid,requestid := range recive.ClientRequestId {
		component.ClientRequestId[clientid] = requestid
	}
}