package kvraft

import "6.824/raft"

func (kv *KVServer) ReadRaftApplyCommandLoop() {
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

// TODO : all the applied Command Should be execute , except "GET" on ~Leader
// TODO : if a WaitChan is waiting for the execute result, --> server is Leader
func (kv *KVServer) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)
	DPrintf("[RaftApplyCommand]Server %d , Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)

	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	// State Machine (KVServer solute the duplicate problem)
	// duplicate command will not be exed
	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
		// execute command
		if op.Operation == "put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == "append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
		// kv.lastRequestId[op.ClientId] = op.RequestId
	}

	if kv.maxraftstate != -1{
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex,9)
	}

	// Send message to the chan of op.ClientId
	kv.SendMessageToWaitChan(op,message.CommandIndex)
}

func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		DPrintf("[RaftApplyMessageSendToWaitChan-->]Server %d , Send Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		ch <- op
	}
	return exist
}

func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool){
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	if exist {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v",op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!",op.ClientId, op.RequestId, op.Key)
	}
	kv.DprintfKVDB()
	return value,exist
}

func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {

	kv.mu.Lock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v",op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

func (kv *KVServer) ExecuteAppendOpOnKVDB(op Op){
	//if op.IfDuplicate {
	//	return
	//}

	kv.mu.Lock()
	value,exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v",op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

func (kv *KVServer) ifRequestDuplicate(newClientId int64, newRequestId int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := kv.lastRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}