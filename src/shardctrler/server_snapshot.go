package shardctrler


import "bytes"
import "6.824/labgob"
import "6.824/raft"

func (kv *ShardCtrler) IfNeedToSendSnapShotCommand(raftIndex int, proportion int){
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate*proportion/10){
		// Send SnapShot Command
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}


// Handler the SnapShot from kv.rf.applyCh
func (kv *ShardCtrler) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		kv.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}



// TODO :  SnapShot include KVDB, lastrequestId map
// Give it to raft when server decide to start a snapshot
func (kv *ShardCtrler) MakeSnapShot() []byte{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.configs)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

func (kv *ShardCtrler) ReadSnapShotToInstall(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_config []Config
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_config) != nil || d.Decode(&persist_lastRequestId) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
	} else {
		kv.configs = persist_config
		kv.lastRequestId = persist_lastRequestId
	}
}
