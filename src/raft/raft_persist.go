package raft

import (
	"bytes"
	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) GetRaftStateSize() int{
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistData() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)
	//e.Encode(rf.persister.ReadSnapshot())
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persist_currentTrem int
	var persist_voteFor int
	var persist_log []Entry
	//var persist_lastApplied int
	var persist_lastSSPointIndex int
	var persist_lastSSPointTerm int
	//var persist_snapshot []byte

	if d.Decode(&persist_currentTrem) != nil ||
		d.Decode(&persist_voteFor) != nil ||
		d.Decode(&persist_log) != nil ||
		//d.Decode(&persist_lastApplied) != nil ||
		d.Decode(&persist_lastSSPointIndex) != nil ||
		d.Decode(&persist_lastSSPointTerm) != nil {
		//d.Decode(&persist_snapshot) != nil{
		DPrintf("%d read persister got a problem!!!!!!!!!!",rf.me)
	} else {
		rf.currentTerm = persist_currentTrem
		rf.votedFor = persist_voteFor
		rf.log = persist_log
		// rf.lastApplied = persist_lastApplied
		rf.lastSSPointIndex = persist_lastSSPointIndex
		rf.lastSSPointTerm = persist_lastSSPointTerm
		// rf.persister.SaveStateAndSnapshot(rf.persistData(),persist_snapshot)
	}
}