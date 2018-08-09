package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"sync"
	"time"
)

type PeerState rune

const HeartbeatInterval = 100
const NotVoted = -1
const (
	Follower PeerState = iota
	Candidate
	Leader
)

func (ps PeerState) String() string {
	switch ps {
	case 0:
		return "Follower"
	case 1:
		return "Candidate"
	case 2:
		return "Leader"
	default:
		return "Undefined"
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	nextIndex  []int // for each server index of the next log entry to send to that server
	matchIndex []int // for each server index of highest log entry known to be replicated on server

	state PeerState // server current state (Follower, Candidate or Leader)

	notifyToApplyCh         chan bool     // notify apply() to apply log
	applyCh                 chan ApplyMsg // for sending message to application
	exitCh                  chan bool     // exit signal
	appendEntriesReceivedCh chan bool     // received AppendEntries signal, for election timeout
	voteGrantedCh           chan bool     // granted vote signal, for election timeout
	leaderCh                chan bool     // become leader signal, used in main loop

	lastIncludedIndex int // for log compaction
	lastIncludedTerm  int // for log compaction
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastApplied)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied, lastIncludedIndex, lastIncludedTerm, currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&lastApplied) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("[raft] [readPersist] [server %v]: error in reading persisted state", rf.me)
	} else {
		rf.lastApplied = lastApplied
		DPrintf("[raft] [readPersist] [server %v]: set lastApplied to %v", rf.me, rf.lastApplied)

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		DPrintf("[raft] [readPersist] [server %v]: set lastIncludedIndex to %v, set lastIncludedTerm to %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		DPrintf("[raft] [readPersist] [server %v]: updated log to %v", rf.me, rf.log)
	}
}

func (rf *Raft) applyLogEntry() {
	// rules for all servers 1
	for {
		select {
		case <-rf.notifyToApplyCh:
			DPrintf("[raft] [applyLogEntry] [server %v]: notified to apply", rf.me)

			rf.mu.Lock()
			var applyMsgs []ApplyMsg
			DPrintf("[raft] [applyLogEntry] [server %v]: commitIndex: %v, lastApplied: %v", rf.me, rf.commitIndex, rf.lastApplied)
			for rf.commitIndex > rf.lastApplied {
				logEntry := rf.getLogByIndex(rf.lastApplied + 1)
				DPrintf("[raft] [applyLogEntry] [server %v]: get log at index %v: %v", rf.me, rf.lastApplied+1, logEntry)
				var msg ApplyMsg
				if value, ok := logEntry.Command.(string); ok {
					msg = ApplyMsg{false, value, logEntry.Index, logEntry.Term}
				} else {
					msg = ApplyMsg{true, logEntry.Command, logEntry.Index, logEntry.Term}
				}
				applyMsgs = append(applyMsgs, msg)
				rf.lastApplied++
			}

			DPrintf("[raft] [applyLogEntry] [server %v]: will apply all logs before rf.commitIndex %v, set lastApplied to %v", rf.me, rf.commitIndex, rf.lastApplied)
			rf.mu.Unlock()

			for _, msg := range applyMsgs {
				rf.applyCh <- msg
				DPrintf("[raft] [applyLogEntry] [server %v]: sent log at index %v to applier: %v", rf.me, msg.CommandIndex, msg)
			}

			rf.persist()
		case <-rf.exitCh:
			return
		}
	}
}

func (rf *Raft) SaveSnapshot(lastCommandIndex int, lastCommandTerm int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if rf.lastIncludedIndex < lastCommandIndex {
		newStartIndex := rf.getSliceIndex(lastCommandIndex) + 1
		newStartLogEntries := rf.log[newStartIndex:]
		rf.log = []LogEntry{{0, "EmptyCommand", 0}}
		rf.log = append(rf.log, newStartLogEntries...)
		DPrintf("[raft] [SaveSnapshot] [server %v]: updated log to %v", rf.me, rf.log)

		rf.lastIncludedIndex = lastCommandIndex
		rf.lastIncludedTerm = lastCommandTerm
		DPrintf("[raft] [SaveSnapshot] [server %v]: set lastIncludedIndex to %v, set lastApplied to %v", rf.me, rf.lastIncludedIndex, rf.lastApplied)

		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
		DPrintf("[raft] [SaveSnapshot] [server %v]: take a snapshot, lastIncludedIndex %v", rf.me, lastCommandIndex)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader

	if isLeader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		logEntry := LogEntry{index, command, rf.currentTerm}

		// rules for leaders 2
		rf.log = append(rf.log, logEntry)
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = index
	}

	return index, term, isLeader
}

func (rf *Raft) Restore() {
	rf.mu.Lock()

	firstRestoreIndex := 1
	DPrintf("[raft] [Restore] [server %v]: lastIncludedIndex %v, firstRestoreIndex", rf.me, rf.lastIncludedIndex, firstRestoreIndex)
	if rf.lastIncludedIndex >= firstRestoreIndex {
		rf.applyCh <- ApplyMsg{false, "ReadSnapshot", rf.lastIncludedIndex, rf.lastIncludedTerm}
		DPrintf("[raft] [Restore] [server %v]: notified applier to read snapshot, lastIncludedIndex %v", rf.me, rf.lastIncludedIndex)
		firstRestoreIndex = rf.lastIncludedIndex + 1
		rf.lastApplied = maxInt(rf.lastIncludedIndex, rf.lastApplied)
		DPrintf("[raft] [Restore] [server %v]: set lastApplied to %v", rf.me, rf.lastApplied)
	}
	entriesToRestore := rf.log[rf.getSliceIndex(firstRestoreIndex):rf.getSliceIndex(rf.lastApplied+1)]
	DPrintf("[raft] [Restore] [server %v]: will restore all logs start from index %v, to index %v", rf.me, firstRestoreIndex, rf.lastApplied)
	rf.mu.Unlock()

	for _, entry := range entriesToRestore {
		rf.applyCh <- ApplyMsg{true, entry.Command, entry.Index, entry.Term}
		DPrintf("[raft] [Restore] [server %v]: sent log at index %v to applier: %v", rf.me, entry.Index, entry)
	}
}

func (rf *Raft) Kill() {
	close(rf.exitCh)
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	receivedVotes := 1
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, args, reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					receivedVotes++
				}

				if rf.state == Candidate && receivedVotes >= len(rf.peers)/2+1 {
					rf.convertToLeader()
					dropAndSet(rf.leaderCh)
				}
			}
		}(i)
	}
}

func (rf *Raft) broadcast() {
	rf.mu.Lock()
	numPeers := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < numPeers; i++ {
		rf.mu.Lock()
		if i == rf.me {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		go func(i int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				DPrintf("[raft] [broadcast] [server %v]: rf.nextIndex[%v] %v <= rf.lastIncludedIndex %v", rf.me, i, rf.nextIndex[i], rf.lastIncludedIndex)
				DPrintf("[raft] [broadcast] [server %v]: will send InstallSnapshot (from server %v to server %v)", rf.me, rf.me, i)

				args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, 0, rf.persister.ReadSnapshot(), true}
				rf.mu.Unlock()
				reply := &InstallSnapshotReply{}
				rf.sendInstallSnapshot(i, args, reply)

				rf.mu.Lock()
				DPrintf("[raft] [broadcast] [server %v]: got InstallSnapshotReply %v", reply)
				if reply.Term == rf.currentTerm {
					DPrintf("[raft] [broadcast] [server %v]: succeed in sending InstallSnapshot (from server %v to server %v)", rf.me, rf.me, i)
					rf.matchIndex[i] = rf.lastIncludedIndex
					rf.nextIndex[i] = rf.lastIncludedIndex + 1
					DPrintf("[raft] [broadcast] [server %v]: set rf.matchIndex[%v] to %v, set rf.nextIndex[%v] to %v", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
				} else if reply.Term > rf.currentTerm {
					DPrintf("[raft] [broadcast] [server %v]: failed in sending InstallSnapshot failed (from server %v to server %v)", rf.me, rf.me, i)
					rf.convertToFollower(reply.Term)
				}
				rf.mu.Unlock()
			} else {
				DPrintf("[raft] [broadcast] [server %v]: rf.nextIndex[%v] %v > rf.lastIncludedIndex %v", rf.me, i, rf.nextIndex[i], rf.lastIncludedIndex)
				DPrintf("[raft] [broadcast] [server %v]: will send AppendEntries (from server %v to server %v)", rf.me, rf.me, i)

				prevLogIndex := rf.nextIndex[i] - 1
				var prevLogTerm int
				if inBetween(prevLogIndex, rf.getFirstStoredLogIndex(), rf.getLastStoredLogIndex()) {
					prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
				} else {
					prevLogIndex = rf.lastIncludedIndex
					prevLogTerm = rf.lastIncludedTerm
				}

				// rules for leaders 3
				var entries []LogEntry
				if rf.getLastStoredLogIndex() >= rf.nextIndex[i] {
					entries = rf.log[rf.getSliceIndex(rf.nextIndex[i]):]
				}

				args := &AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)

				DPrintf("[raft] [broadcast] [server %v]: got AppendEntriesReply %v", reply)
				if reply.Success {
					if len(entries) > 0 {
						rf.mu.Lock()
						rf.matchIndex[i] = entries[len(entries)-1].Index
						rf.nextIndex[i] = entries[len(entries)-1].Index + 1
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.matchIndex[i] = prevLogIndex
						rf.nextIndex[i] = prevLogIndex + 1
						rf.mu.Unlock()
					}

					DPrintf("[raft] [broadcast] [server %v]: succeed in sending AppendEntries (from server %v to server %v)", rf.me, rf.me, i)
					DPrintf("[raft] [broadcast] [server %v]: set rf.matchIndex[%v] to %v, set rf.nextIndex[%v] to %v", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
				} else {
					rf.mu.Lock()
					if reply.Term == rf.currentTerm {
						rf.nextIndex[i] = maxInt(1, reply.ConflictIndex)
					}
					DPrintf("[raft] [broadcast] [server %v]: failed in sending AppendEntries failed (from server %v to server %v)", rf.me, rf.me, i)
					DPrintf("[raft] [broadcast] [server %v]: set rf.matchIndex[%v] to %v, set rf.nextIndex[%v] to %v", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
					rf.mu.Unlock()
				}

				// rules for leaders 4
				rf.mu.Lock()
				N := getMajority(rf.matchIndex)
				if N != -1 && N > rf.commitIndex && rf.getLogByIndex(N).Term == rf.currentTerm {
					rf.commitIndex = N
					rf.notifyToApplyCh <- true
					DPrintf("[raft] [broadcast] [server %v]: more than half followers received logs at index %v, will commit log at index %v", rf.me, N, N)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) serve() {
	for {
		select {
		case <-rf.exitCh:
			return
		default:
		}

		rf.mu.Lock()
		timeout := randomElectionTimeout(300, 500)

		switch rf.state {
		case Follower:
			rf.mu.Unlock()
			select {
			case <-rf.appendEntriesReceivedCh:
			case <-rf.voteGrantedCh:
			case <-rf.leaderCh:
			case <-time.After(timeout * time.Millisecond):
				rf.mu.Lock()
				rf.convertToCandidate()
				rf.mu.Unlock()
			}
		case Candidate:
			rf.mu.Unlock()
			go rf.electLeader()
			select {
			case <-rf.appendEntriesReceivedCh:
			case <-rf.voteGrantedCh:
			case <-rf.leaderCh:
			case <-time.After(timeout * time.Millisecond):
				rf.mu.Lock()
				rf.convertToCandidate()
				rf.mu.Unlock()
			}
		case Leader:
			rf.mu.Unlock()
			rf.broadcast()
			time.Sleep(HeartbeatInterval * time.Millisecond)
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = []LogEntry{{0, "EmptyCommand", 0}}
	rf.state = Follower
	rf.votedFor = NotVoted

	rf.exitCh = make(chan bool, 1)
	rf.appendEntriesReceivedCh = make(chan bool, 1)
	rf.voteGrantedCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.applyCh = applyCh
	rf.notifyToApplyCh = make(chan bool, 100)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serve()
	go rf.applyLogEntry()

	return rf
}
