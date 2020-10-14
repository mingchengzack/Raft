package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

// ApplyMsg defines structure below
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// ServerState defines the state of a Raft peer
type ServerState int

const (
	// Follower state
	Follower ServerState = iota

	// Candidate state
	Candidate

	// Leader state
	Leader
)

const (
	// ElectionInterval (ms)
	// The interval for election timeout
	ElectionInterval = 200

	// HeartbeatInterval (ms)
	// Make sure leader sends heartbeat RPCs no more than ten times per second
	// 10 times per second
	HeartbeatInterval = 100
)

// LogEntry defines the replicated Log entry
// each entry includes command for state machine
// and the index for the command in the Log
// and term received by leader (first index is 1)
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

// Raft defines a Raft server
// A Go object implementing a single Raft peer
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cond      *sync.Cond          // Conditional variables to handle repeated check for certain events
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()
	applyCh   chan ApplyMsg       // Channel to send apply message

	// Raft server's state
	state         ServerState
	electionTimer time.Time // The last time the server has heard from another peer

	// Persistent state on this peer
	CurrentTerm int        // Latest term server has seen (initialized to 0)
	VotedFor    int        // CandidateId that received vote in current term
	Log         []LogEntry // Log entries (commands and term)

	// Volatile state on all servers
	commitIndex int // Index of highest Log entry committed (initialized to 0)
	lastApplied int // Index of highest Log entry applied (initialized to 0)

	// Volatile state on candidates
	voteCount int // current count of received votes

	// Volatile state on leaders
	nextIndex  []int // For each server, index of the next Log entry to send (initialized to leader last Log index + 1)
	matchIndex []int // For each server, index of highest Log entry known to be replicated on server (initialized to 0)
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//

// Kill kills the current Raft peer
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed checks if the current Raft peer is dead
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// GetState returns CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Acquire lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.CurrentTerm
	isLeader := (rf.state == Leader && !rf.killed())

	return term, isLeader
}

// ConvertToFollower converts the server to a follower
// Assuming lock is acquired
func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.voteCount = 0

	rf.persist()

	// Fire election timeout for leader election
	go rf.electionTimeout()
}

// ConvertToCandidate converts the server to a candidate
// Assuming lock is acquired
func (rf *Raft) convertToCandidate() {
	// Increment CurrentTerm
	// Vote for self
	// Reset election timer
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.voteCount = 1
	rf.electionTimer = time.Now()

	rf.persist()
}

// ConvertToLeader converts the server to a leader
// Assuming lock is acquired
func (rf *Raft) convertToLeader() {
	rf.state = Leader

	// Re-initialize nextIndex and matchIndex and matchCount
	lastLogIndex := 0
	if len(rf.Log) != 0 {
		lastLogIndex = rf.Log[len(rf.Log)-1].Index
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

// getLastLog is a helper function that gets the last Log's index and term
// If Log is empty return (0, 0)
// Assuming Log index is the current position in Log (no snapshot)
func (rf *Raft) getLastLog() (lastLogIndex, lastLogTerm int) {
	lastLogIndex = 0
	lastLogTerm = 0
	if len(rf.Log) != 0 {
		lastLogIndex = rf.Log[len(rf.Log)-1].Index
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	return
}

// getPrevLog is a helper function that gets the prev Log for a
// sever preceding the new ones needed to send
// Assuming Log index is the current position in Log (no snapshot)
func (rf *Raft) getPrevLog(server int) (prevLogIndex, prevLogTerm int) {
	prevLogIndex = rf.nextIndex[server] - 1
	prevLogTerm = 0
	if prevLogIndex != 0 {
		prevLogTerm = rf.Log[prevLogIndex-1].Term
	}
	return
}

// persist saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ct int
	var vf int
	var l []LogEntry
	if d.Decode(&ct) != nil ||
		d.Decode(&vf) != nil ||
		d.Decode(&l) != nil {
		log.Fatal("Decode Persistor error")
	} else {
		rf.CurrentTerm = ct
		rf.VotedFor = vf
		rf.Log = l
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

// Start starts the next command sent by client
// If the server is leader, append Log to itself
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = 0
	term = 0
	isLeader = false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Return false if server is not leader or killed
	if rf.killed() || rf.state != Leader {
		return
	}

	isLeader = true
	lastLogIndex, _ := rf.getLastLog()
	index = lastLogIndex + 1
	term = rf.CurrentTerm

	// First append Log to self
	le := LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.Log = append(rf.Log, le)

	rf.persist()

	return
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

// Make creates a Raft server
// given its peers, its id, its state persister and the channel to send msg
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// Initialization on first boot
	rf.state = Follower
	rf.electionTimer = time.Now()
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Fire election timeout for leader election
	go rf.electionTimeout()

	// Fire check commit goroutine that apply Log entries
	go rf.checkCommit()

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// electionTimeout is the background goroutine that
// creates the leader election time out in an infinite for loop
// and performs leader election if randomized election timeouts
func (rf *Raft) electionTimeout() {
	// Periodic check the election timeout for leader election
	for {
		// Election timeout is 200 - 400 ms
		timeout := ElectionInterval + rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		// Check the current server is dead or becomes leader
		rf.mu.Lock()
		if rf.killed() || rf.state == Leader {
			rf.mu.Unlock()
			return
		}

		// Check if election timeout
		if rf.electionTimer.Before(startTime) {
			// First time election (Follower)
			// Re-election (Candidate)
			if rf.state != Leader {
				// Becomes a candidate
				rf.convertToCandidate()

				// Broadcast requests votes to perform leader election
				go rf.broadcastRequestVote()
			}
		}

		rf.mu.Unlock()
	}
}

// checkCommit is the background goroutine that
// checks if commitIndex > lastApplied using conditional variables
// If needs to apply, apply all the Log entries to state machine
func (rf *Raft) checkCommit() {
	// Periodic loop to check for commit
	for {
		// Check the current server is dead
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Use conditional variable to check if commitIndex is updated
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait() // Should wakes up after commitIndex changes
		}

		// Applied Log entries to local state
		rf.apply(rf.Log[rf.lastApplied:rf.commitIndex])
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}
}

// broadcastRequestVote is the goroutine that starts the leader election process
func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()

	// Make sure it's not dead and still a candidate
	if rf.killed() || rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	// Construct RequestVote RPC arguments
	lastLogIndex, lastLogTerm := rf.getLastLog()
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	// Send requests vote to each peer
	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			// Send out request for vote, ok to check if the server replys
			go rf.sendRequestVote(p, args)
		}
	}
}

// broadcastAppendEntries is a background goroutine
// Leader sends out AppendEntries RPC to each server
func (rf *Raft) broadcastAppendEntries() {
	for {
		rf.mu.Lock()

		// Stop if current server stop being a leader or is dead
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// Check if we need to update commit index
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and Log[N].term == CurrentTerm
		// set commitIndex = N
		lastLogIndex, _ := rf.getLastLog()
		index := rf.commitIndex
		for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
			replicates := 1
			for p := range rf.peers {
				if p != rf.me && rf.matchIndex[p] >= i && rf.Log[i-1].Term == rf.CurrentTerm {
					replicates++
				}
			}
			if replicates > len(rf.peers)/2 {
				index = i
			}
		}
		if index != rf.commitIndex {
			rf.commitIndex = index

			// Activate goroutines that check for apply
			rf.cond.Broadcast()
		}

		// Send each peer AppendEntries RPC
		for p := 0; p < len(rf.peers); p++ {
			if p != rf.me {
				prevLogIndex, prevLogTerm := rf.getPrevLog(p)
				args := &AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      rf.Log[prevLogIndex:lastLogIndex],
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAppendEntries(p, args)
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

// apply is a goroutine that applies commits to local state machine
func (rf *Raft) apply(logEntries []LogEntry) {
	for _, le := range logEntries {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: le.Command, CommandIndex: le.Index}
	}
}
