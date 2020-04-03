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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// ApplyMsg defines structure below
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
	ElectionInterval = 300

	// HeartbeatInterval (ms)
	// Make sure leader sends heartbeat RPCs no more than ten times per second
	// 8 times per second
	HeartbeatInterval = 200
)

// LogEntry defines the replicated log entry
// each entry includes command for state machine
// and the index for the command in the log
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
	currentTerm int        // Latest term server has seen (initialized to 0)
	votedFor    int        // CandidateId that received vote in current term
	log         []LogEntry // Log entries (commands and term)

	// Volatile state on all servers
	commitIndex int // Index of highest log entry committed (initialized to 0)
	lastApplied int // Index of highest log entry applied (initialized to 0)

	// Volatile state on leaders
	nextIndex  []int // For each server, index of the next log entry to send (initialized to leader last log index + 1)
	matchIndex []int // For each server, index of highest log entry known to be replicated on server (initialized to 0)
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Acquire lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := (rf.state == Leader && !rf.killed())

	return term, isLeader
}

// persist saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// getLastLog is a helper function that gets the last log's index and term
// If log is empty return (0, 0)
// Assuming log index is the current position in log (no snapshot)
func (rf *Raft) getLastLog() (lastLogIndex, lastLogTerm int) {
	lastLogIndex = 0
	lastLogTerm = 0
	if len(rf.log) != 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

// getPrevLog is a helper function that gets the prev log for a
// sever preceding the new ones needed to send
// Assuming log index is the current position in log (no snapshot)
func (rf *Raft) getPrevLog(server int) (prevLogIndex, prevLogTerm int) {
	prevLogIndex = rf.nextIndex[server] - 1
	prevLogTerm = 0
	if prevLogIndex != 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

// Start starts the next command sent by client
// If the server is leader
// Starts
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
	term = rf.currentTerm

	// Try to append log to itself and other server's
	le := LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	}

	// First append log to self
	rf.log = append(rf.log, le)
	go rf.appendLog(le)

	return
}

// appendLog is a goroutine that tries to append log itself
// and then try to replicate it to other servers
func (rf *Raft) appendLog(le LogEntry) {
	rf.mu.Lock()

	// Make sure it's still leader and not dead
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	replicates := 1
	rf.mu.Unlock()

	// Send request to each peer to append log entry
	// if last log index ≥ nextIndex
	for p := 0; p < len(rf.peers); p++ {
		// Excludes itself and last log index < nextIndex
		if p == rf.me {
			continue
		}

		// Attempts to send AppendEntries RPC to each peer
		go func(server int) {
			// Try sending AppendEntries until success
			for {
				// Stop if current server stop being a leader
				// or is dead
				// or peer is already appended
				rf.mu.Lock()
				lastLogIndex, _ := rf.getLastLog()
				if rf.killed() || rf.state != Leader ||
					lastLogIndex < rf.nextIndex[server] {
					rf.mu.Unlock()
					return
				}

				prevLogIndex, prevLogTerm := rf.getPrevLog(server)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      rf.log[prevLogIndex:lastLogIndex],
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				// Send AppendEntries RPC to peer
				// Don't want to lock while sending RPC request
				reply := &AppendEntriesReply{}
				if ok := rf.sendAppendEntries(server, args, reply); !ok {
					return
				}

				// Acquire lock
				rf.mu.Lock()

				// Process the reply only when current term doesn't change
				// between sending RPC and receiving RPC
				if rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				if rf.currentTerm < reply.Term {
					rf.convertToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				// Success from peer, found matched log
				if reply.Success {
					// Update nextIndex and matchIndex
					if args.PrevLogIndex+len(args.Entries) <= rf.matchIndex[server] {
						rf.mu.Unlock()
						return
					}
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					replicates++

					// If replicated on majority of peers
					// Rules to update commitIndex
					if n := rf.matchIndex[server]; replicates > len(rf.peers)/2 &&
						n > rf.commitIndex && rf.log[n-1].Term == rf.currentTerm {
						rf.commitIndex = n

						// Activate goroutines that check for apply
						rf.cond.Broadcast()
					}
					rf.mu.Unlock()
					return
				}

				// Log inconsistent
				// Peer doesn't have prevLogIndex in its log
				if reply.Xterm == 0 {
					rf.nextIndex[server] = reply.XLen
				} else {
					rf.nextIndex[server] = reply.XIndex

					// Search the confliting term in log
					i := len(rf.log) - 1
					for i >= 0 && rf.log[i].Term != reply.Xterm {
						i--
					}

					// Found the term
					if i >= 0 {
						rf.nextIndex[server] = rf.log[i].Index
					}
				}

				rf.mu.Unlock()

				// Retry
			}
		}(p)
	}
}

// apply is a goroutine that applies commits to local state machine
func (rf *Raft) apply(logEntries []LogEntry) {
	for _, le := range logEntries {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: le.Command, CommandIndex: le.Index}
	}
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Fire election timeout for leader election
	go rf.electionTimeout()

	// Fire check commit goroutine that apply log entries
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
		electionTimeout := ElectionInterval + rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		// Check the current server is dead
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Check if election timeout
		if rf.electionTimer.Before(startTime) {
			// First time election (Follower)
			// Re-election (Candidate)
			if rf.state != Leader {
				go rf.leaderElection()
			}
		}

		rf.mu.Unlock()
	}
}

// checkCommit is the background goroutine that
// checks if commitIndex > lastApplied using conditional variables
// If needs to apply, apply all the log entries to state machine
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

		// Applied log entries to local state
		rf.apply(rf.log[rf.lastApplied:rf.commitIndex])
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}
}

// leaderElection is the goroutine that starts the leader election process
func (rf *Raft) leaderElection() {
	rf.mu.Lock()

	// Make sure it's not dead
	if rf.killed() {
		rf.mu.Unlock()
		return
	}

	// Becomes a candidate
	rf.convertToCandidate()

	// Construct RequestVote RPC arguments
	lastLogIndex, lastLogTerm := rf.getLastLog()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1
	rf.mu.Unlock()

	// Send requests vote to each peer
	for p := 0; p < len(rf.peers); p++ {
		// Excludes itself
		if p == rf.me {
			continue
		}

		go func(server int) {
			// No longer a candidate just return
			rf.mu.Lock()
			if rf.killed() || rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock() // Don't want to lock while sending RPC request

			// Send out request for vote, ok to check if the server replys
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server, args, reply); !ok {
				return
			}

			// Acquire locks
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Process the reply only when current term doesn't change
			// between sending RPC and receiving RPC
			// and is still a candidate
			if rf.currentTerm != args.Term || rf.state != Candidate {
				return
			}

			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower
			if rf.currentTerm < reply.Term {
				rf.convertToFollower(reply.Term)
				return
			}

			// If got vote
			if reply.VoteGranted {
				votes++
			}

			// Candidate has majority of votes becomes leader
			if votes > len(rf.peers)/2 {
				rf.convertToLeader()

				// Act as a leader for each peer server
				for s := 0; s < len(rf.peers); s++ {
					if s == rf.me {
						continue
					}
					go rf.sendHeartbeat(s)
				}
			}
		}(p)
	}

}

// performLeader is a background goroutine
// It sends out heartbeat and AppendEntries RPC to other servers
func (rf *Raft) sendHeartbeat(server int) {
	// Performs periodic leader task
	for {
		rf.mu.Lock()

		// Stop if current server stop being a leader or is dead
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		lastLogIndex, _ := rf.getLastLog()
		prevLogIndex, prevLogTerm := rf.getPrevLog(server)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[prevLogIndex:lastLogIndex],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		// Send heartbeat to given server
		success := make(chan bool)
		go rf.performHeartbeat(server, args, success)

		// RPC timeout
		select {
		case <-success:
		case <-time.After(2 * time.Duration(HeartbeatInterval) * time.Millisecond):
		}
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}

}

// performHeartbeat is the goroutine for sending heartbeat
// and handles the reply from follower for appending log entry
func (rf *Raft) performHeartbeat(server int, args *AppendEntriesArgs, success chan bool) {
	// Stop if current server stop being a leader or is dead
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// Send RPC request
	// Don't want to lock while sending RPC request
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		return
	}
	success <- true

	// Acquire lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process the reply only when current term doesn't change
	// between sending RPC and receiving RPC
	if rf.currentTerm != args.Term {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
		return
	}

	// Success from peer, found matched log
	if reply.Success {
		// Update nextIndex and matchIndex
		if args.PrevLogIndex+len(args.Entries) <= rf.matchIndex[server] {
			return
		}
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// No need to update commit index
		n := rf.matchIndex[server]
		if n <= rf.commitIndex {
			return
		}

		// Iterate all servers to check replicated number
		replicates := 1
		for p := 0; p < len(rf.peers); p++ {
			if p == rf.me {
				continue
			}
			if rf.matchIndex[p] >= n {
				replicates++
			}
		}

		// If replicated on majority of peers
		// Rules to update commitIndex
		if replicates > len(rf.peers)/2 &&
			rf.log[n-1].Term == rf.currentTerm {
			rf.commitIndex = n

			// Activate goroutines that check for apply
			rf.cond.Broadcast()
		}

		return
	}

	// Log inconsistent
	// Peer doesn't have prevLogIndex in its log
	if reply.Xterm == 0 {
		rf.nextIndex[server] = reply.XLen
	} else {
		rf.nextIndex[server] = reply.XIndex

		// Search the confliting term in log
		i := len(rf.log) - 1
		for i >= 0 && rf.log[i].Term != reply.Xterm {
			i--
		}

		// Found the term
		if i >= 0 {
			rf.nextIndex[server] = rf.log[i].Index
		}
	}
}

// ConvertToFollower converts the server to a follower
// Assuming lock is acquired
func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

// ConvertToCandidate converts the server to a candidate
// Assuming lock is acquired
func (rf *Raft) convertToCandidate() {
	// Increment currentTerm
	// Vote for self
	// Reset election timer
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer = time.Now()
}

// ConvertToLeader converts the server to a leader
// Assuming lock is acquired
func (rf *Raft) convertToLeader() {
	rf.state = Leader

	// Re-initialize nextIndex and matchIndex
	lastLogIndex := 0
	if len(rf.log) != 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	rf.electionTimer = time.Now()
}
