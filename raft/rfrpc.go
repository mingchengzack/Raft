package raft

import "time"

//
// Raft RPC definitions and handlers
//

// RequestVoteArgs defines RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term        int // Candidate's term
	CandidateID int // Candidate that is requesting vote

	// LastLogIndex and LastLogTerm
}

// RequestVoteReply RequestVote RPC reply structure
type RequestVoteReply struct {
	Term        int  // Current term for peers to update itself
	VoteGranted bool // True means candidate received vote
}

// RequestVote defines the RPC handler for requesting vote from peers
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Receives stale term request
	if rf.currentTerm > args.Term {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.ConvertToFollower(args.Term)
	}

	// If votedFor is null or candidateId
	// and candidate’s log is at least as up-to-date as receiver’s log
	// grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true

		// Reset election timer only if GRANTING the vote
		rf.electionTimer = time.Now()
	}
}

// AppendEntriesArgs defines AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term     int        // Leader's term
	LeaderID int        // Leader's ID
	Entries  []LogEntry // Log entries to store (empty for heartbeat)
}

// AppendEntriesReply defines AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // Current term for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries defines the RPC handler for appending log entry from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// Receives stale term request
	if rf.currentTerm > args.Term {
		return
	}

	// If RPC request or response contains term T > currentTerm
	// Or it is a candidate and receive heartbeat from leader
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term ||
		(rf.currentTerm == args.Term && rf.state == Candidate) {
		rf.ConvertToFollower(args.Term)
	}

	// Reset election timer only if receives from CURRENT leader
	// (i.e term in arguments should not be outdated)
	rf.electionTimer = time.Now()
}

// For sending and receiving RPC
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//

// sendRequestVote sends a RPC request to ask for vote in leader election
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries sends a RPC request to append log entry or heartbeat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
