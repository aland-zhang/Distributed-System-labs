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

import "sync"
import "log"
import "time"
import "math/rand"
import "labrpc"

// import "bytes"
// import "encoding/gob"

type ServerState int

const (
	LeaderState    ServerState = 2
	CandidateState ServerState = 1
	FollowerState  ServerState = 0
)
const (
	ELECTION_TIME  = 150
	HEARTBEAT_TIME = 15
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu             sync.Mutex
	peers          []*labrpc.ClientEnd
	persister      *Persister
	me             int // index into peers[]
	state          ServerState
	currentTerm    int
	voteFor        map[int]*labrpc.ClientEnd
	heartbeatChan  chan bool
	beFollowerChan chan bool
	beLeaderChan   chan bool
	stopCountChan  chan bool
	countVoteChan  chan int
	countVotes     map[int]int
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LeaderState
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	CandidateID int
	Term        int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Agree bool
	Term  int
}

type AppendEntryArgs struct {
	LeaderID int
	Term     int
}

type AppendEntryReply struct {
	Agree bool
	Term  int
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.state != LeaderState) {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		reply.Agree = true
		log.Println(rf.me, "receives appendEntry", args.LeaderID, args.Term)
		if rf.state != FollowerState {
			rf.beFollowerChan <- true
		}
	}
	reply.Term = rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	reply.Term = args.Term
	rf.mu.Lock()
	log.Printf("%d: %d receives from %d, %d\n", rf.currentTerm, rf.me, args.CandidateID, args.Term)
	if args.Term > rf.currentTerm {
		reply.Agree = true
		rf.voteFor[args.Term] = rf.peers[args.CandidateID]
		rf.currentTerm = args.Term
		rf.beFollowerChan <- true
	} else {
		reply.Term = rf.currentTerm
		reply.Agree = false
	}

	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) getElectionTimeOut() time.Duration {
	return time.Duration(ELECTION_TIME+rand.Intn(ELECTION_TIME)) * time.Millisecond
}
func (rf *Raft) getHearBeatTimeOut() time.Duration {
	return time.Duration(HEARTBEAT_TIME) * time.Millisecond
}

//follower: 2 chans(restart, electionTimeOutChan)
// candidate: (electionTimeOutChan, beFollowerChan)
// leader: (appendEntryChan, beFollowerChan)
// total loop: (stateSwitchChan, )
// blocking
func (rf *Raft) broadcastRequestVote(args RequestVoteArgs) {
	if rf.state == FollowerState {
		return
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.state == FollowerState {
				return
			}
			go func(i int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(i, args, &reply); ok && rf.state == CandidateState {
					if reply.Agree {
						rf.countVoteChan <- args.Term
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) countVotesLoop() {
	var term int
	for {
		select {
		case term = <-rf.countVoteChan:
			rf.mu.Lock()
			if rf.currentTerm <= term {
				rf.countVotes[term]++
				if rf.countVotes[term] > len(rf.peers)/2 {
					rf.beLeaderChan <- true
				}
			}
			rf.mu.Unlock()
		case <-rf.stopCountChan:
			return
		}
	}
}

//blocking
func (rf *Raft) broadcastAppendEntries(args AppendEntryArgs) {
	if rf.state != LeaderState {
		return
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.state != LeaderState {
				return
			}
			go func() {
				reply := AppendEntryReply{}
				if ok := rf.sendAppendEntry(i, args, &reply); ok && !reply.Agree {
					log.Println(i, "refuse appendEntry from ", rf.me)
				}
			}()
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FollowerState
	rf.currentTerm = 0
	rf.voteFor = make(map[int]*labrpc.ClientEnd)
	rf.heartbeatChan = make(chan bool)
	rf.beFollowerChan = make(chan bool)
	rf.beLeaderChan = make(chan bool)
	rf.stopCountChan = make(chan bool)
	rf.countVoteChan = make(chan int, 100)
	rf.countVotes = make(map[int]int)
	// Your initialization code here.
	go rf.countVotesLoop()
	go func() {
		for {
			switch rf.state {
			case FollowerState:
				select {
				case <-time.After(rf.getElectionTimeOut()):
					rf.state = CandidateState
				case <-rf.heartbeatChan:
				case <-rf.beFollowerChan:
				}
			case CandidateState:
				//what if candidate has become follower now?
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.voteFor[rf.currentTerm] = rf.peers[rf.me]
				rf.countVotes[rf.currentTerm]++
				args := RequestVoteArgs{
					CandidateID: rf.me,
					Term:        rf.currentTerm,
				}
				rf.mu.Unlock()
				go rf.broadcastRequestVote(args)
				select {
				case <-rf.beLeaderChan:
					rf.state = LeaderState
				case <-rf.beFollowerChan:
					rf.state = FollowerState
				case <-time.After(rf.getElectionTimeOut()):
				}
			case LeaderState:
				rf.mu.Lock()
				args := AppendEntryArgs{
					LeaderID: rf.me,
					Term:     rf.currentTerm,
				}
				rf.mu.Unlock()
				go rf.broadcastAppendEntries(args)
				select {
				case <-rf.beFollowerChan:
					rf.state = FollowerState
				case <-time.After(rf.getHearBeatTimeOut()):
				}
			}

		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
