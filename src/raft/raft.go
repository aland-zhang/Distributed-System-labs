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
	Leader ServerState = 2
	Candidate ServerState = 1
	Follower ServerState = 0
)
const (
	TIME_INTERVAL = 150
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
	mu          sync.Mutex
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int // index into peers[]
	state       ServerState
	currentTerm int
	leader 		int
	voteFor 		int
	refreshChan chan bool
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
	term = rf.currentTerm
	isleader = rf.leader == rf.me
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
	Candidate int
	Term int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Agree bool
	Term int
}

type AppendEntryArgs struct {
	Leader int
	Term int
}

type AppendEntryReply struct {
	Agree bool
	Term int
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term || rf.currentTerm == args.Term && rf.leader == -1 {
		if rf.leader != args.Leader {
			rf.leader = args.Leader
		}
		if rf.voteFor != -1 {
			rf.voteFor = -1
		}
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		reply.Agree = true
		log.Println(rf.me, "receives appendEntry", args.Leader, args.Term)
		rf.refreshChan <- true
	}  else {
		reply.Agree = false		
	}
	reply.Term = rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm || args.Term == rf.currentTerm && rf.voteFor == -1 {
		reply.Agree = true
		rf.voteFor = args.Candidate
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		if rf.leader != -1 {
			rf.leader = -1
		}
		log.Println(rf.me, "agree candidate", args.Candidate)
	}
	rf.refreshChan <- true
	reply.Term = rf.currentTerm
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

func (rf *Raft) getTimeInterval() time.Duration {
	if rf.leader == rf.me {
		return 50 * time.Millisecond
	}
	return time.Duration(TIME_INTERVAL + rand.Intn(TIME_INTERVAL)) * time.Millisecond
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.leader = -1
	rf.refreshChan = make(chan bool)
	// Your initialization code here.
	go func() {
		for {
			select {
			case <-rf.refreshChan:
			case <- time.After(rf.getTimeInterval()):
				rf.mu.Lock()
				if rf.leader == rf.me {
					//send AppendEntries
					args := AppendEntryArgs {
						Term: rf.currentTerm,
						Leader: rf.me,
					}
					log.Printf("%d: %d sendAppendEntry.", rf.currentTerm, rf.me)
					reply := AppendEntryReply{}
					for i, _ := range rf.peers {
						if i != rf.me {
							if ok := rf.sendAppendEntry(i, args, &reply); !ok {
								log.Printf("%d: %d not respond to leader %d\n", rf.currentTerm, i, rf.me)
							}
						}
					}
				} else {
					//become a candidate
					rf.leader = -1
					rf.currentTerm += 1
					rf.voteFor = rf.me
					args := RequestVoteArgs {
						Candidate: rf.me,
						Term: rf.currentTerm,
					}
					log.Printf("%d: %d sendRequestVote.", rf.currentTerm, rf.me)
					reply := RequestVoteReply{}
					count := 1
					for i, _ := range rf.peers {
						if i != rf.me {
							if ok := rf.sendRequestVote(i, args, &reply); !ok {
								log.Printf("%d: %d not respond to candidate %d, %d\n", rf.currentTerm, i, rf.me)
							}
							if reply.Agree {//default to false
								count += 1
							}
						}
					}
					log.Println(count, len(rf.peers)/2)
					if count > len(rf.peers)/2 {//count vote for itself
						rf.leader = rf.me
						log.Printf("%d become leader!\n", rf.me)
					} 
					rf.voteFor = -1
				}
				rf.mu.Unlock()
			}
		}
	} ()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
