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
	mu            sync.Mutex
	indexMu       sync.Mutex
	peers         []*labrpc.ClientEnd
	num           int
	persister     *Persister
	me            int // index into peers[]
	state         ServerState
	currentTerm   int
	voteFor       map[int]*labrpc.ClientEnd
	heartbeatChan chan bool
	beLeaderChan  chan bool
	stopCountChan chan bool
	applyBufferCh	chan ApplyMsg
	countVoteChan chan int
	countVotes    map[int]int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	applyCh       chan ApplyMsg
	logs          []interface{}
	logTerm       []int
	uncommitQueue chan interface{}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getMax(a, b int) int {
	if a > b {
		return a
	}
	return b
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
	CandidateID  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
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
	LeaderID     int
	Term         int
	Entries      []interface{}
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntryReply struct {
	Success bool // If the entry at PrevLogIndex matches, return true
	Term    int
}

func (rf *Raft) ReceiveApplyMsg(applyMsg ApplyMsg) {
	rf.applyCh <- applyMsg
}

//if empty, sends to heartbeatChan
func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	log.Printf("%d: %d receives AppendEntry:%v", rf.currentTerm, rf.me, args)

	termMatch, logMatch := false, false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		termMatch = true
	} else if rf.currentTerm == args.Term && rf.state != LeaderState {
		termMatch = true
	}
	if len(rf.logTerm) > args.PrevLogIndex && rf.logTerm[args.PrevLogIndex] == args.PrevLogTerm {
		logMatch = true
	}
	// log.Println(rf.currentTerm, rf.me, " agrees appendEntry", args.LeaderID, args.Term)
	if termMatch {
		reply.Success = true
		if len(args.Entries) == 0 {
			go func() {
				rf.heartbeatChan <- true
			}()
		}
	}
	if logMatch && len(args.Entries) > 0 {
		hasConflict := false
		for i := 0; i < len(args.Entries); i++ {
			index := args.PrevLogIndex + i + 1
			if index < len(rf.logs) {
				// Replace on place: old ones
				rf.logs[index] = args.Entries[i]
				// Logs?
				if rf.logTerm[index] != args.Term {
					rf.logTerm[index] = args.Term
					hasConflict = true
				}
			} else {
				rf.logs = append(rf.logs, args.Entries[i])
				rf.logTerm = append(rf.logTerm, args.Term)
			}
		}
		if hasConflict && len(args.Entries)+args.PrevLogIndex < len(rf.logs) {
			// If has conflict, delete the following ones
			rf.logs = rf.logs[0 : len(args.Entries)+args.PrevLogIndex]
			rf.logTerm = rf.logTerm[0 : len(args.Entries)+args.PrevLogIndex]
		}
	}

	reply.Term = rf.currentTerm
	log.Printf("%d: %d replys AppendEntry:%v", rf.currentTerm, rf.me, reply)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
		var applyMsg ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg.Command = rf.logs[rf.lastApplied]
			applyMsg.Index = rf.lastApplied
			log.Printf("r:%d apply msg %v", rf.me, applyMsg)
			rf.applyBufferCh <- applyMsg
		}
	}
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// log.Printf("%d: %d (logs %v logTerm %v)receives RequestVote %v\n", rf.currentTerm, rf.me, rf.logs, rf.logTerm, args)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Agree = false
		log.Printf("%d:%d refuse RequestVote from %d because of its lower term", rf.currentTerm, rf.me, args.CandidateID)
		return
	}
	if rf.voteFor[args.Term] != rf.peers[args.CandidateID] && rf.voteFor[args.Term] != nil {
		log.Printf("%d:%d refuse RequestVote from %d because it has voted for %d", rf.currentTerm, rf.me, args.CandidateID, rf.voteFor[args.Term])
		reply.Agree = false
		return
	}
	// 	Comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	lastLogTerm := rf.logTerm[len(rf.logTerm)-1]
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < len(rf.logs)-1) {
		log.Printf("%d:%d refuse RequestVote from %d because the logs are out-of-date", rf.currentTerm, rf.me, args.CandidateID)
		reply.Agree = false
		return
	}
	log.Printf("%d:%d granted RequestVote from %d", rf.currentTerm, rf.me, args.CandidateID)
	rf.voteFor[args.Term] = rf.peers[args.CandidateID]
	rf.currentTerm = args.Term
	reply.Agree = true
	go func() {
		rf.heartbeatChan <- true
	}()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.commitIndex + len(rf.uncommitQueue) + 1
	term := rf.currentTerm
	isLeader := rf.state == LeaderState
	if isLeader {
		rf.uncommitQueue <- command
	}
	log.Printf("%d:%d start cmd (%d, %v) %v %v", term, rf.me, index, command, isLeader, rf.logs)
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

func (rf *Raft) getHeartBeatTimeOut() time.Duration {
	return time.Duration(HEARTBEAT_TIME) * time.Millisecond
}

func (rf *Raft) checkCommit() {
	count := 0
	min := -1
	for _, i := range rf.matchIndex {
		if i > rf.commitIndex {
			count++
			min = getMax(i, min)
		}
	}
	if count > rf.num/2 {
		rf.commitIndex = min
		log.Printf("%d commitIndex:%d", rf.me, min)
		var applyMsg ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg.Command = rf.logs[rf.lastApplied]
			applyMsg.Index = rf.lastApplied
			log.Printf("r:%d apply msg %v", rf.me, applyMsg)
			rf.applyBufferCh <- applyMsg
		}
	}
}

//follower: 2 chans(restart, electionTimeOutChan)
// candidate: (electionTimeOutChan, heartbeatChan)
// leader: (appendEntryChan, heartbeatChan)
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
	var msg ApplyMsg
	for {
		select {
		case term = <-rf.countVoteChan:
			rf.mu.Lock()
			if rf.currentTerm <= term {
				rf.countVotes[term]++
				if rf.countVotes[term] > rf.num/2 && rf.state != LeaderState {
					go func() {
						rf.beLeaderChan <- true
					}()
				}
			}
			rf.mu.Unlock()
		case <-rf.stopCountChan:
			return
		case msg = <- rf.applyBufferCh:
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) broadcastEmptyAppendEntries(args AppendEntryArgs) {
	if rf.state != LeaderState {
		return
	}
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.state != LeaderState {
				return
			}
			go func(i int) {
				reply := AppendEntryReply{}
				if ok := rf.sendAppendEntry(i, args, &reply); !ok {
					log.Printf("%d:%d error sendAppendEntry to %d: %v", rf.currentTerm, rf.me, i, args)
				}
				if reply.Term > args.Term {

				}
			}(i)
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
	rf.num = len(peers)
	rf.persister = persister
	rf.me = me
	rf.state = FollowerState
	rf.currentTerm = 0
	rf.voteFor = make(map[int]*labrpc.ClientEnd)
	rf.heartbeatChan = make(chan bool)
	rf.beLeaderChan = make(chan bool)
	rf.stopCountChan = make(chan bool)
	rf.countVoteChan = make(chan int, 100)
	rf.countVotes = make(map[int]int)
	rf.uncommitQueue = make(chan interface{}, 100)
	rf.commitIndex = 0
	rf.logs = []interface{}{nil} //logs index starts from 1
	rf.logTerm = []int{-1}
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.num)
	rf.applyCh = applyCh
	rf.applyBufferCh = make(chan ApplyMsg, 100)

	rf.matchIndex = make([]int, rf.num)
	go rf.countVotesLoop()
	go func() {
		for {
			switch rf.state {
			case FollowerState:
				select {
				case <-time.After(rf.getElectionTimeOut()):
					rf.state = CandidateState
				case <-rf.heartbeatChan:
				}
			case CandidateState:
				//what if candidate has become follower now?
				rf.mu.Lock()
				rf.currentTerm += 1
				log.Println(rf.currentTerm, rf.me, " becomes candidate")
				rf.voteFor[rf.currentTerm] = rf.peers[rf.me]
				rf.countVotes[rf.currentTerm]++
				args := RequestVoteArgs{
					CandidateID:  rf.me,
					Term:         rf.currentTerm,
					LastLogIndex: len(rf.logs) - 1,
					LastLogTerm:  rf.logTerm[len(rf.logs)-1],
				}
				rf.mu.Unlock()
				go rf.broadcastRequestVote(args)
				select {
				case <-rf.beLeaderChan:
					log.Println(args.Term, rf.me, " becomes leader")
					rf.state = LeaderState
					rf.mu.Lock()
					if rf.lastApplied != len(rf.logs)-1 {
						panic("lastApplied not equals to logs len")
					}
					for i := range rf.nextIndex {
						if i == rf.me {
							rf.matchIndex[i] = rf.lastApplied
						} else {
							rf.matchIndex[i] = 0
						}
						rf.nextIndex[i] = len(rf.logs)
					}
					rf.mu.Unlock()
				case <-rf.heartbeatChan:
					rf.state = FollowerState
					log.Println(args.Term, rf.me, " candidate becomes follower")
				case <-time.After(rf.getElectionTimeOut()):
				}
			case LeaderState:
				//TODO: reinitialization
				rf.mu.Lock()
				args := AppendEntryArgs{
					LeaderID: rf.me,
					Term:     rf.currentTerm,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				go rf.broadcastEmptyAppendEntries(args)
				select {
				case <-rf.heartbeatChan:
					rf.state = FollowerState
					log.Println(args.Term, rf.me, " leader becomes follower")
				case <-time.After(rf.getHeartBeatTimeOut()):
				case cmd := <-rf.uncommitQueue:
					rf.mu.Lock()
					// applyMsg := ApplyMsg{len(rf.logs), cmd, false, nil}
					// log.Printf("%d:%d apply msg %v", rf.currentTerm, rf.me, applyMsg)
					rf.logs = append(rf.logs, cmd)
					rf.logTerm = append(rf.logTerm, rf.currentTerm)
					rf.matchIndex[rf.me] = len(rf.logs) - 1
					args := AppendEntryArgs{
						LeaderID:     rf.me,
						Term:         rf.currentTerm,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					// go rf.ReceiveApplyMsg(applyMsg)
					go func(logs []interface{}, logTerm []int, lastApplied int, nextIndex []int) {
						// Logs and nextIndex may be changed in the main func.
						for i, _ := range rf.peers {
							if i != rf.me {
								if rf.state != LeaderState {
									return
								}
								if lastApplied >= nextIndex[i] {
									go func(i int, args AppendEntryArgs) {
										reply := AppendEntryReply{}
										t := 1
										for {
											if rf.state != LeaderState {
												return
											}
											args.Entries = logs[nextIndex[i] : lastApplied+1] //To include lastApplied entry
											args.PrevLogIndex = nextIndex[i] - 1
											args.PrevLogTerm = logTerm[args.PrevLogIndex]
											if ok := rf.sendAppendEntry(i, args, &reply); !ok {
												log.Printf("%d:%d sendAppendEntry: retry after %d", args.Term, rf.me, t)
												time.Sleep(time.Duration(t) * time.Millisecond)
												t *= 2
												if t > 128 {
													log.Printf("%d Cannot append entry %v to %d after retry", rf.me, args, i)
													return
												}
												continue
											}
											// log.Printf("%d send logs %v to %d: %v", rf.me, args.Entries, server, reply.Agree)
											if reply.Success {
												rf.mu.Lock()
												rf.nextIndex[i] = getMax(rf.nextIndex[i], lastApplied+1)
												rf.matchIndex[i] = getMax(rf.matchIndex[i], lastApplied)
												rf.checkCommit()
												rf.mu.Unlock()
												return
											} else if reply.Term == args.Term {
												// Followers reject appendEntry because the last log does not match. So move ahead
												if nextIndex[i] <= 0 {
													panic("nextIndex <= 0")
												}
												nextIndex[i]--
												log.Printf("%d:%d sendAppendEntry %v to %d nextIndex:%d", args.Term, rf.me, args, i, nextIndex[i])
											} else {
												return
											}
										}
									}(i, args)
								}
							}
						}
					}(rf.logs, rf.logTerm, len(rf.logs) - 1, rf.nextIndex)
				}
			}

		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
