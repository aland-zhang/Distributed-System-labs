package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "strconv"
import "time"

type Paxos struct {
	mu         sync.Mutex
  proposerMu sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]
	instmap   map[int]*PaxosInstance
	maxInst		int
	// Your data here.
  maxReportedDones map[string]int


}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type PrepareArgs struct{
	Seq int
  Pid string
  Me  string
  MaxDone int
}
type PrepareReply struct{
	Pid string
	Pval interface{}
	OK bool
}
type AcceptArgs struct {
	Seq int
	Pid string
	Pval interface{}
}
type AcceptReply struct{
	Pid string
	OK bool
}
type DecideArgs struct{
	Seq int
	Pval interface{}
}
type PaxosInstance struct{
	NPrep string
	AcceptedId string
	AcceptedVal interface{}
	Done bool
}
func generateIncreasingNum(me int) string {
	return strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.Itoa(me)
}

func (px *Paxos) RunProposor(seq int, v interface{}) {
  px.proposerMu.Lock()
  defer px.proposerMu.Unlock()
  
  if _, exist := px.instmap[seq]; !exist {
    px.instmap[seq] = &PaxosInstance{NPrep:"0",AcceptedId:"0",AcceptedVal:nil,Done:false}
    if seq > px.maxInst {
      px.maxInst = seq
    }
  }
	n := len(px.peers)
	for !px.instmap[seq].Done {
		Nmax := "0"
		count := 0
		var Vmax interface{}
    log.Printf("p%d prepare(%d,%v)", px.me, seq, v)
    meStr := px.peers[px.me]
		args := PrepareArgs {seq, generateIncreasingNum(px.me), meStr, px.maxReportedDones[meStr]}
		for i:=0; i < n; i++ {
			reply := &PrepareReply{}
			call(px.peers[i], "Paxos.Prepare", args, reply)//May need improvement
			if reply.OK {
				count++
				// log.Printf("%d got PrepareOK", px.me)
				if reply.Pid > Nmax {
					Nmax = reply.Pid
					Vmax = reply.Pval
				}
			}
		}
		if count > n/2 {
			// log.Printf("(Nmax,Vmax):(%s,%v) after Prepare(%s)", Nmax, Vmax, args.Pid)
			//if Acceptor has not accepted before, use v; else, use Vmax
      log.Printf("p%d send accept(%d,%v)", px.me, seq, v)
			args2 := AcceptArgs{Seq:seq, Pid:args.Pid}
			if Nmax != "0" {
				args2.Pval = Vmax
			} else {
				args2.Pval = v
			}
			count := 0
			for i:=0; i < n; i++ {
				reply := &AcceptReply{}
				call(px.peers[i], "Paxos.Accept", args2, reply)
				if reply.OK {
					count++
				}
			}
			if count > n/2 {
        log.Printf("p%d send decide(%d,%v)", px.me, seq, v)
				args3 := DecideArgs{seq, args2.Pval}
				reply := &AcceptReply{}
				for i:=0; i < n; i++ {
					if i != px.me {
						call(px.peers[i], "Paxos.Decided", args3, reply)
					} else {
						px.instmap[seq].AcceptedVal = args3.Pval
						px.instmap[seq].Done = true
					}
				}
				// log.Printf("%d:s%d sent Decide %v.", seq, px.me, args3.Pval)
        return
			}

		} else {
			log.Printf("%d prepared(%d): less than half OK", px.me, args.Pid)
		}
	}
}
//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  if seq < px.Min() {
    log.Printf("p%d:seq%d < min",px.me, seq)
    return
  }
  px.mu.Lock()
  defer px.mu.Unlock()
  if inst, ok := px.instmap[seq]; ok && inst.Done {
    return
  }
	// log.Printf("p%d Start %d on %v", px.me, seq, v)
	go px.RunProposor(seq, v)
}
func (px *Paxos) Prepare(args PrepareArgs, reply *PrepareReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
	if _, exist := px.instmap[args.Seq]; !exist {
		// log.Printf("%d's map:%f", px.me, px.instmap)
		px.instmap[args.Seq] = &PaxosInstance{NPrep:"0",AcceptedId:"0",AcceptedVal:nil,Done:false}
		if args.Seq > px.maxInst {
			px.maxInst = args.Seq
		}
	}
  //decide the max done for peer i
  if args.MaxDone > px.maxReportedDones[args.Me] {
    px.maxReportedDones[args.Me] = args.MaxDone
  }
	if args.Pid > px.instmap[args.Seq].NPrep {
		px.instmap[args.Seq].NPrep = args.Pid
		reply.OK = true
		log.Printf("p%d Acceptor Prepare(%d)", px.me, args.Seq)
		reply.Pid = px.instmap[args.Seq].AcceptedId
		reply.Pval = px.instmap[args.Seq].AcceptedVal
	} else {
		reply.OK = false

	}
	return nil
}

func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
	if _, exist := px.instmap[args.Seq]; !exist {
		// log.Printf("%d's map:%f", px.me, px.instmap)
		px.instmap[args.Seq] = &PaxosInstance{NPrep:"0",AcceptedId:"0",AcceptedVal:nil,Done:false}
		if args.Seq > px.maxInst {
			px.maxInst = args.Seq
		}
	}
	if args.Pid >= px.instmap[args.Seq].NPrep {
		px.instmap[args.Seq].AcceptedId = args.Pid
 		px.instmap[args.Seq].AcceptedVal = args.Pval
		px.instmap[args.Seq].NPrep = args.Pid
		reply.OK = true
		reply.Pid = args.Pid
	} else {
		reply.OK = false
	}
	return nil
}

func (px *Paxos) Decided(args DecideArgs, reply *AcceptReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()
	// log.Printf("p%d decided on (%d, %v)", px.me, args.Seq, args.Pval)
	px.instmap[args.Seq].AcceptedVal = args.Pval
	px.instmap[args.Seq].Done = true
	return nil
}

func getMax(a int, b int) int {
  if a > b {
    return a
  } else {
    return b
  }
}
func getMin(a int, b int) int {
  if a < b {
    return a
  } else {
    return b
  }
}
//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  meStr := px.peers[px.me]
  maxDone = getMax(seq, px.maxReportedDones[meStr])
  px.maxReportedDones[meStr] = maxDone  
  log.Printf("p%d Done(%d)", px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
  defer px.mu.Unlock()
	return px.maxInst
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
  px.mu.Lock();
  defer px.mu.Unlock()
  min := px.maxReportedDones[px.peers[0]]
  for i:=1; i < len(px.peers); i++ {
    min = getMin(min, px.maxReportedDones[px.peers[i]])
  }
  for i:=px.maxReportedDones[px.peers[px.me]]; i < min; i++ {
    if _, ok := px.instmap[i]; ok {
      delete(px.instmap, i)
    }
  }
  log.Printf("p%d min:%d", px.me, min)
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
	if inst, ok := px.instmap[seq]; !ok {
		return false, nil
	} else {
		log.Printf("s%d status (%d,%v)", px.me, seq, inst.AcceptedVal)
		if inst.Done {
			return true, inst.AcceptedVal
		} else {
			return false, nil
		}

	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.instmap = make(map[int]*PaxosInstance)
	px.maxInst = 0
  px.maxReportedDones = make(map[string]int)
  for i := range(px.peers) {
    px.maxReportedDones[px.peers[i]] = -1
  }
	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
