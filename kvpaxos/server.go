package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "time"

const (
	Debug=0
	GetOp = 1
	PutOp = 2
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq int
	Key string
	Value string
	UUID int64
	Client string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos
	next	   int
	keyMap	   map[string]int
	// Your definitions here.
	seen 		map[string]int64
	content		map[string]string

}
func (kv *KVPaxos) 
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	log.Printf("s%d get(%s)",kv.me, args.Key)
	kv.mu.Lock()
	var result string
	var found bool
	if max := kv.px.Max(); kv.next <= max {
		log.Printf("s%d left behind:%d", kv.me, kv.next)
		for i := kv.next; i <= max; i++ {
			if ok, tmpOp := kv.px.Status(i); ok {
				op := tmpOp.(Op)
				kv.keyMap[op.Key] = op.Seq
				log.Printf("s%d: %d, (%s,%v)", kv.me, op.Seq, op.Key, op.Value)
				if op.Key == args.Key {
					result = op.Value
					found = true
				}
			}
		}
		kv.next = max + 1
	}
	if found {
		reply.Value = result
	} else {
		seq := kv.keyMap[args.Key]
		if seq != 0 {		
			if ok, tmpOp := kv.px.Status(seq); ok && tmpOp != nil {
				op := tmpOp.(Op)
				reply.Value = op.Value
			} else {
				log.Printf("s%d:Get(%s) not ready:%v", kv.me, args.Key, tmpOp) 
			}
		}
	}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	log.Printf("s%d put(%s,%v)",kv.me, args.Key, args.Value)
	kv.mu.Lock()
	val := args.Value
	max := kv.px.Max()
	if kv.next <= max {
		log.Printf("s%d left behind:%d", kv.me, kv.next)
		for i := kv.next; i <= max; i++ {
			if ok, tmpOp := kv.px.Status(i); ok {
				op := tmpOp.(Op)
				kv.keyMap[op.Key] = op.Seq
			}
		}
		kv.next = max + 1
	}
	seq := kv.next
	kv.next++
	kv.keyMap[args.Key] = seq

	op := Op {}
	op.Seq = seq
	op.Key = args.Key
	if args.DoHash {
		if existed, previousOp := kv.px.Status(seq); existed && previousOp!=nil {
			if tmpOp, ok := previousOp.(Op); ok {
				val = tmpOp.Value+val
			}
		}
		op.Value = strconv.Itoa(int(hash(val)))
		args.Value = op.Value
	} else {
		op.Value = val
	}
	
	to := 10 * time.Millisecond
	for {
	  	kv.px.Start(seq, op)
	    decided, tmp := kv.px.Status(seq)
	    if decided {
	    	if tmpOp, ok := tmp.(Op); ok {
	    		if tmpOp.Value == args.Value {
	    			log.Printf("s%d:Put(%s,%s) is decided", kv.me, args.Key, args.Value)
	      			break 
	    		} else {
	    			log.Printf("s%d:Put(%s,%s) -> %s", kv.me, args.Key, args.Value, tmpOp.Value)
	    		}
	    	} else {
	    		log.Println("Non-Op")
	    	}
	    }
	    time.Sleep(to)
	    if to < 10 * time.Second {
	      to *= 2
	    }
	}
	kv.mu.Unlock()
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.next = 1
	kv.keyMap = make(map[string]int)

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
