package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	maxSeen		int
	gids		map[int64]bool

}

type Op struct {
	// Your data here.
}
// func isConfigSame(args* JoinArgs, conf Config) bool {
// 	for i := range(conf.Shards) {
// 		if
// 	}
// }
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.maxSeen++
	sm.configs[0].Groups[args.GID] = args.Servers
	sm.gids[args.GID] = true

	pieces := 10 / len(sm.gids)
	count := 0
	var tmp int64
	for j := range(sm.gids) {
		for i:= 0; i < pieces;i++ {
			sm.configs[0].Shards[count] = j
			count++
		}
		tmp = j
	}
	for count < 10 {
		sm.configs[0].Shards[count] = tmp
		count++
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	if args.Num == -1 || args.Num > sm.maxSeen {
		reply.Config = sm.configs[0] 
	} else {
		reply.Config = sm.configs[0]
	}
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 100)
	sm.configs[0].Groups = map[int64][]string{}
	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)
	sm.maxSeen = 0
	sm.gids = make(map[int64]bool)
	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
