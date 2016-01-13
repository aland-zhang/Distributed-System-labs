package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	viewnum	   uint
	currView   viewservice.View
	data    map[string]string
}
func (pb *PBServer) isPrimary(name string) bool{
	return pb.currView.Primary == name
}
func (pb *PBServer) isBackup(name string) bool{
	return pb.currView.Backup == name
}
func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
 	val := pb.data[args.Key]
	reply.PreviousValue = val
	// if reply.Err != "" {
	// 	reply.Err = ""
	// }
	pb.data[args.Key] = (val + args.Value)
	
	log.Printf("Put %s -> %s", args.Key, pb.data[args.Key])
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	reply.Value = pb.data[args.Key]
	log.Printf("Get %s -> %s", args.Key, pb.data[args.Key])
	// if reply.Err != "" {
	// 	reply.Err = ""
	// }
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	var err error
	pb.currView, err = pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		log.Printf("%s Ping(%d) got error:%s", pb.me, pb.currView.Viewnum, err)
		return
	}
	
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.currView.Viewnum = 0

	pb.data = make(map[string]string)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)
	log.Println("Register PB ", me)
	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
