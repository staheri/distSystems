package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "time"
import (
	"math/rand"
	//"crypto/x509/pkix"
)


const Debug = 0

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
	What	string
	Key 	string
	Value  	string
	XID		int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	currentSeq	int
	interLog 		map[string]string
	seenOp		map[int64]int
	blackList 	map[int]int
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	min := kv.px.Min()
	ship := new(Op)
	ship.Key = args.Key
	ship.What = "Get"
	ship.XID = args.XID
	kv.mu.Lock()
	kv.currentSeq = kv.px.Max() + 1
	kv.px.Start(kv.currentSeq,*ship)
	to := 10 * time.Millisecond
	for {
		status, vvv := kv.px.Status(kv.currentSeq)
		if status == paxos.Decided{
			vvv1 := vvv.(Op)
			if vvv1.XID != ship.XID {
				kv.currentSeq = kv.px.Max() + 1
				kv.px.Start(kv.currentSeq,*ship)
				to = 10 * time.Millisecond
				continue
			} else {
				break
			}
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	to = 10 * time.Millisecond
	min = kv.px.Min()
	for i:= min ;  i < kv.currentSeq ; i++{
		for {
			status, _ := kv.px.Status(i)
			if status == paxos.Decided{
			} else if status == paxos.Forgotten{
			} else {
				if kv.blackList[i] == 1{
					break
				}
				
			}
			if status == paxos.Decided{
				val := kv.px.GetLog(i)
				org := val.(Op)
				oper := org.What
				key := org.Key
				value := org.Value
				xid := org.XID
				if kv.seenOp[xid] == 1 {
					break
				} else {
					kv.seenOp[xid] = 1
				}
				if oper == "Put" {
					kv.interLog[key] = value
				} else if oper == "Append"{
					kv.interLog[key] =  kv.interLog[key] + value
				} else {
				}
				break
			}
			time.Sleep(to)
			if to < 5 * time.Second {
				to *= 2
			} else{
				kv.blackList[i] = 1
				to = 10 * time.Millisecond
				break
			}
		}	
	}
	kv.mu.Unlock()
	key1 := args.Key
	reply.Err = "OK"
	kv.px.Done(kv.currentSeq-1)
	reply.Value =  kv.interLog[key1]

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	ship := new(Op)
	ship.Key = args.Key
	ship.Value = args.Value
	ship.What = args.Oper
	ship.XID = args.XID
	kv.mu.Lock()
	kv.currentSeq = kv.px.Max() + 1
	//kv.mu.Unlock()
	kv.px.Start(kv.currentSeq,*ship)
	to := 10 * time.Millisecond
	for {
		status, vvv := kv.px.Status(kv.currentSeq)
		if status == paxos.Decided{
			vvv1 := vvv.(Op)
			if vvv1.XID != ship.XID {
				kv.currentSeq = kv.px.Max() + 1
				kv.px.Start(kv.currentSeq,*ship)
				to = 10 * time.Millisecond
				continue
			} else {
				break
			}
		} else {
			if status == paxos.Decided{
			} else if status == paxos.Forgotten{
			} else {
				
			}
			
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
		
	}
	kv.mu.Unlock()

	reply.Err = "OK"
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
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
	// Your initialization code here.
	kv.currentSeq = -1
	kv.interLog = make(map[string]string)
	kv.seenOp = make(map[int64]int)
	kv.blackList = make(map[int]int)

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
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
