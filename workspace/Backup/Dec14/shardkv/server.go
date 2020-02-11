package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
//import "sort"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	What	string
	Key 	string
	Value  	string
	XID		int64
	Conf	shardmaster.Config
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	
	// To hold info about config
	config 		shardmaster.Config
	configMap	map[int]shardmaster.Config
	
	//to hold info about duplicates
	requests 	map[int64]int64
	
	//Foe moving
	
	
	// from kvpaxos
	currentSeq	int
	interLog 	map[string]string
	seenOp		map[int64]int
	blackList 	map[int]int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//fmt.Printf("%d %d S-Get key: %v \n",kv.me,kv.gid, args.Key)
	//fmt.Printf("******** %d %d CONFIG: %v \n",kv.me,kv.gid, kv.config)
	kv.mu.Lock()
	if args.ConfigNum != kv.config.Num {
		//time.Sleep(5 * time.Second)
		reply.Err = "ErrWrongGroup"
		fmt.Printf("%d %d WWW S-Get key: %v conf: %d \n",kv.me,kv.gid, args.Key,kv.config.Num)
	} else {
		fmt.Printf("%d %d RRR S-Get key: %v conf: %d \n",kv.me,kv.gid, args.Key,kv.config.Num)
		min := kv.px.Min()
		ship := new(Op)
		ship.Key = args.Key
		ship.What = "Get"
		ship.XID = args.XID
		
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
		//fmt.Printf("SGET : Inside for 20\n")
		to = 10 * time.Millisecond
		min = kv.px.Min()
		//maxCNUM := 0
		for i:= min ;  i < kv.px.Max() ; i++{

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
						fmt.Printf("\n(%d,%d)-%d #### PUT %d-%d ###### %v \n" , kv.me , kv.gid,kv.config.Num,i,kv.px.Max(),org)
					} else if oper == "Append"{
						fmt.Printf("\n(%d,%d)-%d #### APP %d-%d ###### %v \n" , kv.me , kv.gid,kv.config.Num,i,kv.px.Max(),org)
						kv.interLog[key] =  kv.interLog[key] + value
					} else if oper == "Reconf" {

						fmt.Printf("\n(%d,%d)-%d #### Reconf %d-%d ###### %v \n" , kv.me , kv.gid,kv.config.Num,i,kv.px.Max(),org)
						//maxCNUM = org.Conf.Num

					} else {
						fmt.Printf("\n(%d,%d)-%d #### Get %d-%d ###### %v \n" , kv.me , kv.gid,kv.config.Num,i,kv.px.Max(),org)
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
		//fmt.Printf("%d %d <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< GET UNLOCK\n" , kv.me , kv.gid)
		//fmt.Printf("\n##########\n## < < GET UN- LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)
		//if maxCNUM <= kv.config.Num {
			//reply.Err = "ErrWrongGroup"
		//} else {
			key1 := args.Key
			reply.Err = "OK"
			kv.px.Done(kv.currentSeq-1)
			reply.Value =  kv.interLog[key1]
		//}
		
	}
	kv.mu.Unlock()
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//fmt.Printf("%d %d S-PutAppend key: %v value: %v \n",kv.me,kv.gid, args.Key, args.Value)
	kv.mu.Lock()
	if args.ConfigNum != kv.config.Num {
		reply.Err = "ErrWrongGroup"
		fmt.Printf("%d %d WWWW S-PutAppend key: %v value: %v conf: %d \n",kv.me,kv.gid, args.Key, args.Value,kv.config.Num)
	} else {
		//fmt.Printf("%d %d RRR S-PutAppend key: %v value: %v conf: %d \n",kv.me,kv.gid, args.Key, args.Value,kv.config.Num)
		ship := new(Op)
		ship.Key = args.Key
		ship.Value = args.Value
		ship.What = args.Op
		ship.XID = args.XID
		//fmt.Printf("\n##########\n##  > > PUT LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)
		
		
		//fmt.Printf(" %d %d >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PUT LOCK\n" , kv.me , kv.gid)
		kv.currentSeq = kv.px.Max() + 1
		//kv.mu.Unlock()
		fmt.Printf("%d %d RRR S-PutAppend key: %v value: %v kv.conf: %d args.conf: %d MAX=%d \n",kv.me,kv.gid, args.Key, args.Value,kv.config.Num,args.ConfigNum,kv.currentSeq)
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
		//fmt.Printf("%d %d <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< PUT UNLOCK\n" , kv.me , kv.gid)
		
		//fmt.Printf("\n##########\n## < < PUT UN- LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)

		reply.Err = "OK"
	}
	kv.mu.Unlock()
	return nil
	
}



func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) error {
	// Your code here.
	fmt.Printf("%d %d S-MoveShard CONT: %v \n",kv.me,kv.gid, args.Cont)
	if kv.requests[args.XID] != 0 {
		reply.Err = "OK"
		fmt.Printf("\nALREADY (%d,%d) : %v \n" , kv.me , kv.gid, args)
	} else {
		kv.mu.Lock()
		kv.requests[args.XID] = args.XID
		for jj := range args.Cont {
			ship := new(Op)
			ship.Key = args.Cont[jj].Key
			ship.Value = args.Cont[jj].Value
			ship.What = args.Cont[jj].Opt
			ship.XID = args.Cont[jj].XID
			//fmt.Printf("\n##########\n##  > > MOVESHARD LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)

			//fmt.Printf("%d %d >> MOVESHARD AFTER LOCK\n" , kv.me , kv.gid)
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
			//fmt.Printf("%d %d <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< MOVE SHARD UNLOCK\n" , kv.me , kv.gid)

			//fmt.Printf("\n##########\n## < < MOVESHARD UN- LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)

			reply.Err = "OK"
		}
		kv.mu.Unlock()

	}
	
	return nil

}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	//fmt.Printf("############ %d %d Current Conf: %v\n",kv.me,kv.gid,kv.config)
	//fmt.Printf("BEFORE LOCK %d  %d \n",kv.me,kv.gid)
	//fmt.Printf("\n##########\n##  > > TICK LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)
	kv.mu.Lock()
	isNew := false
	//mvsh := make(map[int]*MoveShardArgs)
	//mvggg := make(map[int]int64)
	mapShardArgs := make(map[int64]*MoveShardArgs)
	//mapShardArgs.Cont = make(map[int64][]INDMoveShardArgs)
	//fmt.Printf("%d %d >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> THICK LOCK\n" , kv.me , kv.gid)
	//fmt.Printf("--------------\n%d %d Inter Log: %v \n--------------\n",kv.me,kv.gid,kv.interLog)
	 //fmt.Printf("\n\n>> %d  %d <<\n",kv.me,kv.gid)
	 //for i := 0 ; i < kv.px.Max() ; i ++ {
		// fmt.Printf("(%d,%d) i = %d , %v \n",kv.me,kv.gid,i,kv.px.GetLog(i))
	 //}
	// fmt.Printf("<< %d  %d >>\n",kv.me,kv.gid)
	newConf := kv.sm.Query(-1)
	if newConf.Num != kv.config.Num {
		fmt.Printf("%d %d Config Changed from %d to %d \n***\noldConf Shards %v\nnewConf Shards %v \n",kv.me,kv.gid, kv.config.Num,newConf.Num,kv.config.Shards,newConf.Shards)
		isNew = true
		ship := new(Op)
		ship.XID = nrand()
		ship.What = "Reconf"
		ship.Conf = newConf
		//kv.mu.Lock()
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
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
			
		}
		//kv.mu.Unlock()
		// if kv.config.Num == 0 {
			// kv.config = newConf
		// }
		min := kv.px.Min()
		//mapShardArgs := &MoveShardArgs{}
		//mapShardArgs.Cont = make(map[int64][]INDMoveShardArgs)
		//fmt.Printf("%d %d MIN : %d Max %d\n",kv.me,kv.gid,min,kv.px.Max())
		for i:= min ;  i < kv.px.Max() ; i++{
			//fmt.Printf("\n INSIDE FOR %d %d i= %d\n",kv.me,kv.gid,i)
			status, vvv := kv.px.Status(i)
			//fmt.Printf("\n     ##### %d %d i= %d\n",kv.me,kv.gid,i)
			if status == paxos.Decided{
				vvv1 := vvv.(Op)
				if vvv1.What != "Reconf" {
					mvArgs := INDMoveShardArgs{}
					mvArgs.Key = vvv1.Key
					mvArgs.Opt = vvv1.What
					mvArgs.Value = vvv1.Value
					mvArgs.XID = vvv1.XID
					shard := key2shard(vvv1.Key)
					ggg := newConf.Shards[shard]
					//fmt.Printf("N-conf: %d O-conf: %d ggg : %d ,  kv.Gid :  %d , kv.config.shards[%d] : %d  \n" , ggg,kv.gid,shard,kv.config.Shards[shard])
					if ggg != kv.config.Shards[shard] {
						if ggg != kv.gid {
							//fmt.Printf("Add %v to (%d) from %d \n" , mvArgs,ggg,kv.gid)
							if _, okkk := mapShardArgs[ggg] ; okkk{
								tempslice := mapShardArgs[ggg].Cont
								tempslice = append(tempslice,mvArgs)
								mapShardArgs[ggg].Cont = tempslice
								mapShardArgs[ggg].XID = nrand()
							} else {
								mapShardArgs[ggg] = &MoveShardArgs{}
								mapShardArgs[ggg].Cont = []INDMoveShardArgs{mvArgs}
								mapShardArgs[ggg].XID = nrand()
							}
//							tempslice := mapShardArgs[ggg].Cont
//							tempslice = append(tempslice,mvArgs)
//							mapShardArgs[ggg].Cont = tempslice
//							mapShardArgs[ggg].XID = nrand()
//							mvggg[i] = ggg
//							mvsh[i] = mvArgs
							//fmt.Printf("\n ADD TO MAP %d %d i= %d\n",kv.me,kv.gid,i)
							// servers, ok := newConf.Groups[ggg]
							// if ok {
								// // try each server in the shard's replication group.
								// for _, srv := range servers {
									// var reply MoveShardReply
									// fmt.Printf("MOVE SHARDS %v from %d (%d) to %d - oldNum : %d newNum : %d\n", vvv1,kv.gid,kv.me,ggg,kv.config.Num,newConf.Num)
									// ok := call(srv, "ShardKV.MoveShard", mvArgs, &reply)
									// if ok {
										// if reply.Err == "OK"{
											// break
										// } else {
											// fmt.Printf("Fucked 2\n")
										// }	
									// } else {
										// fmt.Printf("Fucked\n")
									// }
								// }
								// fmt.Printf("\n NEXT %d %d i= %d\n",kv.me,kv.gid,i)
							// } else {
								// fmt.Printf("Servers Not OK\n")
							// }
						}else {
							//fmt.Printf("DO NOT MOVE SHARDS ITSELF \n")
						}
					}else {
						//fmt.Printf("NO NEED \n")
					}
				} else {
					//fmt.Printf("OP is RECONF %d %d \n",kv.me,kv.gid)
				}
			} else {
				//fmt.Printf("Not Decided %d \n", i)
			}
		}
		kv.config = newConf
	}
//	fmt.Printf("%d %d <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< THICK UNLOCK\n" , kv.me , kv.gid)
	//fmt.Printf("\n\n\n")
	kv.mu.Unlock()
	
	if isNew {
//		var kks []int
//		for k := range mvsh {
//			//fmt.Printf("SSRRTT key : %v  from %d (%d)\n",k,kv.gid,kv.me)
//			kks = append(kks, k)
//		}
//		sort.Ints(kks)
//		fmt.Printf("KKS : %v \n",kks)
		for kk,_ := range mapShardArgs {
			//fmt.Printf("KOON %d \n",kk)
			ggg2 := kk
			moveARGS := mapShardArgs[kk]
			servers, ok := newConf.Groups[ggg2]
			if ok{
				//fmt.Printf("KOS %d %v kk= %d \n",ggg2,moveARGS,kk)
				for _, srv := range servers {
					var reply MoveShardReply
					fmt.Printf("@@ MOVE SHARDS %v \n from %d (%d) to %d conf=%d\n", moveARGS,kv.gid,kv.me,ggg2,kv.config.Num)
					ok := call(srv, "ShardKV.MoveShard",moveARGS, &reply)
					if ok {
						if reply.Err == "OK"{
							break
						} else {
						fmt.Printf("Fucked 2\n")
						}	
					} else {
						fmt.Printf("Fucked\n")
					}
				}
			} else {
				//fmt.Printf("KIR %d %v kk= %d \n",ggg2,moveARGS,kk)
			}
		}
	}

	//fmt.Printf("\n##########\n## < < TICK UN- LOCK (%d,%d)\n##########\n" , kv.me , kv.gid)
	
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	
	kv.requests = make(map[int64]int64)
	kv.config = kv.sm.Query(-1)
	
	kv.currentSeq = -1
	kv.interLog = make(map[string]string)
	kv.seenOp = make(map[int64]int)
	kv.blackList = make(map[int]int)
	kv.configMap = make(map[int]shardmaster.Config)
	

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
