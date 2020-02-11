package shardmaster

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
import "math/rand"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         		int
	dead       		int32 // for testing
	unreliable 		int32 // for testing
	px         		*paxos.Paxos

	configs 		[]Config // indexed by config num
	currentSeq		int
	activeGroups	[]int64
	seenGID			map[int64]int
}


type Op struct {
	// Your data here.
	Conf 		Config
	//ConfigNum	int
	UID			int64
	
	
}

func Extend(slice []Config, element Config) []Config {
    n := len(slice)
	//fmt.Printf("INSIDE EXTEND \n")
    if n == cap(slice) {
		fmt.Printf("SLICE IS FULL \n")
        // Slice is full; must grow.
        // We double its size and add 1, so if the size is zero we still grow.
        newSlice := make([]Config, len(slice), 2*len(slice)+1)
        copy(newSlice, slice)
        slice = newSlice
    }
    slice = slice[0 : n+1]
    slice[n] = element
    return slice
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	
	if sm.seenGID[args.GID] == 1 {
		fmt.Printf("Already seen\n")
	} else {
		sm.mu.Lock()
		ttt := sm.px.Max()
		for i:= len(sm.configs) ; i < ttt ; i++ {
			fmt.Printf("UPDATE server = %d  SeqMax = %d \n" ,sm.me,ttt)
			nt := sm.px.GetLog(i)
			ntt := nt.(Op)
			sm.configs = Extend(sm.configs,*ntt)
		}
		sm.seenGID[args.GID] = 1 
		ship := new(Op)
		//ship.Conf = new (Config)
		ship.ConfigNum = len(sm.configs) //sm.px.Max() + 1// // New config number for propose
		ship.UID = (args.GID * 1000) + int64(ship.ConfigNum)
		
		sm.currentSeq = sm.px.Max() + 1
		//sm.mu.Unlock()
		fmt.Printf("JOIN GID = %d seq = %d server = %d  SeqMax = %d \n" ,args.GID,sm.currentSeq,sm.me,sm.px.Max())
		sm.px.Start(sm.currentSeq,*ship)
		to := 10 * time.Millisecond
		for {
			status, vvv := sm.px.Status(sm.currentSeq)
			if status == paxos.Decided{
				vvv1 := vvv.(Op)
				if vvv1.UID != ship.UID {
					sm.currentSeq = sm.px.Max() + 1
					sm.px.Start(sm.currentSeq,*ship)
					to = 10 * time.Millisecond
					fmt.Printf("DEC -- GID = %d seq = %d config = %d vvConfig = %d \n" ,args.GID,sm.currentSeq,ship.ConfigNum,vvv1.ConfigNum)
					continue
				} else {
					fmt.Printf("DEC ++ GID = %d seq = %d config = %d \n" ,args.GID,sm.currentSeq,ship.ConfigNum)
					break
				}
			} else {
				fmt.Printf("PEN PEN GID = %d seq = %d \n" ,args.GID,sm.currentSeq)
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
			
		}
		
		// Add GID of the joining group to Active Groups
		if sm.activeGroups[0] == 0 {
			fmt.Printf("CATCHHHHHHHHHHHHHHHHHHHH \n")
			sm.activeGroups = sm.activeGroups[1:]
		}
		sm.activeGroups = append(sm.activeGroups,args.GID)
		
		// Make new config instance to add to configs
		ne := new (Config)
		ne.Num = len(sm.configs)
		
		
		// isEmpty := true
		// if len(sm.configs) > 1 {
			// fmt.Printf("configs not empty \n")
			// isEmpty = false 
		// }
		
		// Assign Shards to groups
		for i := 0 ; i < len(ne.Shards) ; i++ {
			//fmt.Printf("Assign Shard %d to GID %d \n" , i , i%len(sm.activeGroups))
			ne.Shards[i] = sm.activeGroups[i%len(sm.activeGroups)]
		}
		
		// Holding info about previous config
		previousConfig := sm.configs[len(sm.configs)-1]
		
		//Make new map to hold info about new config
		ne.Groups = make(map[int64][]string)
		
		//Copy info about old config to the new one
		for k := range previousConfig.Groups {
			ne.Groups[k] = previousConfig.Groups[k]
		}
		
		//Add new group to the config
		ne.Groups[args.GID] = args.Servers
		
		sm.configs = Extend(sm.configs,*ne)
		//reply.Err = "OK"
		sm.mu.Unlock()
	
	
	
	}
	
	return nil
	
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	ttt := sm.px.Max()
	for i:= len(sm.configs) ; i < ttt ; i++ {
		fmt.Printf("UPDATE server = %d  SeqMax = %d \n" ,sm.me,ttt)
		nt := sm.px.GetLog(i)
		ntt := nt.(Op)
		sm.configs = Extend(sm.configs,ntt)
	}
	fmt.Printf("LEAVE GID = %d \n" ,args.GID)
	delete(sm.seenGID,args.GID)

	ship := new(Op)
	ship.ConfigNum = len(sm.configs) //sm.px.Max() + 1 // // New config number for propose
	ship.UID = (args.GID * 1000) + int64(ship.ConfigNum)
	sm.currentSeq = sm.px.Max() + 1
	//sm.mu.Unlock()
	sm.px.Start(sm.currentSeq,*ship)
	to := 10 * time.Millisecond
	for {
		status, vvv := sm.px.Status(sm.currentSeq)
		if status == paxos.Decided{
			vvv1 := vvv.(Op)
			if vvv1.UID != ship.UID {
				sm.currentSeq = sm.px.Max() + 1
				sm.px.Start(sm.currentSeq,*ship)
				to = 10 * time.Millisecond
				fmt.Printf("DEC -- GID = %d seq = %d \n" ,args.GID,sm.currentSeq)
				continue
			} else {
				fmt.Printf("DEC -- GID = %d seq = %d \n" ,args.GID,sm.currentSeq)
				break
			}
		} else {
			fmt.Printf("PEN PEN GID = %d seq = %d \n" ,args.GID,sm.currentSeq)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
		
	}
	
	// Remove GID of the leaving group from Active Groups
	for i := 0 ; i < len(sm.activeGroups) ; i++ {
		if sm.activeGroups[i] == args.GID {
			sm.activeGroups = append(sm.activeGroups[:i],sm.activeGroups[i+1:]...)
		}
	}
	
	// Make new config instance to add to configs
	ne := new (Config)
	ne.Num = len(sm.configs)
	
	
	//isEmpty := true
	// if len(sm.configs) > 1 {
		// fmt.Printf("configs not empty \n")
		// isEmpty = false 
	// }
	
	// Assign Shards to groups
	for i := 0 ; i < len(ne.Shards) ; i++ {
		ne.Shards[i] = sm.activeGroups[i%len(sm.activeGroups)]
	}
	
	// Holding info about previous config
	previousConfig := sm.configs[len(sm.configs)-1]
	
	//Make new map to hold info about new config
	ne.Groups = make(map[int64][]string)
	
	//Copy info about old config to the new one
	for k := range previousConfig.Groups {
		ne.Groups[k] = previousConfig.Groups[k]
	}
	
	//Delete leaving group from the config
	delete(ne.Groups,args.GID)
	
	sm.configs = Extend(sm.configs,*ne)
	//reply.Err = "OK"
	sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	fmt.Printf("MOVE GID = %d \n" ,args.GID)
	// Your code here.
	ship := new(Op)
	ship.ConfigNum = len(sm.configs) // New config number for propose
	ship.UID = (args.GID * 1000) + int64(ship.ConfigNum)
	sm.mu.Lock()
	sm.currentSeq = sm.px.Max() + 1
	//sm.mu.Unlock()
	sm.px.Start(sm.currentSeq,*ship)
	to := 10 * time.Millisecond
	for {
		status, vvv := sm.px.Status(sm.currentSeq)
		if status == paxos.Decided{
			vvv1 := vvv.(Op)
			if vvv1.UID != ship.UID {
				sm.currentSeq = sm.px.Max() + 1
				sm.px.Start(sm.currentSeq,*ship)
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
	
	// Remove GID of the leaving group from Active Groups
	// for i := 0 ; i < len(sm.activeGroups) ; i++ {
		// if sm.activeGroups[i] == args.GID {
			// sm.activeGroups = append(sm.activeGroups[:i],sm.activeGroups[i+1:]...)
		// }
	// }
	
	// Make new config instance to add to configs
	ne := new (Config)
	ne.Num = len(sm.configs)
	
	
	//isEmpty := true
	// if len(sm.configs) > 1 {
		// fmt.Printf("configs not empty \n")
		// isEmpty = false 
	// }
	
	// Holding info about previous config
	previousConfig := sm.configs[len(sm.configs)-1]
	ne.Shards = previousConfig.Shards
	
	// Assign Shard to group
	fmt.Printf("Move shard %d to GID %d \n",args.Shard, args.GID)
	ne.Shards[args.Shard] = args.GID
	
	
	//Make new map to hold info about new config
	ne.Groups = make(map[int64][]string)
	
	//Copy info about old config to the new one
	for k := range previousConfig.Groups {
		ne.Groups[k] = previousConfig.Groups[k]
	}
	
	sm.configs = Extend(sm.configs,*ne)
	//reply.Err = "OK"
	sm.mu.Unlock()


	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	//Your code here.
	
	// for i := 0 ; i < len(sm.activeGroups) ; i ++ {
		// fmt.Printf("ACtive Groups %d  \n",sm.activeGroups[i])
	// }
	if args.Num == -1  || args.Num >= len(sm.configs){
		reply.Config = sm.configs[len(sm.configs)-1]
		fmt.Printf("** len(configs) = %d , NUM = %d , groupsLen %v shards %v \n" , len(sm.configs) , sm.configs[len(sm.configs)-1].Num,sm.configs[len(sm.configs)-1].Groups,sm.configs[len(sm.configs)-1].Shards)
	} else {
		reply.Config = sm.configs[args.Num]
		fmt.Printf("len(configs) = %d , NUM = %d , groupsLen %v \n" , len(sm.configs) , sm.configs[args.Num].Num,sm.configs[args.Num].Groups,sm.configs[args.Num].Shards)
	}
	
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.activeGroups = make([]int64,1)
	sm.seenGID = make(map[int64]int)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
