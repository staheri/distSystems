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
import (
	//"time"
	"sort"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         		int
	dead       		int32 // for testing
	unreliable 		int32 // for testing
	px         		*paxos.Paxos

	currentSeq		int

}


type Op struct {
	// Your data here.
	Conf 		Config
	Query 		int
	UID			int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {

	//fmt.Printf("Join GID = %d, server: %d \n" ,args.GID,sm.me)
	var flag bool
	sm.mu.Lock()
	ship := new(Op)
	// This for is to know about the previous config
	// The inner for is for making decision on new config
	for {

		ship.Conf.Groups = make(map[int64][]string)
		sm.currentSeq = sm.px.Max() + 1

		// checking the status of prev config
		status, vvv := sm.px.Status(sm.currentSeq-1)

		var vvv1 Op
		if status == paxos.Decided {
			vvv1 = vvv.(Op)
		}else {
			// The previous config is not decided yet, Start Over
			continue
		}

		previousConfig := vvv1.Conf

		//fmt.Printf("\n$$$$$$\nJoin Prev Config currSeq = %d  GID = %d, server: %d \n%v\n$$$$$$\n" ,sm.currentSeq,args.GID,sm.me,previousConfig.Groups)

		//Copy info about old config to the new one
		for k := range previousConfig.Groups {
			//fmt.Printf("TRANSFER GID = %d, server: %d k: %d \n" ,args.GID,sm.me,k)
			if k == 0 {
				continue
			}
			ship.Conf.Groups[k] = previousConfig.Groups[k]
		}


		// Add the joining group
		ship.Conf.Groups[args.GID] = args.Servers

		//Find the active groups to assign Shards
		activeGroups := make([]int, 0, len(ship.Conf.Groups))
		for k := range ship.Conf.Groups {
			activeGroups = append(activeGroups, int(k))
		}

		// Sorting for minimal transfer
		sort.Ints(activeGroups)

		//Assigning Shards
		for i := 0 ; i < len(ship.Conf.Shards) ; i++ {
			ship.Conf.Shards[i] = int64(activeGroups[i%len(activeGroups)])
		}

		//Assigning unique ID . Join : 10
		ship.UID = (args.GID*100)+int64(10+sm.me)

		// Increment the config Num
		ship.Conf.Num = previousConfig.Num + 1

		// It is not Query . set it to -2
		ship.Query = -2

		//fmt.Printf("## Start Join %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)

		//Start the new config
		sm.px.Start(sm.currentSeq,*ship)
		for {
			//fmt.Printf("## Start Join %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
			status, vvv = sm.px.Status(sm.currentSeq)
			if status == paxos.Decided {
				vvv2 := vvv.(Op)
				if vvv2.UID != ship.UID {
					//fmt.Printf("not DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = false
					break
				} else {
					//to = 10 * time.Millisecond
					//fmt.Printf("DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = true
					break
				}
			} else {

			}
		}
		// If previous config is decided and the new config is already set to decided, break. Otherwise, Start all over!
		if flag {
			break
		} else{
			continue
		}
	}

	//fmt.Printf(">>> DOne Join %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
	sm.px.Done(sm.currentSeq)
	sm.mu.Unlock()
	
	return nil
	
}




func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {

	//fmt.Printf("Leave GID = %d, server: %d \n" ,args.GID,sm.me)

	var flag bool
	sm.mu.Lock()
	ship := new(Op)
	// This for is to know about the previous config
	// The inner for is for making decision on new config
	for {
		ship.Conf.Groups = make(map[int64][]string)
		sm.currentSeq = sm.px.Max() + 1

		// checking the status of prev config
		status, vvv := sm.px.Status(sm.currentSeq-1)
		var vvv1 Op
		if status == paxos.Decided {
			vvv1 = vvv.(Op)
		}else {
			// The previous config is not decided yet, Start Over
			continue
		}

		previousConfig := vvv1.Conf

		//Copy info about old config to the new one
		for k := range previousConfig.Groups {
			//fmt.Printf("TRANSFER GID = %d, server: %d k: %d \n" ,args.GID,sm.me,k)
			if k == 0 {
				continue
			}
			ship.Conf.Groups[k] = previousConfig.Groups[k]
		}

		// Delete the leaving group
		delete(ship.Conf.Groups,args.GID)

		//Find the active groups to assign Shards
		activeGroups := make([]int, 0, len(ship.Conf.Groups))
		for k := range ship.Conf.Groups {
			activeGroups = append(activeGroups, int(k))
		}

		sort.Ints(activeGroups)

		//Assigning Shards
		for i := 0 ; i < len(ship.Conf.Shards) ; i++ {
			ship.Conf.Shards[i] = int64(activeGroups[i%len(activeGroups)])
		}


		ship.UID = (args.GID*100)+int64(20+sm.me)
		ship.Conf.Num = previousConfig.Num + 1
		//fmt.Printf("## Start Join %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
		ship.Query = -2
		sm.px.Start(sm.currentSeq,*ship)
		for {
			//fmt.Printf("Start LEAVE %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
			status, vvv = sm.px.Status(sm.currentSeq)
			if status == paxos.Decided {
				vvv2 := vvv.(Op)
				if vvv2.UID != ship.UID {
					//fmt.Printf("not DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = false
					break
				} else {
					//to = 10 * time.Millisecond
					//fmt.Printf("DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = true
					break
				}
			}
		}
		if flag {
			break
		} else{
			continue
		}
	}

	//fmt.Printf(">>> DONE LEAVE %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
	sm.mu.Unlock()
	return nil
}



func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	//fmt.Printf("Move GID = %d shards= %d , server: %d \n" ,args.GID,args.Shard,sm.me)

	var flag bool
	sm.mu.Lock()
	ship := new(Op)
	//to := 10 * time.Millisecond
	for {
		ship.Conf.Groups = make(map[int64][]string)
		sm.currentSeq = sm.px.Max() + 1
		status, vvv := sm.px.Status(sm.currentSeq-1)
		//fmt.Printf("currSeq = %d Join GID = %d, server: %d \n" ,sm.currentSeq,args.GID,sm.me)
		var vvv1 Op
		if status == paxos.Decided {
			//fmt.Printf("DD 11 Seq = %d \n" ,sm.currentSeq-1)
			vvv1 = vvv.(Op)
		}else {
			//fmt.Printf("not DD Seq = %d \n" ,sm.currentSeq-1)
			continue
		}
		previousConfig := vvv1.Conf
		//Copy info about old config to the new one
		//fmt.Printf("\n$$$$$$\nJoin Prev Config currSeq = %d  GID = %d, server: %d \n%v\n$$$$$$\n" ,sm.currentSeq,args.GID,sm.me,previousConfig.Groups)
		for k := range previousConfig.Groups {
			//fmt.Printf("TRANSFER GID = %d, server: %d k: %d \n" ,args.GID,sm.me,k)
			ship.Conf.Groups[k] = previousConfig.Groups[k]
		}

		//Find the active groups to assign Shards
		activeGroups := make([]int64, 0, len(ship.Conf.Groups))
		for k := range ship.Conf.Groups {
			activeGroups = append(activeGroups, k)
		}

		//Assigning Shards
		for i := 0 ; i < len(ship.Conf.Shards) ; i++ {
			if i == args.Shard {
				//fmt.Printf(" >>>>>> i = %d GID = %d\n" ,i,args.GID)
				ship.Conf.Shards[i] = args.GID
			} else {
				//fmt.Printf(" <<<<<< i = %d GID = %d\n" ,i,args.GID)
				ship.Conf.Shards[i] = previousConfig.Shards[i]
			}
		}
		ship.UID = (args.GID*100)+int64(30+sm.me)
		ship.Conf.Num = previousConfig.Num + 1
		//fmt.Printf("## Start Join %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
		ship.Query = -2
		sm.px.Start(sm.currentSeq,*ship)
		for {
			status, vvv = sm.px.Status(sm.currentSeq)
			if status == paxos.Decided {
				vvv2 := vvv.(Op)
				if vvv2.UID != ship.UID {
					//fmt.Printf("not DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = false
					break
				} else {
					//to = 10 * time.Millisecond
					//fmt.Printf("DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = true
					break
				}
			}
		}
		if flag {
			break
		} else{
			continue
		}
	}
	//fmt.Printf(">>> DONE MOVE %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
	sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {

	//fmt.Printf("Query num = %d, server: %d \n" ,args.Num,sm.me)


	// Initial Q set to Paxos.Max() if -1. It will change later
	var q int
	if args.Num == -1  || args.Num > sm.px.Max(){
		//fmt.Printf("MAX : %d server : %d\n ",sm.px.Max(),sm.me)
		q = sm.px.Max()
	} else {
		q = args.Num
	}


	var flag bool
	sm.mu.Lock()
	ship := new(Op)
	for {
		ship.Conf.Groups = make(map[int64][]string)
		sm.currentSeq = sm.px.Max() + 1
		status, vvv := sm.px.Status(sm.currentSeq-1)
		//fmt.Printf("currSeq = %d Join GID = %d, server: %d \n" ,sm.currentSeq,args.GID,sm.me)
		var vvv1 Op
		if status == paxos.Decided {
			vvv1 = vvv.(Op)
		}else {
			continue
		}

		previousConfig := vvv1.Conf
		//Copy info about old config to the new one
		for k := range previousConfig.Groups {
			ship.Conf.Groups[k] = previousConfig.Groups[k]
		}

		ship.Conf.Shards = previousConfig.Shards

		ship.UID = int64(q*100)+int64(40+sm.me)
		ship.Conf.Num = previousConfig.Num

		//fmt.Printf("## Start Join %d server %d SEQ %d  \n",args.GID,sm.me,sm.currentSeq)
		ship.Query = q
		sm.px.Start(sm.currentSeq,*ship)
		for {
			status, vvv = sm.px.Status(sm.currentSeq)
			if status == paxos.Decided {
				vvv2 := vvv.(Op)
				if vvv2.UID != ship.UID {
					//fmt.Printf("not DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = false
					break
				} else {
					//to = 10 * time.Millisecond
					//fmt.Printf("DD 11 Seq = %d \n" ,sm.currentSeq)
					flag = true
					break
				}
			}
		}
		if flag {
			break
		} else{
			continue
		}
	}

	ytr := 0
	status,v := sm.px.Status(sm.px.Max()  - ytr)
	for {
		//fmt.Printf("MAXXXXXXX : %d server : %d ytr: %d \n ",sm.px.Max(),sm.me,ytr)
		if status == paxos.Decided {
			//fmt.Printf("DD 11 Seq = %d  server= %d\ninterface = %v \n" ,q,sm.me, v)
			v1 := v.(Op)
			if args.Num == -1 {
				reply.Config = v1.Conf
				if reply.Config.Num == 0 {
					//fmt.Printf("CATCHHHHH\n")
					delete(reply.Config.Groups,0)
				}
				//delete(reply.Config.Groups,0)
				//fmt.Printf("***\nNUM = %d\n%v\n%v\n***\n", reply.Config.Num,reply.Config.Groups,reply.Config.Shards)
				break
			} else {
				if v1.Conf.Num == q{
					reply.Config = v1.Conf
					if reply.Config.Num == 0 {
						//fmt.Printf("CATCHHHHH\n")
						delete(reply.Config.Groups,0)
					}
					//delete(reply.Config.Groups,0)
					//fmt.Printf("*#*\nNUM = %d\n%v\n%v\n***\n", reply.Config.Num,reply.Config.Groups,reply.Config.Shards)
					break
				} else {
					ytr = ytr + 1
					status,v = sm.px.Status(sm.px.Max() - ytr)
					continue
				}
			}
		}
	}
	sm.mu.Unlock()

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


	//fmt.Printf("First Group GID = %d seq = %d \n" ,args.GID,sm.currentSeq)
	
	ship1 := new(Op)
	//ship.Conf := new (Config)
	ship1.Conf.Num = 0
	ship1.Conf.Groups = make(map[int64][]string)
	ship1.Conf.Groups[0] = []string{}
	//ship.Conf.Shards = []int64{}
	
	
	
	//sm.currentSeq = 0


	//sm.configs = make([]Config, 1)
	//sm.configs[0].Groups = map[int64][]string{}
	//sm.activeGroups = make([]int64,1)
	//sm.seenGID = make(map[int64]int)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)
	
	// if sm.me == 0 {
		// fmt.Printf("Server %d send 0000000 \n" ,sm.me)
		// sm.px.Start(sm.currentSeq,ship1)
	// }
	
	sm.px.SetLog(0,*ship1)
	
	//fmt.Printf("Server %d send 0000000 \n" ,sm.me)
	//sm.px.Start(sm.currentSeq,ship1)
	
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
