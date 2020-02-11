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
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
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
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   		Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)


type ProposerInstance struct{
	//Proposer
	seq 				int
	preN 				int // N for prepare phase
	preValue			interface{} // the value to be accepted. Subject to change
	chooseN				int // highest N seen so far, for propose number to be higher than this

}


type AcceptorInstance struct{
	seq 				int
	//Acceptor
	minProposal 		int // n_p highest prepare seen
	acceptedProposal 	int // n_a highest accept seen
	acceptedValue 		interface{} // v_a highest accept seen
}





type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	// Your data here.

	//General
	log 		map[int]interface{}
	instPre		map[int]ProposerInstance
	instAcc		map[int]AcceptorInstance
	zValue		[]int
	isDecided	map[int]bool

	//MAX, MIN, DONE
	maxValue 	int
	minValue 	int
	doneValue	int
}


type PrepareArgs struct {
	N 			int
	Seq 		int
	Done 		int
	DoneIndex 	int
}

type PrepareReply struct {
	Err 			string
	AcceptedValue 	interface{}
	AcceptedPreNum 	int
	CurrentPreNum 	int
	
}

type AcceptArgs struct {
	N int
	Value interface{}
	Seq int
}

type AcceptReply struct {
	Err string
	MinProposal int
}

type DecidedArgs struct {
	Value interface{}
	Seq int
}


type DecidedReply struct {
	Err string
}

func (px *Paxos) GetLog(seq int) interface{} {
	return px.log[seq]
}

func (px *Paxos) PrintStat(){
	//px.mu.Lock()
	fmt.Printf("\n\n************ Current Stat for %d ***************** \n*\n*\n",px.me)
	fmt.Printf("Prepare Instances:\n",)
	for i:= px.Min() ; i< px.Max() ; i++{
		fmt.Printf("seq : %d px.preInst[%d] = %v \n",i,i,px.instPre[i])
	}
	for i:= px.Min() ; i< px.Max() ; i++{
		fmt.Printf("seq : %d px.preAcc[%d] = %v \n",i,i,px.instAcc[i])
	}
	for i:= px.Min() ; i< px.Max() ; i++{
		fmt.Printf("seq : %d px.IsDecided[%d] = %v \n",i,i,px.isDecided[i])
	}
	fmt.Printf("\n\n************ END ***************** \n\n")
	//px.mu.Unlock()
}

func (px *Paxos) nGen(n int, seq int) int {
	number := ((px.instPre[seq].chooseN/10)+1)*10+n
	//fmt.Printf("******** (((%d))) INSIDE N GENERATOR %d LIST %d\n",px.me,number,px.instPre[seq].chooseN)
	return number
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


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//fmt.Printf("%d :: Propose * SEQ: %v | VALUE: %v\n",px.me,seq,v)
	
	px.mu.Lock()
	if seq > px.maxValue{
		//fmt.Printf("(%d) IF MAX \n",px.me)
		px.maxValue = seq
	}
	px.mu.Unlock()
	
	if px.isDecided[seq] {
		fmt.Printf("ALREADY %d seq %d\n",px.me,seq)
	} else if seq < px.Min() {
		fmt.Printf("%d :: Seq < MIN \n",px.me)
	} else {
		px.mu.Lock()
		px.isDecided[seq] = false
		px.mu.Unlock()
		//px.PrintStat()
		go func(){
		for ! px.isDecided[seq] {
			prepMajor := 0
			accMajor := 0
			highestAcceptedSeen := 0
			ttt := ProposerInstance{}
			ttt.seq = seq
			ttt.preValue = v
			ttt.preN = px.nGen(px.me,seq)
			
			px.mu.Lock()
			px.instPre[seq] = ttt
			px.mu.Unlock()
			//fmt.Printf("START pID: %d , SEQ: %d ,Proposal N: %d, Value: %v\n",px.me,seq,ttt.preN,v)
			//px.PrintStat()
			//ttt = ProposerInstance{}
			for idx := 0; idx < len(px.peers); idx++ {
				//fmt.Printf("pID: %d , SEQ: %d , Proposal N: %d Value: %v TO: %d\n",px.me,seq,ttt.preN,v,idx)
			//	px.PrintStat()
				args := &PrepareArgs{}
				args.N = px.instPre[seq].preN
				args.Seq = seq
				args.Done = px.doneValue
				args.DoneIndex = px.me
				var reply PrepareReply
				if px.me == idx{
					//fmt.Printf("%d,%d :: PREPARE ITSELF\n",px.me,seq)
					px.Prepare(args,&reply)
				} else {
					//fmt.Printf("%d,%d :: PREPARE TO (%d) \n",px.me,seq,idx)
					ok := call(px.peers[idx], "Paxos.Prepare", args, &reply)
					if ok == false {
						//fmt.Printf("%d,%d :: CALL PREPARE FAILED for peer (%d) \n",px.me,seq,idx)
						//fmt.Errorf("CALL Decided failed")
					}
				}
				if reply.Err == "OK" {
					prepMajor += 1
					if reply.AcceptedPreNum > highestAcceptedSeen { // To be Compeleted
						//fmt.Printf("REPLY.AcceptPreNum : %d **** highestAcceptedSeen : %d\n",reply.AcceptedPreNum, highestAcceptedSeen)
						px.mu.Lock()
						highestAcceptedSeen = reply.AcceptedPreNum
						fmt.Printf("%d::%d  VALUE CHANGED \n",px.me,seq)
						t1 := px.instPre[seq]
						t1.chooseN = reply.AcceptedPreNum
						t1.preValue = reply.AcceptedValue
						px.instPre[seq] = t1
						px.mu.Unlock()
						//t1 = ProposerInstance{}
					} else{
						//fmt.Printf("%d,%d :: PREPARE OK from %d -  NO VALUE CHANGE\n",px.me,seq,idx)
						px.mu.Lock()
						if (px.instPre[seq].chooseN < reply.AcceptedPreNum){
							t1 := px.instPre[seq]
							t1.chooseN = reply.AcceptedPreNum
							px.instPre[seq] = t1
						}
						px.mu.Unlock()
					}
				} else{

					//fmt.Printf("%d,%d:: PREPARE REJECTED (%d) \n",px.me,seq,idx)
					//fmt.Printf("@(RRRR) Prepare Reply from %d : acceptedPreNum : %d | accVal : %v | currentN : %d\n",idx,reply.AcceptedPreNum , reply.AcceptedValue , reply.CurrentPreNum)
					px.mu.Lock()
					if (px.instPre[seq].chooseN < reply.AcceptedPreNum){
						t1 := px.instPre[seq]
						t1.chooseN = reply.AcceptedPreNum
						//fmt.Printf("%d,%d :: CHOOOOOOOOOOSSSEE NNNNN : %d \n",px.me,seq,t1.chooseN)
						px.instPre[seq] = t1
						//t1 = ProposerInstance{}
						//fmt.Printf("%d,%d :: KIRRRRRRRRRRRRRRRRRRRRR : %d \n",px.me,seq,px.instPre[seq].chooseN)
					}
					px.mu.Unlock()
					continue

				}

				//fmt.Printf("%d,%d :: @ Prepare Reply from (%d) : acceptedPreNum : %d | accVal : %v | currentN : %d\n",px.me,seq,idx,reply.AcceptedPreNum , reply.AcceptedValue , reply.CurrentPreNum)
				
			}
			
			if prepMajor < (len(px.peers)/2) + 1{
				//fmt.Printf("%d,%d :: NOT MAJORITY PREPARE \n",px.me,seq)
				continue
			}
			fmt.Printf("%d,%d :: Majority Prepared \n",px.me,seq)
			//px.PrintStat()
			for idx := 0; idx < len(px.peers); idx++ {
				args := &AcceptArgs{}
				args.N = px.instPre[seq].preN
				args.Value = px.instPre[seq].preValue
				args.Seq = seq
				var reply AcceptReply
				if px.me == idx{
					px.Accept(args,&reply)
					//fmt.Printf("%d,%d :: ACCEPT ITSELF\n",px.me,seq)
				}else{
					ok := call(px.peers[idx], "Paxos.Accept", args, &reply)
					if ok == false {
						//fmt.Printf("%d,%d :: CALL ACCEPT FAILED for peer (%d) \n",px.me,seq,idx)
						//fmt.Errorf("CALL Decided failed")
					}
				}
				//fmt.Printf("%d,%d :: # Accept Reply from (%d) : minProposal : %d \n",px.me,seq,idx,reply.MinProposal)
				
				
				
				if reply.Err == "OK" {
					accMajor += 1
					if reply.MinProposal > highestAcceptedSeen { // To be Compeleted
						//fmt.Printf("REPLY.AcceptPreNum : %d **** highestAcceptedSeen : %d\n",reply.AcceptedPreNum, highestAcceptedSeen)
						px.mu.Lock()
						highestAcceptedSeen = reply.MinProposal
						//fmt.Printf("VALUE CHANGED \n")
						t1 := px.instPre[seq]
						t1.chooseN = reply.MinProposal
						px.instPre[seq] = t1
						px.mu.Unlock()
						//t1 = ProposerInstance{}
					} else{
						//fmt.Printf("%d,%d :: PREPARE OK from %d -  NO VALUE CHANGE\n",px.me,seq,idx)
						px.mu.Lock()
						if (px.instPre[seq].chooseN < reply.MinProposal){
							t1 := px.instPre[seq]
							t1.chooseN = reply.MinProposal
							px.instPre[seq] = t1
						}
						px.mu.Unlock()
					}
				} else{

					//fmt.Printf("%d,%d:: ACCEPT REJECTED (%d) \n",px.me,seq,idx)
					//fmt.Printf("@(RRRR) Prepare Reply from %s : acceptedPreNum : %d | accVal : %v | currentN : %d\n",px.peers[idx],reply.AcceptedPreNum , reply.AcceptedValue , reply.CurrentPreNum)
					px.mu.Lock()
					if (px.instPre[seq].chooseN < reply.MinProposal){
						t1 := px.instPre[seq]
						t1.chooseN = reply.MinProposal
						highestAcceptedSeen = reply.MinProposal
						//fmt.Printf("%d,%d :: CHOOOOOOOOOOSSSEE NNNNN : %d \n",px.me,seq,t1.chooseN)
						px.instPre[seq] = t1
						//t1 = ProposerInstance{}
						//fmt.Printf("%d,%d :: KIRRRRRRRRRRRRRRRRRRRRR : %d \n",px.me,seq,px.instPre[seq].chooseN)
					}
					px.mu.Unlock()
					continue

				}
				
				
				
				
				
				
				
				// if reply.Err == "OK" {
					// accMajor += 1
				// } else {
					// fmt.Printf("%d,%d ACCEPT  REJECTED\n",px.me,seq)
					// continue
				// }
				
				
				
			}

			if accMajor < (len(px.peers)/2) + 1{
				//fmt.Printf("%d,%d :: NOT Majority Accepted \n",px.me,seq)
				continue
			}
			fmt.Printf("%d,%d :: Majority Accepted \n",px.me,seq)
			//px.PrintStat()
			px.mu.Lock()
			//fmt.Printf("DECC(%d) : %v for seq %d\n",px.me,px.instPre[seq].preValue,seq)
			px.log[seq] = px.instPre[seq].preValue
			px.isDecided[seq] = true
			px.mu.Unlock()
			//px.PrintStat()
			for i := 0; i < len(px.peers); i++ {
				if px.me == i {
					continue
				}
				args2 := &DecidedArgs{}
				args2.Value = px.instPre[seq].preValue
				args2.Seq = seq
				var reply2 DecidedReply
				//fmt.Printf("%d,%d :: DECIDED %d before \n",px.me,seq,i)
				ok := call(px.peers[i], "Paxos.Decided", args2, &reply2)
				if ok == false {
					fmt.Errorf("CALL Decided failed")
				}
			}
		}
	}()
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	//fmt.Printf("doneCALLED %d for seq %d",px.me,seq)
	px.mu.Lock()
	px.doneValue = seq
	px.mu.Unlock()
	//fmt.Printf("BEFORE DONE(%d) MIN : %d LEN LOG %d \n",px.me,px.minValue,len(px.log))
	//fmt.Printf("AFTER DONE(%d) MIN : %d LEN LOG %d \n",px.me,px.minValue,len(px.log))
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.maxValue
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
	px.mu.Lock()
	min := 100
	for i := 0; i < len(px.zValue); i++ {
		if px.zValue[i] < min {
			min = px.zValue[i]
		}
	} 
	px.minValue = min+1
	//fmt.Printf("(%d) MIN : %d LEN LOG %d \n",px.me,px.minValue,len(px.log))
	for i := 0; i < min + 1; i++ {
		delete(px.log,i)
		delete(px.instAcc,i)
		delete(px.instPre,i)
	}
	px.mu.Unlock()
	// tmp := make(map[int]interface{})
	// tmp = px.log
	// px.log = nil
	// px.log = tmp
	//fmt.Printf("(%d) MIN : %d LEN LOG %d \n",px.me,px.minValue,len(px.log))
	//fmt.Printf("minCALLED(%d) MIN : %d \n",px.me,px.minValue)
	return min+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq > px.maxValue{
	//	fmt.Printf("MAX : SEQ %d MAX %d\n",seq,px.maxValue)
		return Pending, nil
	}
	
	if seq < px.minValue {
		return Forgotten,nil
	}
	if px.log[seq] != nil{
		//fmt.Printf("STATUS of %d for seq %d : DECIDED value %v \n",px.me,seq,px.log[seq])
		return Decided, px.log[seq]
	} else{
		return Pending, nil
	}

}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
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
	// Your initialization code here.
	px.instPre = make(map[int]ProposerInstance)
	px.instAcc = make(map[int]AcceptorInstance)
	px.log = make(map[int]interface{})
	px.zValue = make([]int, len(peers))
	px.isDecided = make(map[int]bool)
	for i := 0; i < len(peers); i++ {
		px.zValue[i] = -1
	}
	px.maxValue = -1
	px.doneValue = -1

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
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}
	return px
}




func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	//fmt.Printf("P(%d) seq %d with N = %d \n",px.me,args.Seq,args.N)
	px.mu.Lock()
	if args.Seq > px.maxValue{
		//fmt.Printf("(%d) IF MAX \n",px.me)
		px.maxValue = args.Seq
	}
	if px.zValue[args.DoneIndex] < args.Done {
		px.zValue[args.DoneIndex] = args.Done
	}
	px.mu.Unlock()
	if val, ok := px.instAcc[args.Seq]; ok{
		// if px.isDecided[args.Seq] {
			//fmt.Printf("(%d) LOWWWWWW seq %d\n",px.me,args.Seq)
			//fmt.Printf("(%d) loooooo CurrN= %d , MINPROP= %d \n",px.me,args.N,val.minProposal)
			// reply.AcceptedPreNum = val.minProposal
			// fmt.Printf("P(%d) REJECT 1111 ACCPRENUM %d VAL.MINPROP %d \n",px.me,reply.AcceptedPreNum ,val.minProposal)
			// reply.Err = "REJECT"
		// } else if args.N >= val.minProposal {
		if args.N >= val.minProposal {
			//fmt.Printf("(%d) PRE - OK CurrN= %d , MINPROP= %d \n",px.me,args.N,val.minProposal)
			val.minProposal = args.N
			
			px.mu.Lock()
			px.instAcc[args.Seq]= val
			px.mu.Unlock()
			
			reply.AcceptedPreNum = val.acceptedProposal
			reply.AcceptedValue = val.acceptedValue
			reply.CurrentPreNum = args.N
			reply.Err = "OK"
			//val = AcceptorInstance{}
		} else{
			//fmt.Printf("(%d) PRE - NOT OK CurrN= %d , MINPROP= %d \n",px.me,args.N,val.minProposal)
			reply.AcceptedPreNum = val.minProposal
			//fmt.Printf("P(%d) REJECT 2222\n",px.me)
			reply.Err = "REJECT"
		}
	} else{
		//fmt.Printf("(%d) PRE - NOT IN LIST \n",px.me)
		act := AcceptorInstance{}
		act.acceptedProposal = -1
		act.acceptedValue = ""
		act.minProposal = args.N
		
		px.mu.Lock()
		px.instAcc[args.Seq] = act
		px.mu.Unlock()
		
		reply.AcceptedPreNum = act.acceptedProposal
		reply.AcceptedValue = act.acceptedValue
		reply.CurrentPreNum = args.N
		reply.Err = "OK"

	}
	return nil
}


func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	fmt.Printf("A(%d) seq %d with N = %d and Val = %v\n",px.me,args.Seq,args.N,args.Value)
	//fmt.Printf("A(%d) ARGS_N : %d , px.instAcc[%d].minproposal : %d\n",px.me,args.N,args.Seq,px.instAcc[args.Seq].minProposal)
	//if px.isDecided[args.Seq] {
		//fmt.Printf("(%d) HIiii seq %d\n",px.me,args.Seq)
		//fmt.Printf("HIII A(%d) N(%d) > MinProposal(%d)\n",px.me,args.N,px.instAcc[args.Seq].minProposal)
		//reply.MinProposal = px.instAcc[args.Seq].minProposal
		//reply.Err = "REJECT"
	//} else if args.N >= px.instAcc[args.Seq].minProposal{
	if args.N >= px.instAcc[args.Seq].minProposal{
		//fmt.Printf("---A(%d) N(%d) > MinProposal(%d)\n",px.me,args.N,px.instAcc[args.Seq].minProposal)
		t2 := px.instAcc[args.Seq]
		t2.acceptedProposal = args.N
		t2.acceptedValue = args.Value
		t2.minProposal = args.N
		
		px.mu.Lock()
		px.instAcc[args.Seq] = t2
		px.mu.Unlock()
		
		reply.Err = "OK"
		reply.MinProposal = args.N
		//t2 = AcceptorInstance{}
		
	} else {
		//fmt.Printf("---A(%d) REJECTED\n",px.me)
		reply.MinProposal = px.instAcc[args.Seq].minProposal
		reply.Err = "REJECT"
	}

	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	if px.isDecided[args.Seq] {
		//fmt.Printf("FaTAAAAAAAAAAAAALLLL(%d) : %v for seq %d\n",px.me,args.Value,args.Seq)	
	}
	fmt.Printf("DECCC(%d) : seq %d\n",px.me,args.Seq)	
	px.mu.Lock()
	px.isDecided[args.Seq] = true
	px.log[args.Seq] = args.Value
	px.mu.Unlock()
	
	return nil
}


