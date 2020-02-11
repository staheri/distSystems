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

func (px *Paxos) SetLog(seq int, v interface{}) {
	px.mu.Lock()
	px.maxValue = seq
	px.isDecided[seq] = true
	px.log[seq] = v
	px.mu.Unlock()
}

func (px *Paxos) nGen(n int, seq int) int {
	number := ((px.instPre[seq].chooseN/10)+1)*10+n
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

	// Set the Max
	px.mu.Lock()
	if seq > px.maxValue{
		px.maxValue = seq
	}
	px.mu.Unlock()
	

	if px.isDecided[seq] {
		// This seq is already decided
	} else if seq < px.Min() {
		// This seq is already forgotten
	} else {
		px.mu.Lock()
		px.isDecided[seq] = false
		px.mu.Unlock()
		go func(){
		for ! px.isDecided[seq] {
			prepMajor := 0
			accMajor := 0
			highestAcceptedSeen := 0

			//create Prepare package
			ttt := ProposerInstance{}
			ttt.seq = seq
			ttt.preValue = v
			ttt.preN = px.nGen(px.me,seq)
			
			px.mu.Lock()
			px.instPre[seq] = ttt
			px.mu.Unlock()

			//Send Prepare to everybody
			for idx := 0; idx < len(px.peers); idx++ {
				//fmt.Printf("pID: %d , SEQ: %d , Proposal N: %d Value: %v TO: %d\n",px.me,seq,ttt.preN,v,idx)
				args := &PrepareArgs{}
				args.N = px.instPre[seq].preN
				args.Seq = seq
				args.Done = px.doneValue
				args.DoneIndex = px.me
				var reply PrepareReply
				if px.me == idx{
					// prepare itself
					px.Prepare(args,&reply)
				} else {
					ok := call(px.peers[idx], "Paxos.Prepare", args, &reply)
					if ok == false {
						//fmt.Printf("FAIL  %d \n",idx)
						continue
					}
				}

				if reply.Err == "OK" {
					prepMajor += 1
					if reply.AcceptedPreNum > highestAcceptedSeen { // prepare ok - value change - choose N change
						px.mu.Lock()
						highestAcceptedSeen = reply.AcceptedPreNum
						t1 := px.instPre[seq]
						t1.chooseN = reply.AcceptedPreNum
						t1.preValue = reply.AcceptedValue
						px.instPre[seq] = t1
						px.mu.Unlock()
					} else{
						px.mu.Lock()
						if (px.instPre[seq].chooseN < reply.AcceptedPreNum){ // prepare ok - choose N change
							t1 := px.instPre[seq]
							t1.chooseN = reply.AcceptedPreNum
							px.instPre[seq] = t1
						}
						px.mu.Unlock()
					}
				} else{ // prepare not ok - choose N change
					px.mu.Lock()
					if (px.instPre[seq].chooseN < reply.AcceptedPreNum) {
						t1 := px.instPre[seq]
						t1.chooseN = reply.AcceptedPreNum
						px.instPre[seq] = t1
					} 
					px.mu.Unlock()
					continue

				}
			}
			if prepMajor < (len(px.peers)/2) + 1{ // Start Over if not majority prepared
				continue
			}

			for idx := 0; idx < len(px.peers); idx++ { // Majority prepared - start sending accept
				args := &AcceptArgs{}
				args.N = px.instPre[seq].preN
				args.Value = px.instPre[seq].preValue
				args.Seq = seq
				var reply AcceptReply
				if px.me == idx{
					px.Accept(args,&reply)
				}else{
					ok := call(px.peers[idx], "Paxos.Accept", args, &reply)
					if ok == false {
						continue
					}
				}
				if reply.Err == "OK" {
					accMajor += 1
					if reply.MinProposal > highestAcceptedSeen { // accept ok - change highest accepted seen and choose N
						px.mu.Lock()
						highestAcceptedSeen = reply.MinProposal
						t1 := px.instPre[seq]
						t1.chooseN = reply.MinProposal
						px.instPre[seq] = t1
						px.mu.Unlock()
					} else{ //// accept ok - change choose N
						px.mu.Lock()
						if (px.instPre[seq].chooseN < reply.MinProposal){
							t1 := px.instPre[seq]
							t1.chooseN = reply.MinProposal
							px.instPre[seq] = t1
						}
						px.mu.Unlock()
					}
				} else{ // accept ok - change highest accepted seen and choose N
					px.mu.Lock()
					if (px.instPre[seq].chooseN < reply.MinProposal){
						t1 := px.instPre[seq]
						t1.chooseN = reply.MinProposal
						highestAcceptedSeen = reply.MinProposal
						px.instPre[seq] = t1
					}
					px.mu.Unlock()
					continue

				}
			}

			if accMajor < (len(px.peers)/2) + 1{
				continue
			}
			
			px.mu.Lock()
			px.log[seq] = px.instPre[seq].preValue
			px.isDecided[seq] = true
			px.mu.Unlock()
			for i := 0; i < len(px.peers); i++ {
				if px.me == i {
					continue
				}
				args2 := &DecidedArgs{}
				args2.Value = px.instPre[seq].preValue
				args2.Seq = seq
				var reply2 DecidedReply
				for tmp := 0 ; tmp < 10 ; tmp++ {
					ok := call(px.peers[i], "Paxos.Decided", args2, &reply2)
					if ok == false {
						fmt.Errorf("CALL Decided failed")
					} else {
						break
					}
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
	px.mu.Lock()
	px.doneValue = seq
	px.mu.Unlock()
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
	for i := 0; i < min + 1; i++ {
		delete(px.log,i)
		delete(px.instAcc,i)
		delete(px.instPre,i)
	}
	px.mu.Unlock()
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
	//fmt.Printf("STATUS max: %d server : %d servers: %v me: %v\n",px.Max(),px.me,px.peers,px.peers[px.me])
	if seq > px.maxValue{
		return Pending, nil
	}
	
	if seq < px.minValue {
		return Forgotten,nil
	}
	if px.log[seq] != nil{
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
	px.mu.Lock()
	if args.Seq > px.maxValue{
		//fmt.Printf("$$$$ MAX CHANGE from %d to %d server %d \n",px.maxValue,args.Seq,px.me)
		px.maxValue = args.Seq
	}
	if px.zValue[args.DoneIndex] < args.Done {
		px.zValue[args.DoneIndex] = args.Done
	}
	px.mu.Unlock()
	if val, ok := px.instAcc[args.Seq]; ok{
		if args.N >= val.minProposal {
			val.minProposal = args.N
			
			px.mu.Lock()
			px.instAcc[args.Seq]= val
			px.mu.Unlock()
			
			reply.AcceptedPreNum = val.acceptedProposal
			reply.AcceptedValue = val.acceptedValue
			reply.CurrentPreNum = args.N
			reply.Err = "OK"
		} else{
			reply.AcceptedPreNum = val.minProposal
			reply.Err = "REJECT"
		}
	} else{
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
	if args.N >= px.instAcc[args.Seq].minProposal{
		t2 := px.instAcc[args.Seq]
		t2.acceptedProposal = args.N
		t2.acceptedValue = args.Value
		t2.minProposal = args.N
		
		px.mu.Lock()
		px.instAcc[args.Seq] = t2
		px.mu.Unlock()
		
		reply.Err = "OK"
		reply.MinProposal = args.N
		
	} else {
		reply.MinProposal = px.instAcc[args.Seq].minProposal
		reply.Err = "REJECT"
	}

	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	px.isDecided[args.Seq] = true
	px.log[args.Seq] = args.Value
	px.mu.Unlock()
	
	return nil
}


