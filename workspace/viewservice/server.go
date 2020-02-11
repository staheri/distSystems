package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import (
	"sync/atomic"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string
	currentValidView View // Current View
	preValidView View // Previous View
	track map[string]time.Time // Keep Track of alive/dead servers
	isAcked bool
	vol string // Keep idle servers wait as backup candidates.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Print Statements for Debug

//	fmt.Printf("\n\n\nCurrent view: %d \n P: %s\n B: %s\n",vs.currentValidView.Viewnum,vs.currentValidView.Primary,vs.currentValidView.Backup)
//	fmt.Printf("Previous view: %d \n P: %s\n B: %s\n",vs.preValidView.Viewnum,vs.preValidView.Primary,vs.preValidView.Backup)
//	fmt.Printf("Ping %d from %s\n",args.Viewnum,args.Me)


	vs.track[args.Me] = time.Now()
	if args.Me != ""{

//		fmt.Printf("\n ARGS.ME = %s \n", args.Me)
//		fmt.Printf("\n Curren Primary = %s \n", vs.currentValidView.Primary)
//		fmt.Printf("\n ARGS.Veiwnum = %s \n", args.Viewnum)
//		fmt.Printf("\n Curren ViewNum = %s \n", vs.currentValidView.Viewnum)

		if vs.currentValidView.Viewnum == 0{ // first primary --- First Primary Test
			vs.currentValidView.Primary = args.Me
			vs.currentValidView.Viewnum = 1
			vs.isAcked = true
		} else if args.Me != vs.currentValidView.Primary {
			if vs.currentValidView.Backup == "" { // first backup --- First Backup Test
				vs.preValidView = vs.currentValidView
				vs.currentValidView.Backup = args.Me
				vs.currentValidView.Viewnum += 1
				vs.isAcked = false // CHANGE POINT --- for wait for ack test
			} else if args.Me != vs.currentValidView.Backup { // idle third server test
				// Add the idle third server to a volunteer for later assignments
				vs.vol = args.Me
			} else if vs.vol != "" && vs.currentValidView.Backup == ""{ // Assign the vol as backup if there is no backup
				vs.preValidView = vs.currentValidView
				vs.currentValidView.Backup = vs.vol
				vs.vol = ""
				vs.currentValidView.Viewnum += 1
				//vs.isAcked = true
			}
		} else if args.Me == vs.currentValidView.Primary && args.Viewnum == 0{ // Restarted Primary Dead test
			vs.preValidView = vs.currentValidView
			vs.currentValidView.Primary = vs.currentValidView.Backup
			vs.currentValidView.Backup = ""
			vs.currentValidView.Viewnum += 1
			vs.isAcked = false
		} else if args.Me ==  vs.currentValidView.Primary &&  ( args.Viewnum == vs.currentValidView.Viewnum || args.Viewnum == vs.preValidView.Viewnum) { // Acking by Primary
			vs.isAcked = true
		}
	}
	if vs.isAcked{
		reply.View = vs.currentValidView
	} else {
		reply.View = vs.preValidView
	}
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentValidView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	//fmt.Printf("\nTIME %d \n",time.Now().Sub(vs.track[vs.currentValidView.Primary]))
	//fmt.Printf("\nVS PULS %s\n",vs.me)
	//fmt.Printf("\nVS PULS CURRENT VIEW * %s * %s * %d \n", vs.currentValidView.Primary,vs.currentValidView.Backup, vs.currentValidView.Viewnum)
	//fmt.Printf("\nVS PULS VOL %s\n",vs.vol)
	if vs.isAcked{// for Viewserver waits for primary to ack view test- Even if the Primaray died without Acking, there should not be any promotion
		if time.Now().Sub(vs.track[vs.currentValidView.Primary]) > PingInterval*DeadPings && vs.currentValidView.Primary != ""{ // Check to see if the primary died
			vs.preValidView = vs.currentValidView
			vs.currentValidView.Primary = vs.currentValidView.Backup
			if vs.vol != "" { // Assign the volunteer to be backup if there is any (not just up there but here )

				vs.currentValidView.Backup = vs.vol
				vs.vol = ""
			} else {
				vs.currentValidView.Backup = ""
			}
			vs.currentValidView.Viewnum += 1
			vs.isAcked = false
		}
		if time.Now().Sub(vs.track[vs.currentValidView.Backup]) > PingInterval*DeadPings && vs.currentValidView.Backup != ""{ // Check to see if the Backup died
			//fmt.Printf("\nCATCH\n")
			//fmt.Printf("\nINSIDE IF\n")
			vs.preValidView = vs.currentValidView
			if vs.vol != "" { // Assign the volunteer to be backup if there is any (not just up there but here )
				vs.currentValidView.Backup = vs.vol
				vs.vol = ""
			} else {
				vs.currentValidView.Backup = ""
			}
			vs.currentValidView.Viewnum += 1
			vs.isAcked = false
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.rpccount = vs.GetRPCCount()
	vs.dead = 0
	vs.currentValidView = View{Primary:"",Backup:"",Viewnum:0}
	vs.preValidView = vs.currentValidView
	vs.isAcked = false
	vs.track = make(map[string]time.Time)
	vs.vol = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
