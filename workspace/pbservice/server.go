package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	database   map[string]string
	isPrimary  bool
	isBackup   bool
	backup	   string
	currentView viewservice.View
	requests map[int64]int64
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Print Statements for Debug

//	fmt.Printf("\nServer GET ARGS.PRIMARY SERVER = %s \n", args.PrimaryServer)
//	fmt.Printf("\n SERVER GET ARGS.key = %s \n", args.Key)
//	fmt.Printf("\n SERVER GET PB.ME = %s \n", pb.me)
//	fmt.Printf("\nServer:Get CURRENT VIEW : %s * %s * %d \n",pb.currentView.Primary ,pb.currentView.Backup ,pb.currentView.Viewnum)

	if args.Numview == pb.currentView.Viewnum && pb.isPrimary{
		reply.Value = pb.database[args.Key]
		reply.Err = OK
	}else{
		reply.Err = ErrWrongServer
	}

	return nil
}



func (pb *PBServer) Update(args1 *UpdateArgs, reply1 *UpdateReply) error {

	if ! pb.isBackup{
		reply1.Err = "ERROR...!"
	} else {
		pb.database = args1.DB
		reply1.Err = OK
	}

	return nil
}

func (pb *PBServer) ForPutAppend(args1 *ForPutAppendArgs, reply1 *ForPutAppendReply) error {

	if ! pb.isBackup{
		reply1.Err = "ERROR...!"
	} else {
		if args1.What == "Put"{
			pb.database[args1.Key] = args1.Value
		} else {
			value, ok := pb.database[args1.Key]
			if ok {
				pb.database[args1.Key] = value + args1.Value
			} else {
				pb.database[args1.Key] = args1.Value
			}
		}
		reply1.Err = OK
	}

	return nil
}






func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Print Statements for Debug

//	fmt.Printf("\nServer:Put CURRENT VIEW : %s * %s * %d \n",pb.currentView.Primary ,pb.currentView.Backup ,pb.currentView.Viewnum)
//	fmt.Printf("\n ARGS.NumVIEW = %s \n", args.Numview)
//	fmt.Printf("\n PRIMARY PB.ME = %s \n", pb.me)
//	fmt.Printf("\n Priamar? %t \n", pb.isPrimary)
//	fmt.Printf("\n ARGS.KEY : %s  VALUE : %s XID: %d\n",args.Key,args.Value,args.XID)

	pb.mu.Lock()// For concurrent operations
	if pb.requests[args.XID] != 0 { // Taking care of Duplicates
		reply.Err = OK
	} else if args.Numview == pb.currentView.Viewnum && pb.isPrimary { // New Request
		pb.requests[args.XID] = args.XID
		if args.What == "Put" {
			pb.database[args.Key] = args.Value
		} else { // APPEND
			value, ok := pb.database[args.Key]
			if ok {
				pb.database[args.Key] = value + args.Value
			} else {
				pb.database[args.Key] = args.Value
			}
		}
		if pb.currentView.Backup != "" { // Forward Updates to Backup
			args1 := &ForPutAppendArgs{}
			args1.Key = args.Key
			args1.Numview = args.Numview
			args1.Value = args.Value
			args1.What = args.What
			var reply1 ForPutAppendReply
			ok := call(pb.currentView.Backup, "PBServer.ForPutAppend", args1, &reply1)
			if ok == false {
				fmt.Errorf("FORWARD FAILED!\n")
			}
		}
		reply.Err = OK
	}else { // Client requested to the Wrong server. It makes the client to update the view (client.go)
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}





//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	pb.currentView, _ = pb.vs.Ping(pb.currentView.Viewnum)
	if pb.currentView.Primary == pb.me { // Update the backup
		if pb.currentView.Backup != "" {
			args := &UpdateArgs{}
			args.DB = pb.database
			var reply UpdateReply
			ok := call(pb.currentView.Backup, "PBServer.Update", args, &reply)
			if ok == false {
				fmt.Errorf("UPDATE FAILED!\n")
			}
		}
		pb.isPrimary = true
	} else if pb.currentView.Backup == pb.me && !pb.isBackup {
		pb.isBackup = true
	}
	if pb.isPrimary {
		pb.backup = pb.currentView.Backup
		if pb.currentView.Primary != pb.me {
			pb.isPrimary = false
		}
	}
}


// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	//Saeed <<
	
	pb.database = make(map[string]string)
	pb.requests = make(map[int64]int64)
	pb.isBackup = false
	pb.isPrimary = false
	pb.currentView = viewservice.View{Primary:"",Backup:"",Viewnum:0}
	pb.backup = pb.currentView.Backup
	
	//Saeed >>
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
