package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	currentView viewservice.View
	isFirst bool
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.currentView = viewservice.View{Primary:"",Backup:"",Viewnum:0}
	ck.isFirst = true

	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	if ck.isFirst { // Ping to VS if it is the first time (in order to not to Ping everytime)
		ck.currentView,_ = ck.vs.Ping(ck.currentView.Viewnum)
		ck.isFirst = false
	}
	args := &GetArgs{}
	args.Key = key
	args.Numview = ck.currentView.Viewnum
	var reply GetReply
	ok := call(ck.currentView.Primary, "PBServer.Get", args, &reply)
	if ok == false {
		fmt.Printf("CALL GET failed")
		fmt.Errorf("CALL GET failed")
	}
	for reply.Err == ErrWrongServer || reply.Err == "" { // If it sends request to the wrong server, update the view and do it again until it gets the right answer
		ck.currentView,_ = ck.vs.Ping(ck.currentView.Viewnum)
		args.Numview = ck.currentView.Viewnum
		ok := call(ck.currentView.Primary, "PBServer.Get", args, &reply)
		if ok == false {
			fmt.Errorf("CALL GET failed")
		}
		time.Sleep(viewservice.PingInterval)
	}
	return reply.Value
}

// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	if ck.isFirst { // Ping to VS if it is the first time (in order to not to Ping everytime)
		ck.currentView,_ = ck.vs.Ping(ck.currentView.Viewnum)
		ck.isFirst = false
	}
	args := &PutAppendArgs{}
	args.Key = key
	args.Numview = ck.currentView.Viewnum
	args.Value = value
	args.What = op
	args.XID = nrand() // Unique ID for requests
	var reply GetReply
	ok := call(ck.currentView.Primary, "PBServer.PutAppend", args, &reply)
	if ok == false {
		fmt.Errorf("CALL PUT failed")
	}
	if reply.Err == ErrWrongServer{ // If it sends request to the wrong server, update the view and do it again until it gets the right answer
		for reply.Err == ErrWrongServer {

			ck.currentView,_ = ck.vs.Ping(ck.currentView.Viewnum)
			args.Numview = ck.currentView.Viewnum
			ok := call(ck.currentView.Primary, "PBServer.PutAppend", args, &reply)
			if ok == false {
				fmt.Errorf("CALL GET failed")
			}
			time.Sleep(viewservice.PingInterval)
		}
	} else if reply.Err != OK{ // If nobody listen to the request, nobody reply or the primary died, update the view and do it again until it gets the right answer
		for reply.Err != OK {
			ck.currentView,_ = ck.vs.Ping(ck.currentView.Viewnum)
			args.Numview = ck.currentView.Viewnum
			ok := call(ck.currentView.Primary, "PBServer.PutAppend", args, &reply)
			if ok == false {
				fmt.Errorf("CALL PUT failed")
			}
			time.Sleep(viewservice.PingInterval)
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
