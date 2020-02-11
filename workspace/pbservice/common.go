package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrWrongView = "ErrWrongView"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	What string
	Numview uint
	XID int64 // for duplicates

}


type PutAppendReply struct {
	Err Err
}


type GetArgs struct {
	Key string
	Numview uint
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type UpdateArgs struct {
	DB map[string]string
}

// For Updating Backup
type UpdateReply struct {
	Err Err
}


type ForPutAppendReply struct {
	Err Err
}

//For Forwarding to Backup
type ForPutAppendArgs struct {
	Key   string
	Value string
	What string
	Numview uint
	XID int64 // for duplicates

}