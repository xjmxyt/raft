package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"log"
	"math/rand"
	"time"
)

const(
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

const(
	TickerSleepTime = 20 * time.Millisecond 
	ElectionSleepTime = 50 * time.Millisecond // 选举睡眠时间
	HeartBeatSendTime = 50 * time.Millisecond // 心跳包发送时间
	CheckCommitSleepTime = 150 * time.Millisecond // 检查log是否被commit的时间
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm	 int // 2B: 任期号

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int		// 当前任期
	votedFor 	int		// 获得选票的总数
	state 		int		// 当前状态 follower, candidate, leader
	cntVoted 	int

	log			[]ApplyMsg

	heartBeatTimeOut	time.Time 

	debugLevel	int

	
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// volatile state
	commitIndex		int // 即将提交的最大log编号
	lastApplied		int	// state machine应用的最大编号
	applyCh 		chan ApplyMsg

	// 领导者上的易失性状态，选举之后会重新初始化
	nextIndex		[]int // 对于每一台服务器，发送到该服务器的下一日志条目的索引（初始化为领导者最后的日志条目+1）
	matchIndex		[]int // 对于每一台服务器已知复制到该服务器最高日志条目的索引（初始化为0，单调递增）
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == STATE_LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // current term
	VoteGranted bool // true means receive vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// 如果请求的任期号更大，更新任期号，转为follower
	if args.Term > int(rf.currentTerm){
		log.Printf("s[%v] update Term [%v]->[%v] by s[%v] when Request Vote", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.updateState(STATE_FOLLOWER)
		rf.votedFor = -1
		reply.VoteGranted = false
		rf.heartBeatTimeOut = time.Now().Add(randTime())
	}

	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor==args.CandidateId){
		// 2B check log: 只有在candidate的log和receiver的log一样新才会投票
		// 最开始没有log
		if args.LastLogIndex == 0 && len(rf.log) == 0{
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.heartBeatTimeOut = time.Now().Add(randTime())
		}else if args.LastLogTerm > rf.log[len(rf.log)-1].CommandTerm{
			// 任期号更小
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.heartBeatTimeOut = time.Now().Add(randTime())			
		}else if args.LastLogTerm == rf.log[len(rf.log)-1].CommandTerm && len(rf.log) <= args.LastLogIndex{
			// 任期号相等但长度<=arg
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.heartBeatTimeOut = time.Now().Add(randTime())			
		}

	}else{
		reply.VoteGranted = false
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	log.Printf("s[%v] be asked vote to [%v] reply:%v args:%v\n", rf.me, args.CandidateId, reply, args)
	rf.mu.Unlock()
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果请求的term更小
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
	}else{
		rf.currentTerm = args.Term
		rf.updateState(STATE_FOLLOWER)
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		reply.Success = true
		// 2B: 日志的处理
		if args.Entries!=nil { log.Printf("S[%v] handle log, prev index: %v", rf.me, args.PrevLogIndex) }
	
		if args.PrevLogIndex == 0 {
			// 前面没有日志
			if args.Entries != nil{
				// 追加未保存的条目
				for i:=0; i<len(args.Entries); i++{
					rf.log = append(rf.log, args.Entries[i])
				}
				log.Printf("S[%v] log len: %v, args.Entries: %v", rf.me, len(rf.log), len(args.Entries))
				reply.Success = true		
			}	
		}else{
			if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].CommandTerm != args.PrevLogTerm{
				reply.Success = false 
			}else{
				if args.Entries!=nil && len(args.Entries)!=0 {
					i := 0
					// 如果一个已经存在的条目和新条目冲突，清空这个条目及之后内容
					for ; i < len(args.Entries); i++ {
						ind := args.Entries[i].CommandIndex
						// rf.log中不存在这些记录
						if ind > len(rf.log) { break }
						// 判断term
						if args.Entries[i].CommandTerm != rf.log[ind-1].CommandTerm{
							rf.log = rf.log[: ind-1]
							log.Printf("S[%v] clear previous log, after log len: %v", rf.me, len(rf.log))
							break
						}
					}
					if i==len(args.Entries){ i=0 }
					// 追加未保存的条目
					log.Printf("S[%v] append log: [%v]->[%v]", rf.me, i, len(args.Entries))
					for ; i<len(args.Entries); i++{
						rf.log = append(rf.log, args.Entries[i])
					}
					reply.Success = true
				}
			}
		}
		// 检查LeaderCommit，提交日志，重设commitIndex
		if reply.Success == true {
			log_len := len(rf.log)
			new_commit := log_len 
			if args.LeaderCommit < new_commit{
				new_commit = args.LeaderCommit
			}
			if rf.commitIndex < new_commit{
				rf.doCommit(rf.commitIndex+1, new_commit)
				rf.commitIndex = new_commit
			}
		}
		//2B: end
	}
	rf.heartBeatTimeOut = time.Now().Add(randTime())
	//log.Printf("[%v]->[%v]:s[%v] trans to Follower by L[%v] when receive AppendEntries\n", time.Now(), rf.heartBeatTimeOut, rf.me, args.LeaderId)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.debugLevel==1 {
		log.Printf("sendRequestVote: [%v]->[%v]", rf.me, server )
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if rf.debugLevel==1 {
		log.Printf("finish sendRequestVote: [%v]->[%v]", rf.me, server )
	}
	return ok
}



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != STATE_LEADER{
		isLeader = false
	}else{
		log.Printf("L[%v] Leader get a Start request, commitIndex:[%v], commandIndex:[%v], term:[%v]\n", rf.me, rf.commitIndex + 1, len(rf.log) + 1, rf.currentTerm)
		entry := ApplyMsg{CommandValid: false, Command: command, CommandTerm: rf.currentTerm}
		entry.CommandIndex = len(rf.log) + 1
		rf.log = append(rf.log, entry)
		term = rf.currentTerm
		index = len(rf.log)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.isHeartBeatTimeOut() && rf.state==STATE_FOLLOWER{
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(TickerSleepTime)
	}
}

func (rf *Raft) isHeartBeatTimeOut() bool {
	return rf.heartBeatTimeOut.Before(time.Now())
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
	rf.heartBeatTimeOut = time.Now().Add(randTime())
	rf.debugLevel = 0

	rf.log = make([]ApplyMsg, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) startElection() {
	log.Printf("s[%v] start a election now\n", rf.me)
	rf.mu.Lock()
	// 为自己投票，这时候状态是candidate
	rf.votedFor = rf.me
	rf.cntVoted = 1
	rf.currentTerm += 1
	rf.state = STATE_CANDIDATE
	// 设置选举超时时间
	electionTimeOut := time.Now().Add(randTime())
	rf.mu.Unlock()
	// 开始收集投票
	go rf.collectVotes()
	//检查投票结果
	for rf.killed() == false{
		voteCount := rf.getGrantedVotes()
		if rf.debugLevel==1 {
			log.Printf("s[%v] check vote [%v]\n", rf.me, voteCount)
		}
		rf.mu.Lock()
		if voteCount > len(rf.peers)/2{
			// 成为leader
			rf.updateState(STATE_LEADER)
			// 初始化领导者上的易失性状态
			if rf.debugLevel==1 { log.Printf("L[%v] initialize volatile status, nextIndex & matchIndex", rf.me) }
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i:=0; i<len(rf.peers); i++{
				if len(rf.log) == 0{
					rf.nextIndex[i] = 1
					// log.Printf("....")
					// log.Printf("[%v] nextIndex: [%v]\n", i, rf.nextIndex[i])
				}else{
					rf.nextIndex[i] = rf.log[len(rf.log)-1].CommandIndex + 1 // 领导者最后日志条目+1
					// log.Printf("....")
				}
			}
			log.Printf("L[%v] is a Leader now, term[%v], log len[%v]\n", rf.me, rf.currentTerm, len(rf.log))
			// 发送心跳包
			go rf.sendHeartBeats()
			// 检查提交的log
			go rf.checkCommitedLogs()
			rf.mu.Unlock()
			return

		}else if rf.state == STATE_FOLLOWER{
			// 成为folloer
			log.Printf("F[%v] another server is Leader now\n", rf.me)
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}else if electionTimeOut.Before(time.Now()){
			// 超时重新选举
			log.Printf("C[%v] election time out\n", rf.me)
			rf.votedFor = -1
			rf.updateState(STATE_FOLLOWER)
			rf.mu.Unlock()
			return 
		}
		rf.mu.Unlock()
		time.Sleep(ElectionSleepTime)
	}
}

// 检查提交的log
func (rf *Raft) checkCommitedLogs(){
	for rf.killed() == false{
		if rf.debugLevel==1 { log.Printf("L[%v] check commited logs..., commit index: %v", rf.me, rf.commitIndex) }
		rf.mu.Lock()
		if rf.state == STATE_LEADER{ // check to avoid state change
			ind := len(rf.log)
			cnt := 1
			for i := 0; i < len(rf.peers); i++{
				// log.Printf("L[%v] %v:%v, log length:%v", rf.me, i, rf.matchIndex[i], ind)
				if i!=rf.me && rf.matchIndex[i] == ind{
					cnt ++
				}
			}

			if cnt > len(rf.peers)/2{
				// 进行commit操作
				if(rf.commitIndex < ind){
					rf.doCommit(rf.commitIndex+1, ind)
					rf.commitIndex = ind
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(CheckCommitSleepTime)
	}

}

// 进行提交
func (rf *Raft) doCommit(from_ind int, to_ind int){
	log.Printf("S[%v] commit logs [%v]->[%v]", rf.me, from_ind, to_ind)
	for i:=from_ind; i<=to_ind; i++{
		rf.log[i-1].CommandValid = true 
		rf.applyCh <- rf.log[i-1]
	}
}

// 并行向所有peer收集选票
func (rf *Raft) collectVotes() {
	if rf.debugLevel==1 {
		log.Printf("C[%v] start to collect votes\n", rf.me)
	}
	wg := sync.WaitGroup{}
	for i := 0; i<len(rf.peers); i++{
		if i == rf.me{
			continue
		}
		wg.Add(1)
		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) }
		if len(rf.log)>0 {
			args.LastLogTerm = rf.log[len(rf.log)-1].CommandTerm
		}else{
			args.LastLogTerm = 0
		}
		go func(server int, args RequestVoteArgs){
			if rf.debugLevel==1 {
				log.Printf("C[%v] start to sendRequest vote ->[%v]\n", rf.me, server)	
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply){
				// log.Printf("C[%v] request vote ok [%v]\n", rf.me, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()	
				if reply.VoteGranted{
					rf.cntVoted += 1
				}else{
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.updateState(STATE_FOLLOWER)
					}
				}
			}else{
				log.Printf("Send request vote from %d to %d error", rf.me, server)
			}
			wg.Done()
		}(i, args)		
	}
	wg.Wait()
	log.Printf("C[%v] end collect votes\n", rf.me)
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []ApplyMsg // 需要被保存的日志条目
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 发送心跳包
func (rf *Raft)sendHeartBeats(){
	// 单个心跳包函数
	sendHeartBeat := func(server int, args *AppendEntriesArgs) bool{
		reply := AppendEntriesReply{}
		if rf.debugLevel==1 { log.Printf("go func: sendHeartBeat:[%v]->[%v]", rf.me, server) }
		if rf.sendAppendEntries(server, args, &reply){
			if rf.debugLevel==1 {
				log.Printf("s[%v] sendHeartBeat to [%v] reply:%v args:%v\n", rf.me, server, reply, args)
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 检查收到的任期号
			if reply.Term > rf.currentTerm{
				rf.updateState(STATE_FOLLOWER)
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				log.Printf("F[%v] from leader to follower because larger term received", rf.me)
			} else{
				// 2B: 处理日志
				if reply.Success == false && args.Term == reply.Term{
					// 因为日志不一致失败，减少nextIndex
					rf.nextIndex[server] -= 1
					log.Printf("F[%v] decrease nextIndex because of failure", server)
				}else{
					// 成功接受，更新matchIndex和nextIndex
					if args.Entries != nil{
						rf.matchIndex[server] = args.Entries[len(args.Entries)-1].CommandIndex 
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						log.Printf("F[%v] accept log[%v]", server, rf.matchIndex[server])
					}
				}
			}
		}else{
			// log.Printf("F[%v] Send request heartbeat fail", rf.me)
			return false
		}
		return reply.Success
	}
	
		// 发送心跳包，直到不为leader
		for rf.killed() == false{
			// log.Printf("F[%v] is alive!\n", rf.me)
			rf.mu.Lock()
			if rf.state != STATE_LEADER{
				log.Printf("F[%v] is not a leader now!\n", rf.me)
				rf.mu.Unlock()
				rf.votedFor = -1
				return 
			}else {
				// 遍历peer
				for i := 0; i<len(rf.peers); i++{
					if i != rf.me{
						heartBeatArgs := AppendEntriesArgs{Term:rf.currentTerm, LeaderId: rf.me, Entries: nil, LeaderCommit: rf.commitIndex}
						// 更新args中的log相关的信息
						heartBeatArgs.PrevLogIndex = rf.nextIndex[i] - 1
						if rf.nextIndex[i]!=1 { 
							heartBeatArgs.PrevLogTerm = rf.log[heartBeatArgs.PrevLogIndex-1].CommandTerm 
						}else {
							heartBeatArgs.PrevLogTerm = 0
						}
						if len(rf.log)>0 && rf.log[len(rf.log)-1].CommandIndex >= rf.nextIndex[i]{
							// 发送从nextIndex开始的所有条目
							// log.Printf("L[%v]: C[%v] next index is: [%v]", rf.me, i, rf.nextIndex[i])
							for j:=rf.nextIndex[i]; j<=len(rf.log); j++{
								heartBeatArgs.Entries = append(heartBeatArgs.Entries, rf.log[j-1])
							}
						}
						// log.Printf("send heartbeat [%v]->[%v]", rf.me, i)
						// 除了自己外发送心跳
						go sendHeartBeat(i, &heartBeatArgs)
					}
				}
			}
			rf.mu.Unlock()
			time.Sleep(HeartBeatSendTime)
		}
}

func (rf *Raft) getGrantedVotes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.cntVoted
}


func (rf *Raft) updateState(state int){
	// need lock 
	if rf.state == state {
		return 
	}
	old_state := rf.state
	switch state{
	case STATE_FOLLOWER:
		rf.state = STATE_FOLLOWER
	case STATE_CANDIDATE:
		rf.state = STATE_CANDIDATE
	case STATE_LEADER:
		rf.state = STATE_LEADER
	default:
		log.Fatalf("Unknown state %d", state)
	}
	log.Printf("In term %d machine %d updating state from %d to %d", rf.currentTerm, rf.me, old_state, state)
}

func randTime() time.Duration{
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := time.Millisecond * time.Duration((r.Intn(150)+200))
	// log.Printf("RAND time: %s", t)
	return t
}

