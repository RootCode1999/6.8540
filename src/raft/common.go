package raft

type ServerState int8

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type Entry struct {
	Term int
	Cmd  interface{}
}
