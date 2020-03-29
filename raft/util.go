package raft

import "log"

// Debug mode
const Debug = 1

// DPrintf defines debug message helper
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
