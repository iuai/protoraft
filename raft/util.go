package raft

import (
	"github.com/jmcvetta/randutil"
	"log"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func inBetween(i, min, max int) bool {
	if i >= min && i <= max {
		return true
	} else {
		return false
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func getMajority(nums []int) int {
	m := make(map[int]int)
	for i := 0; i < len(nums); i++ {
		num := nums[i]
		m[num] = m[num] + 1
		if m[num] > len(nums)/2 {
			return num
		}
	}

	return -1
}

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func randomElectionTimeout(min, max int) time.Duration {
	r, _ := randutil.IntRange(min, max)
	return time.Duration(r)
}
