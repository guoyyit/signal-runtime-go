package signalruntime

import (
	"runtime"
	"sync"
)

func runtimeYield() { runtime.Gosched() }

func newTestMu() *sync.Mutex { return &sync.Mutex{} }
