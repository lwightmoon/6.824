package mr

import (
	"testing"
)

func TestRpc(t *testing.T) {
	// m := &Master{}
	// m.mReduces = []*taskState{&taskState{running: true, ok: true}}
	// m.mTasks = []*taskState{&taskState{running: true, ok: true}}
	// m.lock = &sync.Mutex{}
	// m.monitorTask()
	// t.Logf("mapok:%v", m.mapOk)
	ret := CallGetTasks(reducePhase)
	t.Logf("file szie:%d", ret.Nfiles)
}
