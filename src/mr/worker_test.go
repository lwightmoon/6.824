package mr

import (
	"encoding/json"
	"testing"
)

func TestRpc(t *testing.T) {
	ret := CallGetTasks(mapPhase)
	b, _ := json.Marshal(ret)
	t.Logf("tasks:%s", b)
}
