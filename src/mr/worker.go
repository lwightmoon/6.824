package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	ticker := time.NewTicker(time.Second)
	log.Println("work run")
	var mapOk bool
	for {
		<-ticker.C
		if !mapOk {
			log.Println("start call")
			ok := CallPhaseOk(mapPhase)
			log.Printf("work run ret:%v---\n", ok)
			mapOk = ok
		}
		var task *TaskReply
		if mapOk {
			task = CallGetTasks(reducePhase)
			ri := task.Index
			kva := []KeyValue{}
			for i := 0; i < task.Nfiles; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, ri)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("open file:%s err:%v", filename, err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			oname := fmt.Sprintf("mr-out-%d", task.Index)
			ofile, _ := os.Create(oname)
			sort.Sort(ByKey(kva))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()

			CallFinishTask(reducePhase, task.Index)
		} else {
			log.Println("get tasks begin")
			task = CallGetTasks(mapPhase)
			log.Printf("get tasks:%v\n", task)
			filename := task.File
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			// sort.Sort(ByKey(kva))
			for _, kv := range kva {
				fi := ihash(kv.Key) % task.NReduce
				outfname := fmt.Sprintf("mr-%d-%d", task.Index, fi)
				outfile, _ := os.OpenFile(outfname, os.O_CREATE|os.O_WRONLY, os.ModePerm)
				enc := json.NewEncoder(outfile)
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("json encode err:%v", err)
				}
				outfile.Close()
			}
			CallFinishTask(mapPhase, task.Index)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetTasks(phase string) *TaskReply {
	args := TaskArgs{
		Phase: phase,
	}
	reply := TaskReply{}
	suc := call("Master.GetTasks", &args, &reply)
	log.Printf("call api ret:%v", suc)
	return &reply
}

func CallFinishTask(phase string, index int) {
	args := &FinishArgs{
		Index: index,
		Phase: phase,
	}
	reply := &FinishReply{}
	call("Master.FinishTask", args, reply)
}

func CallPhaseOk(phase string) bool {
	args := &OkArgs{
		Phase: phase,
	}
	rep := &OkReply{}
	call("Master.Ok", args, rep)
	return rep.Ok
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
