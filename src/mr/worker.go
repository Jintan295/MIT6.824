package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

func Worker(mapf MapFunc, reducef ReduceFunc) {

	// Your worker implementation here.
	log.SetOutput(ioutil.Discard)
	for {
		time.Sleep(1 * time.Second)
		task := Task{}
		//  请求一个任务
		call("Master.RequestTask", &Placeholder{}, &task)
		// 说明当前无任务
		if task.Operation == ToWait {
			continue
		}
		if task.IsMap {
			log.Printf("received map task %s", task.Map.Filename)
			err := handleMap(task, mapf)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		} else {
			log.Printf("received reduce task %d %v", task.Reduce.Id, task.Reduce.IntermediateFilenames)
			err := handleReduce(task, reducef)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		}
	}
}

// 处理Map请求
func handleMap(task Task, mapf MapFunc) error {
	filename := task.Map.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	KV := mapf(filename, string(content))
	var encoders []*json.Encoder
	for i := 0; i < task.NReduce; i++ {
		// 为每个Reduce worker都copy一份
		f, err := os.Create(fmt.Sprintf("mr-%d-%d", task.Map.Id, i))
		if err != nil {
			log.Fatalf("cannot create intermediate result file")
		}
		encoders = append(encoders, json.NewEncoder(f))
	}
	for _, kv := range KV {
		_ = encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
	}
	call("Master.Finish", &FinishArgs{IsMap: true, Id: task.Map.Id}, &Placeholder{})
	return nil
}

// 处理Reduce请求
func handleReduce(task Task, reducef ReduceFunc) error {
	var KV []KeyValue
	for _, filename := range task.Reduce.IntermediateFilenames {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decode := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decode.Decode(&kv); err != nil {
				break
			}
			KV = append(KV, kv)
		}
		err = f.Close()
		if err != nil {
			log.Fatalf("cannot close %v", filename)
		}
	}
	sort.Sort(ByKey(KV))
	outName := fmt.Sprintf("mr-out-%d", task.Reduce.Id)
	temp, err := os.CreateTemp(".", outName)
	if err != nil {
		log.Fatalf("cannot create reduce result tempfile %s", outName)
		return err
	}
	i := 0
	for i < len(KV) {
		j := i + 1
		for j < len(KV) && KV[j].Key == KV[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, KV[k].Value)
		}
		output := reducef(KV[i].Key, values)
		_, _ = fmt.Fprintf(temp, "%v %v\n", KV[i].Key, output)
		i = j
	}
	err = os.Rename(temp.Name(), outName)
	if err != nil {
		return err
	}
	call("Master.Finish", &FinishArgs{IsMap: false, Id: task.Reduce.Id}, &Placeholder{})
	return nil
}

// ByKey 定义按健排序
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
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
