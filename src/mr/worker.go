package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		time.Sleep(1 * time.Second)

		reply := TaskReply{}
		ok := call("Coordinator.GetTask", struct{}{}, &reply)
		if ok {
			if reply.Done {
				break
			}
			TaskType := reply.Task.Type
			switch TaskType {
			case MapTask:
				DoMap(mapf, reply.Task, reply.NReduce)
			case ReduceTask:
				DoReduce(reducef, reply.Task)
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMap(mapf func(string, string) []KeyValue, task Task, nReduce int) bool {

	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	kva := mapf(filename, string(content))

	mappedFile := make(map[int][]KeyValue)
	for _, kv := range kva {
		key := ihash(kv.Key) % nReduce
		mappedFile[key] = append(mappedFile[key], KeyValue{kv.Key, kv.Value})
	}

	var reduceFiles []string
	for hash, kvs := range mappedFile {
		name := "mr-mepped-" + strconv.Itoa(hash)
		ofile, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
		if err != nil {
			log.Fatalf("cannot create %v", name)
		}
		defer ofile.Close()
		for _, kv := range kvs {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		reduceFiles = append(reduceFiles, name)
	}
	go func() {
		MapCallback(filename, reduceFiles)
	}()
	return false
}

func MapCallback(mapFile string, reduceFiles []string) {
	args := MapCallbackArgs{mapFile, reduceFiles}
	reply := MapCallbackReply{}

	ok := call("Coordinator.MapTaskSuccess", &args, &reply)
	if !ok {

	}
}

func DoReduce(reducef func(string, []string) string, task Task) bool {

	reduceFileName := task.Filename
	file, err := os.Open(reduceFileName)
	if err != nil {
		log.Fatalf("cannot open %v", reduceFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reduceFileName)
	}

	lines := strings.Split(string(content), "\n")
	intermediate := []KeyValue{}
	for _, line := range lines {
		kv := strings.Split(line, " ")
		if len(kv) == 2 {
			intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
		}
	}

	oname := "mr-out-" + strings.Split(reduceFileName, "-")[2]

	ofile, _ := os.Create(oname)
	defer ofile.Close()

	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	go func() {
		ReduceCallback(reduceFileName)
	}()
	return true
}

func ReduceCallback(reduceFile string) {
	args := ReduceCallbackArgs{reduceFile}
	reply := ReduceCallbackReply{}

	ok := call("Coordinator.ReduceTaskSuccess", &args, &reply)
	if !ok {

	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
