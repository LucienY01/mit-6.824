package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var noneArgs = 0
var noneReply = 0

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := Reply{}
		ok := call("Coordinator.Task", &noneArgs, &reply)
		if ok {
			if reply.Act == actWait {
				log.Print("worker: wait for other task to be done.")
				time.Sleep(5 * time.Second)
				continue
			}
			if reply.Act == actExit {
				log.Print("worker: normally exit.")
				return
			}

			switch reply.MR {

			case workMap:
				executeMap(&reply, mapf)
				continue

			case workReduce:
				executeReduce(&reply, reducef)
				continue
			}
		} else {
			log.Print("rpc call failed. consider it as all jobs done, so exit.")
			return
		}
	}
}

func executeMap(reply *Reply, mapf func(string, string) []KeyValue) {
	inputFile, err := os.Open(reply.Filenames[0])
	if err != nil {
		log.Fatal(err)
	}
	bytes, err := io.ReadAll(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	inputFile.Close()
	content := string(bytes)

	intermediate := mapf(reply.Filenames[0], content)
	outputFilename := make([]string, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		outputFilename[i] = fmt.Sprintf("mr-%v-%v", reply.IMap, i)
	}

	outputFile := make([]*os.File, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		outputFile[i], err = os.CreateTemp(".", "temp-"+outputFilename[i]+"-*")
		if err != nil {
			log.Fatalf("store intermediate output: %v", err)
		}
	}
	enc := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		enc[i] = json.NewEncoder(outputFile[i])
	}
	for _, kv := range intermediate {
		iReduce := ihash(kv.Key) % reply.NReduce
		err = enc[iReduce].Encode(&kv)
		if err != nil {
			log.Fatalf("store intermediate output: %v", err)
		}
	}
	tempName := make([]string, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		tempName[i] = outputFile[i].Name()
		outputFile[i].Close()
		err = os.Rename(tempName[i], outputFilename[i])
		if err != nil {
			log.Fatalf("store intermediate output: %v", err)
		}
	}

	args := ArgsMapDone{
		NumberMap: reply.IMap,
		Filenames: outputFilename,
	}
	call("Coordinator.MapDone", &args, &noneReply)
}

func executeReduce(reply *Reply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, filename := range reply.Filenames {
		inputFile, err := os.Open(filename)
		if err != nil {
			log.Fatalln("read input file:", err)
		}

		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				log.Printf("worker decode kvs from input file %v: %v", filename, err)
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-X.
	outName := fmt.Sprintf("mr-out-%v", reply.IReduce)
	tempFile, err := os.CreateTemp(".", "temp-"+outName+"-*")
	if err != nil {
		log.Fatalln("store reduce output:", err)
	}
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tempName := tempFile.Name()
	tempFile.Close()
	if err := os.Rename(tempName, outName); err != nil {
		log.Fatalln("store reduce output:", err)
	}

	args := ArgsReduceDone{
		I: reply.IReduce,
	}
	call("Coordinator.ReduceDone", &args, &noneReply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
