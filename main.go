package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"
)

// constants and types
type TaskType int
type TaskState int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	StateIdle TaskState = iota
	StateInProgress
	StateCompleted
)

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

type Master struct {
	mu           sync.Mutex
	inputs       []string    // the data to process
	mapStatus    []TaskState // mark which map taks are done
	reduceStatus []TaskState // mark which reduce tasks are done

	intermediate map[int][]KeyValue // temporary storage
	outputs      map[int]string     // final results
	nReduce      int
}

func NewMaster(inputs []string, nReduce int) *Master {
	return &Master{
		inputs:       inputs,
		mapStatus:    make([]TaskState, len(inputs)),
		reduceStatus: make([]TaskState, nReduce),
		intermediate: make(map[int][]KeyValue),
		outputs:      make(map[int]string),
		nReduce:      nReduce,
	}
}

type Task struct {
	Type     TaskType // map task, reduce task or no task
	TaskID   int
	ReduceID int
	Data     string // actual data to process
}

func (m *Master) RequestTask() Task {
	m.mu.Lock()         // lock the door
	defer m.mu.Unlock() // unlock when done

	// now only one worker can be here at a time

	// check for map tasks
	allMapsDone := true
	for i, status := range m.mapStatus {
		if status == StateIdle {
			m.mapStatus[i] = StateInProgress
			return Task{Type: MapTask, TaskID: i, Data: m.inputs[i]}
		}
		if status != StateCompleted {
			allMapsDone = false
		}
	}

	if !allMapsDone {
		// map phase not finished, but no idle tasks. workers must wait.
		return Task{Type: NoTask}
	}

	// check for reduce tasks
	for i, status := range m.reduceStatus {
		if status == StateIdle {
			m.reduceStatus[i] = StateInProgress
			return Task{Type: ReduceTask, TaskID: i, ReduceID: i}
		}
	}

	// if everything is done, tell worker to exit
	return Task{Type: ExitTask}
}

// receives intermediate data from a worker
func (m *Master) ReportMapSuccess(taskID int, partitions map[int][]KeyValue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mapStatus[taskID] = StateCompleted

	// store intermediate data grouped by reduce partition
	for rID, kvs := range partitions {
		m.intermediate[rID] = append(m.intermediate[rID], kvs...)
	}
}

// receives a final result from a worker
func (m *Master) ReportReduceSuccess(reduceID int, output string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.reduceStatus[reduceID] = StateCompleted
	m.outputs[reduceID] = output
}

// helper to get data for a specified reducer
func (m *Master) GetReduceData(reduceID int) []KeyValue {
	m.mu.Lock()
	defer m.mu.Unlock()

	// return a copy or slice of the data for this partition
	return m.intermediate[reduceID]
}

// Worker
type Worker struct {
	ID       int
	Master   *Master
	mapFn    MapFunc
	reduceFn ReduceFunc
}

func (w *Worker) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		// ask master for work
		task := w.Master.RequestTask()

		switch task.Type {
		case MapTask:
			w.doMap(task)
		case ReduceTask:
			w.doReduce(task)
		case NoTask:
			// wait for a while before asking again
			time.Sleep(100 * time.Millisecond)
		case ExitTask:
			fmt.Printf("Worker %d: Exiting.\n", w.ID)
			return
		}
	}
}

func (w *Worker) doMap(task Task) {
	// run user Map function
	kvs := w.mapFn("doc", task.Data)

	// partition the data
	partitions := make(map[int][]KeyValue)
	for _, kv := range kvs {
		// hash the key to find which reducer it belongs to
		reducerNum := ihash(kv.Key) % w.Master.nReduce
		partitions[reducerNum] = append(partitions[reducerNum], kv)
	}

	// send back to Master
	w.Master.ReportMapSuccess(task.TaskID, partitions)
}

func (w *Worker) doReduce(task Task) {
	// get data from Master for this partition
	rawKVs := w.Master.GetReduceData(task.ReduceID)

	// group values by Key
	groups := make(map[string][]string)
	for _, kv := range rawKVs {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}

	// sort keys for deterministic output
	var keys []string
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// run Reduce function
	var resultBuilder strings.Builder
	for _, k := range keys {
		output := w.reduceFn(k, groups[k])
		resultBuilder.WriteString(fmt.Sprintf("%s %s\n", k, output))
	}

	// report success
	w.Master.ReportReduceSuccess(task.ReduceID, resultBuilder.String())
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func mapF(docID string, contents string) []KeyValue {
	words := strings.Fields(contents)
	var kvs []KeyValue
	for _, word := range words {
		kvs = append(kvs, KeyValue{
			Key:   strings.ToLower(word),
			Value: "1",
		})
	}
	return kvs
}

func reduceF(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

func main() {
	inputs := []string{
		"foo bar baz",
		"foo foo qux",
		"baz qux bar",
	}

	// initialize Master with 3 Reducers
	master := NewMaster(inputs, 3)

	var wg sync.WaitGroup

	// Start 4 Workers
	fmt.Println("Starting MapReduce...")
	for i := 0; i < 4; i++ {
		wg.Add(1)
		w := Worker{ID: i, Master: master, mapFn: mapF, reduceFn: reduceF}
		go w.Run(&wg)
	}

	// wait for all workers to exit
	wg.Wait()

	fmt.Println("--- Final Output ---")
	
	for i := 0; i < 3; i++ {
		if out, ok := master.outputs[i]; ok {
			fmt.Printf("Reducer %d output:\n%s", i, out)
		}
	}
}