package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

var noOfThreads int = 1000
var noOfRequestsPerThread int = 50
var commands []string

func init() {
	go main()
}

/*
TestConcurrentSet() checks concurrent set() operations by multiple clients and PASSes the test if the last version number matches the expected version number
*/

func TestConcurrentSet(t *testing.T) {

	done := make(chan bool)

	//spwan client threads
	i := 0
	for i < noOfThreads {
		go check_concurrent_set(t, done)
		i += 1
	}
	i = 0

	for i < noOfThreads {
		<-done
		i += 1
	}

	go check_concurrent_set_PASS(t, done)
	<-done

}

/*
check_concurrent_set_PASS() works for TestConcurrentSet() to check if value version matches expected number
*/
func check_concurrent_set_PASS(t *testing.T, done chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- true
		return
	}
	reader := bufio.NewReader(conn)
	io.Copy(conn, bytes.NewBufferString("set key1 10 5\r\nmayur\r\n"))
	data, err := reader.ReadBytes('\n')
	input := strings.TrimRight(string(data), "\r\n")

	response := strings.Split(input, " ")
	result, _ := strconv.Atoi(response[1])
	expected_res := noOfThreads * noOfRequestsPerThread
	if result != expected_res {
		t.Fail()
	}

	done <- true

	conn.Close()

}

/*
check_concurrent_set() spawns multiple client threads for TestConcurrentSet(), which will then call set() command on server

*/
func check_concurrent_set(t *testing.T, done chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- true
		return
	}
	j := 0
	reader := bufio.NewReader(conn)

	for j < noOfRequestsPerThread {
		io.Copy(conn, bytes.NewBufferString("set key1 10 5\r\nmayur\r\n"))

		data, err := reader.ReadBytes('\n')
		input := strings.TrimRight(string(data), "\r\n")
		array := strings.Split(input, " ")
		result := array[0]
		if result == "VALUE" {
			data, err = reader.ReadBytes('\n')
			input = strings.TrimRight(string(data), "\r\n")
			if err != nil {
				break
			}
		}
		j += 1
	}
	if err != nil {
		t.Error(err)
	}
	done <- true

	conn.Close()
}

/*
TestBasicFunctionality() tests for basic functionalities set,cas,delete,get,getm on single client
*/
func TestBasicFunctionality(t *testing.T) {

	done := make(chan bool)
	go check_basic_functionality(t, done)
	<-done

}

/*
check_basic_functionality() actually performs basic operation for TestBasicFunctionality
*/

func check_basic_functionality(t *testing.T, done chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- true
		return
	}
	reader := bufio.NewReader(conn)

	/*
		Checking set() functionality
	*/
	io.Copy(conn, bytes.NewBufferString("set key 10 5\r\nmayur\r\n"))

	data, err := reader.ReadBytes('\n')
	input := strings.TrimRight(string(data), "\r\n")
	array := strings.Split(input, " ")
	result := array[0]
	temp, _ := strconv.Atoi(array[1])

	if result != "OK" || temp != 0 {
		t.Fail()
	}

	/*
		Checking get() functoinality
	*/
	io.Copy(conn, bytes.NewBufferString("get key\r\n"))
	data, err = reader.ReadBytes('\n')
	input = strings.TrimRight(string(data), "\r\n")
	array = strings.Split(input, " ")
	result = array[0]
	if result == "VALUE" {
		data, err = reader.ReadBytes('\n')
		input = strings.TrimRight(string(data), "\r\n")
		array1 := strings.Split(input, " ")

		temp, _ = strconv.Atoi(array[1])
		if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
		}

	} else {
		t.Fail()
	}

	/*
		Checking getm() functionality
	*/

	io.Copy(conn, bytes.NewBufferString("getm key\r\n"))
	data, err = reader.ReadBytes('\n')
	input = strings.TrimRight(string(data), "\r\n")
	array = strings.Split(input, " ")
	result = array[0]
	if result == "VALUE" {
		data, err = reader.ReadBytes('\n')
		input = strings.TrimRight(string(data), "\r\n")
		array1 := strings.Split(input, " ")

		temp, _ = strconv.Atoi(array[3])
		if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
		}

	} else {
		t.Fail()
	}

	/*
		Checking cas() functionality
	*/

	io.Copy(conn, bytes.NewBufferString("getm key\r\n"))
	data, err = reader.ReadBytes('\n')
	input = strings.TrimRight(string(data), "\r\n")
	array = strings.Split(input, " ")
	result = array[0]
	if result == "VALUE" {
		data, err = reader.ReadBytes('\n')
		input = strings.TrimRight(string(data), "\r\n")
		array1 := strings.Split(input, " ")

		temp, _ = strconv.Atoi(array[3])
		if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
		}

	} else {
		t.Fail()
	}

	/*
		Checking delete() functionality
	*/

	io.Copy(conn, bytes.NewBufferString("delete key\r\n"))
	data, err = reader.ReadBytes('\n')
	input = strings.TrimRight(string(data), "\r\n")
	array = strings.Split(input, " ")
	result = array[0]
	if result != "DELETED" {
		t.Fail()
	}

	done <- true

	conn.Close()

}

/*
TestBasicErrorCommands() tests for basic functionalities set,cas,delete,get,getm on single client
*/
func TestBasicErrorCommands(t *testing.T) {

	done := make(chan bool)
	go checkBasicErrorCommands(t, done)
	<-done

}

/*
check_basic_functionality() actually performs basic operation for TestBasicFunctionality
*/

func checkBasicErrorCommands(t *testing.T, done chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- true
		return
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	reader := bufio.NewReader(conn)

	/*
		Checking if there is any error message with 'noreply' flag and wrong command text

	*/
	io.Copy(conn, bytes.NewBufferString("set key abc 5 noreply\r\nmayur\r\n"))

	_, error := reader.ReadBytes('\n')

	if error == nil {
		t.Fail()

	}

	/*
		Checking error messsage when wrong command is sent
	*/

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	io.Copy(conn, bytes.NewBufferString("set key abc 5\r\nmayur\r\n"))

	data, err := reader.ReadBytes('\n')
	input := strings.TrimRight(string(data), "\r\n")
	if input != "ERRCMDERR" {
		t.Fail()
	}
	io.Copy(conn, bytes.NewBufferString("set key 100 5\r\nmayur\r\n"))

	data, err = reader.ReadBytes('\n')
	input = strings.TrimRight(string(data), "\r\n")
	array := strings.Split(input, " ")
	result := array[0]
	temp, _ := strconv.Atoi(array[1])

	if result != "OK" || temp != 0 {
		t.Fail()
	}

	//t.Log(result,temp)

	/*
		Test version mismatch error message in case of cas functionality
	*/

	io.Copy(conn, bytes.NewBufferString("cas key 2672 1 5\r\nmayur\r\n"))

	data, err = reader.ReadBytes('\n')

	input = strings.TrimRight(string(data), "\r\n")
	//t.Log(input)

	if input != "ERR_VERSION" {
		t.Fail()
	}

	/*
		Test for wrong command sent to server
	*/

	io.Copy(conn, bytes.NewBufferString("cal key 2672 1 5\r\nmayur\r\n"))

	data, err = reader.ReadBytes('\n')

	input = strings.TrimRight(string(data), "\r\n")
	//t.Log(input)

	if input != "ERRCMDERR" {
		t.Fail()
	}

	done <- true

	conn.Close()

}

func TestExpiryOfKeys(t *testing.T) {
	done := make(chan bool)
	go checkTestExpiryOfKeys(t, done)
	<-done

}

func checkTestExpiryOfKeys(t *testing.T, done chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- true
		return
	}
	reader := bufio.NewReader(conn)

	/*
		Setting a key with expiry time 4 seconds
	*/
	io.Copy(conn, bytes.NewBufferString("set key5 4 5\r\nmayur\r\n"))

	data, err := reader.ReadBytes('\n')
	input := strings.TrimRight(string(data), "\r\n")
	array := strings.Split(input, " ")
	result := array[0]
	temp, _ := strconv.Atoi(array[1])

	if result != "OK" || temp != 0 {
		t.Fail()
	}

	/*
		Checking if key exists before expiry
	*/
	io.Copy(conn, bytes.NewBufferString("get key5\r\n"))
	data, err = reader.ReadBytes('\n')
	input = strings.TrimRight(string(data), "\r\n")
	array = strings.Split(input, " ")
	result = array[0]

	if result == "VALUE" {
		data, err = reader.ReadBytes('\n')
		input = strings.TrimRight(string(data), "\r\n")
		array1 := strings.Split(input, " ")

		temp, _ = strconv.Atoi(array[1])

		if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
		}

	} else {
		t.Fail()
	}

	/* Sleeping for 5 seconds and then try to access to the key value which is supposed to be expired*/

	timer := time.NewTimer(time.Duration(5) * time.Second)
	go func() {
		<-timer.C
		io.Copy(conn, bytes.NewBufferString("get key5\r\n"))

		data, err = reader.ReadBytes('\n')
		input = strings.TrimRight(string(data), "\r\n")
		if input != "ERRNOTFOUND" {
			t.Fail()
		}

	}()

	done <- true

	conn.Close()

}


/*
TestConcurrentMulti() tests ability of system to sustain multiple commands from multiple clients and ensures clients are responded with proper response to clients.
*/

func TestConcurrentMulti(t *testing.T) {

	commands = []string{
		"set DUMMYKEY 15 10\r\nDUMMYVALUE\r\n",
		"set DUMMYKEY2 15 10\r\nDUMMYVALUE\r\n",			
		"delete DUMMYKEY2\r\n",
		"delete DUMMYKEY\r\n",
		"getm DUMMYKEY\r\n",
		"getm DUMMYKEY2\r\n",
	}

	done := make(chan bool)

	i := 0
	for i < noOfThreads {
		go CheckConcurrentMulti(t, done)
		i += 1
	}
	i = 0

	for i < noOfThreads {
		<-done
		i += 1
	}

	

}

func CheckConcurrentMulti(t *testing.T, done chan bool) {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		t.Error(err)
		done <- true
		return
	}
	reader := bufio.NewReader(conn)
	i := 0
	randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i < noOfRequestsPerThread {
	index := randomGenerator.Int() % len(commands)
	io.Copy(conn, bytes.NewBufferString(commands[index]))
		

		data,_ := reader.ReadBytes('\n')
		input := strings.TrimRight(string(data), "\r\n")
		array := strings.Split(input, " ")
		result := array[0]
		if result == "VALUE"{
			data,_ = reader.ReadBytes('\n')
			input := strings.TrimRight(string(data), "\r\n")
			temp,_ := strconv.Atoi(array[2])
			if input != "DUMMYVALUE" && temp <= 15{
			
			t.Fail()
			}
			
		}else{
		
		if result != "ERRNOTFOUND" && result != "ERR_VERSION" && result != "DELETED" && result != "OK" {
		
		t.Fail()
		}
		}
	
	i = i+1
	}
	
	
	
	done <- true

	conn.Close()
	
	}

