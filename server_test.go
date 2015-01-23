package main
//package verbose


import (
	//"verbose"
	//"fmt"
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	//"sync"
	"testing"
	"time"
	"strings"
	
	//	"math/rand"
)

var noOfThreads int = 2000
var noOfRequestsPerThread int = 1
var commands []string



func init(){
go main()
}


/* 
TestConcurrentSet() checks concurrent set() operations by multiple clients and PASSes the test if version mateches the expected version number
*/

func TestConcurrentSet(t *testing.T) {

	
	done := make(chan bool)

	//spwan client threads
	i := 0
	for i < noOfThreads {
		go check_concurrent_set(t,done)
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
func check_concurrent_set(t *testing.T,done chan bool) {
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

func check_basic_functionality(t *testing.T,done chan bool) {
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
		temp,_ :=strconv.Atoi(array[1])

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
			
			temp ,_ = strconv.Atoi(array[1])
			if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
			}
			
		}else{
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
			
			temp ,_ = strconv.Atoi(array[3])
			if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
			}
			
		}else{
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
			
			temp ,_ = strconv.Atoi(array[3])
			if temp != len(array1[0]) || array1[0] != "mayur" {
			t.Fail()
			}
			
		}else{
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

func checkBasicErrorCommands(t *testing.T,done chan bool) {
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
				if input != "ERRCMDERR"{
			t.Fail()
		}
		io.Copy(conn, bytes.NewBufferString("set key 100 5\r\nmayur\r\n"))

		data, err = reader.ReadBytes('\n')
		input = strings.TrimRight(string(data), "\r\n")
		array := strings.Split(input, " ")
		result := array[0]
		temp,_ :=strconv.Atoi(array[1])
		
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
			
				if input != "ERR_VERSION"{
			t.Fail()
		}
		
		/*
		Test for wrong command sent to server
		*/
		
		io.Copy(conn, bytes.NewBufferString("cal key 2672 1 5\r\nmayur\r\n"))

		data, err = reader.ReadBytes('\n')
			
		
		input = strings.TrimRight(string(data), "\r\n")
		//t.Log(input)	
			
				if input != "ERRCMDERR"{
			t.Fail()
		}
		

		
	done <- true
	
	conn.Close()

}







