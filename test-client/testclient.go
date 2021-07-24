package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	pb "pa2/pb/protobuf"
	"strconv"
	"time"

	proto "github.com/golang/protobuf/proto"
)

// List of commands that can be sent to the server
const (
	PUT 				= 0x01
	GET 				= 0x02
	REMOVE 				= 0x03
	SHUTDOWN 			= 0x04
	WIPEOUT				= 0x05
	IS_ALIVE			= 0x06
	GET_PID				= 0x07
	GET_MEMBERSHIP_CNT	= 0x08
)

// List of errors that can be returned to client
const (
	NO_ERR			= 0x00
	KEY_DNE_ERR		= 0x01
	NO_SPC_ERR		= 0x02
	SYS_OVERLOAD_ERR	= 0x03
	KV_INTERNAL_ERR		= 0x04
	UNKNOWN_CMD_ERR		= 0x05
	INVALID_KEY_ERR		= 0x06
	INVALID_VAL_ERR		= 0x07
)

// Generate a unique message ID in the following format:
//    bytes           field
//    0 - 3      Client IP address
//    4 - 5      Client port number
//    6 - 7      Randomly generated
//    8 - 15     Timestamp in nanoseconds
//
// Arguments:
//     clientIPaddress: client IP address as a byte array
//     clientPort:      client port number 
// Returns:
//     Uniquely generated 16-byte message ID
func generateUniqueMsgID(clientIPaddress []byte, clientPort int)([]byte) {
	uniqueID := make([]byte, 16)

	// First 4 bytes contain the client IP address
	copy(uniqueID[0:4], clientIPaddress)
	log.Println("hello",uniqueID[0:4])

	// Next 2 bytes contain the client port number
	port := make([]byte, 2)
	binary.LittleEndian.PutUint16(port, uint16(clientPort))
	copy(uniqueID[4:6], port)

	// Next 2 bytes are randomly generated
	uniqueID[6] = uint8(rand.Intn(math.MaxUint8))
	uniqueID[7] = uint8(rand.Intn(math.MaxUint8))

	// Last 8 bytes are the timestamp in nanoseconds
	timeNs := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeNs, uint64(time.Now().UnixNano()))
	copy(uniqueID[8:], timeNs)

	return uniqueID
}

// Get the CRC-32 IEEE checksum of the message ID and payload
//
// Arguments:
//     messageID: unique ID of the message
//     payload:   payload of the message
// Returns:
//     CRC-32 of the message ID concatenated with the payload
func getChecksum(messageID []byte, payload []byte)(uint64) {
	return uint64(crc32.ChecksumIEEE(append(messageID, payload...)))
}

// Send a command to the server and get its response
func sendAndReceiveCommand(clientAddr *net.UDPAddr, serverIPaddress string, reqPay pb.KVRequest)(pb.KVResponse) {
	reqPayBytes, err := proto.Marshal(&reqPay)
	if err != nil {
		fmt.Println("Error marshalling the request payload:", err)
		return pb.KVResponse{}
	}

	// Generate the message ID
	msgID := generateUniqueMsgID(clientAddr.IP, clientAddr.Port)
	fmt.Println(msgID)

	// Generate the checksum
	checkSum := getChecksum(msgID, reqPayBytes)

	// Make the request message
	reqMsg := pb.Msg {
		MessageID: msgID,
		Payload: reqPayBytes,
		CheckSum: checkSum,
	}
	reqMsgBytes, err := proto.Marshal(&reqMsg)
	if err != nil {
		fmt.Println("Error marshalling the request message:", err)
		return pb.KVResponse{}
	}

	// Create the UDP connection object
	serverAddr, err := net.ResolveUDPAddr("udp", serverIPaddress)
	if err != nil {
		fmt.Println("Error resolving server address:", err)
		return pb.KVResponse{}
	}
	localAddr, err := net.ResolveUDPAddr("udp", clientAddr.String())
	if err != nil {
		fmt.Println("Error resolving client address:", err)
		return pb.KVResponse{}
	}

	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		fmt.Println("Error setting up the UDP client:", err)
		return pb.KVResponse{}
	}
	defer conn.Close()

	// Send the request
	_, _ = conn.WriteToUDP(reqMsgBytes, serverAddr)

	// Wait for a response
	buffer := make([]byte, 10064)
	numBytes, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		fmt.Println("Error during read from server:", err)
		return pb.KVResponse{} 
	} else {
		// Unmarshal the response message
		respMsg := pb.Msg {}
		err = proto.Unmarshal(buffer[:numBytes], &respMsg)
		if err != nil {
			fmt.Println("Error unmarshalling the response message:", err)
			return pb.KVResponse{}
		}

		// First verify if the checksum is correct
		checkSum = getChecksum(respMsg.MessageID, respMsg.Payload)
		if checkSum != respMsg.CheckSum {
			fmt.Println("Rceived checksum", respMsg.CheckSum, 
						"does not match actual checksum", checkSum)
			return pb.KVResponse{}
		}

		// Next verify if the message ID is correct
		if string(msgID) != string(respMsg.MessageID) {
			fmt.Println("Received message ID", hex.EncodeToString(respMsg.MessageID), 
						"does not match actual message ID", hex.EncodeToString(msgID))
			return pb.KVResponse{}
		}

		// Package looks good so far, unmarshal the payload
		respPay := pb.KVResponse {}
		err = proto.Unmarshal(respMsg.Payload, &respPay)
		if err != nil {
			fmt.Println("Error unmarshalling the response payload:", err)
		} else {
			return respPay
		}
	}

	return pb.KVResponse{}
}

// Runs the server tests
//
// Arguments:
//     clientAddr:      client local address object of type *net.UDPAddr
func testServer(clientAddr *net.UDPAddr, serverIPaddress string, port int) () {
		serverFullIP := fmt.Sprintf("%s:%d", serverIPaddress, port)

		/****** TEST 1: invalid PUT commands ******/
		
		// Key too long
		fmt.Println("Test 1a: Invalid PUT command, key too long")
		reqPay := pb.KVRequest {
			Command: PUT,
			Key: make([]byte, 33) , 
			Value: make([]byte, 1),
		}

		respPay := sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != INVALID_KEY_ERR {
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}

		// Value too long
		fmt.Println("Test 1b: Invalid PUT command, value too long")
		reqPay.Key = make([]byte, 32) 
		reqPay.Value = make([]byte, 10001)

		respPay = sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != INVALID_VAL_ERR {
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}

		/****** TEST 2: invalid GET and REMOVE commands ******/
		fmt.Println("Test 2a: Invalid GET command, key does not exist")
		reqPay.Command = GET
		reqPay.Key = []byte("invalid key")		
		reqPay.Value = []byte{}

		respPay = sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != KEY_DNE_ERR {
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}

		fmt.Println("Test 2b: Invalid REMOVE command, key does not exist")
		reqPay.Command = GET
		respPay = sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != KEY_DNE_ERR {
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}
		
		/****** TEST 3: IS_ALIVE, PID, GET_MEMBERSHIP_CNT commands ******/
		fmt.Println("Test 3a: IS_ALIVE")
		reqPay.Command = IS_ALIVE
		reqPay.Key = []byte{}
		respPay = sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != NO_ERR {
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}

		fmt.Println("Test 3b: PID")
		reqPay.Command = GET_PID
		respPay = sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != NO_ERR {
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}

		fmt.Println("Test 3c: GET_MEMBERSHIP_CNT")
		reqPay.Command = GET_MEMBERSHIP_CNT
		respPay = sendAndReceiveCommand(clientAddr, serverFullIP, reqPay)
		if respPay.ErrCode != KEY_DNE_ERR && respPay.MembershipCount != 1{
			fmt.Println("TEST FAILED")
		} else {
			fmt.Println("TEST PASSED")
		}
}

// Print the usage of the program
func usage() {
	fmt.Println("Usage: go run test-client/testclient.go [serverIPaddress] [port]")
}

func main() {
	//Check that number of arguments is correct
	if len(os.Args) != 3 {
		fmt.Println("Error: number of arguments is incorrect")
		usage()
		os.Exit(1)
	}

	// Get the arguments
	serverIPaddress := os.Args[1]

	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Error: port argument should be valid integer")
		usage()
		os.Exit(1)
	}

	// Get the client IP address
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		fmt.Println("Error: could not get local IP address:", err)
		os.Exit(2)
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	conn.Close()

	testServer(localAddr, serverIPaddress, port)
}