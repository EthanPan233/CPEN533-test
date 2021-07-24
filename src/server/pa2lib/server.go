package pa2lib

import (
	"fmt"
	"log"
	"net"
	"os"
	pb "pa2/pb/protobuf"
	"strconv"

	"github.com/golang/protobuf/proto"
)

// List of errors that can be returned to client
const (
	NO_ERR           = 0x00
	KEY_DNE_ERR      = 0x01
	NO_SPC_ERR       = 0x02
	SYS_OVERLOAD_ERR = 0x03
	KV_INTERNAL_ERR  = 0x04
	UNKNOWN_CMD_ERR  = 0x05
	INVALID_KEY_ERR  = 0x06
	INVALID_VAL_ERR  = 0x07
)

// List of commands that can be sent to the server
const (
	PUT                       = 0x01
	GET                       = 0x02
	REMOVE                    = 0x03
	SHUTDOWN                  = 0x04
	WIPEOUT                   = 0x05
	IS_ALIVE                  = 0x06
	GET_PID                   = 0x07
	GET_MEMBERSHIP_CNT        = 0x08
	GET_MEMBERSHIP_LIST       = 0x22
	PUT_FORWARD               = 0x23
	GET_FORWARD               = 0x24
	REMOVE_FORWARD            = 0x25
	PUT_REPLICATE_SON         = 0x26
	PUT_REPLICATE_GRANDSON    = 0x27
	REMOVE_REPLICATE_SON      = 0x28
	REMOVE_REPLICATE_GRANDSON = 0x29
	WIPEOUT_REPLICATE_SON 	  = 0x2a
	WIPEOUT_REPLICATE_GRANDSON= 0x2b

	GRANDSON_DIED = 0x30
	SON_DIED = 0x31
	FATHER_DIED = 0x32
	GRANDFATHER_DIED_1 = 0x33
	GRANDFATHER_DIED_2 = 0x34
	I_AM_YOUR_FATHER = 0x35
	I_AM_YOUR_GRANDFATHER = 0x36
	I_AM_YOUR_SON = 0x37
	I_AM_YOUR_GRANDSON = 0x38
	HELLO = 0x40
)

// Constant to use for the server overload condition
var overloadWaitTimeMs = int32(5000)

// Global variable to store the UDP connection object so
// that it does not need to be passed to every function
var conn *net.UDPConn

// Global counter of how many clients are currently being handled,
// should be updated atomically
var numClients uint64

// Channel to signal to the server to quit
var shutdown = make(chan bool)

// hash ring
var consistent = newConsistent()

var localIP string

func unmarshalMsg(msg []byte)([]byte, []byte, uint64){

	Msg := pb.Msg{}
	err := proto.Unmarshal(msg, &Msg)
	if err != nil{
		log.Println("Error unmarshal message!")
	}
	checkSum := getChecksum(Msg.MessageID, Msg.Payload)
	if checkSum != Msg.CheckSum {
		log.Println("Checksum is not right for: ", Msg.MessageID)
		return nil, nil, 0
	}

	return Msg.MessageID, Msg.Payload, Msg.CheckSum
}

// Create and send the response message to the client
//
// Arguments:
//		clientAddr: address to return to the response to
//		msgID: ID of the request
//		respPay: payload to return in the response
func sendResponse(clientAddr *net.UDPAddr, msgID []byte, respPay pb.KVResponse) {
	// Marshal the payload
	respPayBytes, err := proto.Marshal(&respPay)
	if err != nil {
		return
	}

	// Generate a checksum
	checkSum := getChecksum(msgID, respPayBytes)

	// Create the response message
	respMsg := pb.Msg{
		MessageID: msgID,
		Payload:   respPayBytes,
		CheckSum:  checkSum,
	}

	// Marshal the message
	respMsgBytes, err := proto.Marshal(&respMsg)
	if err != nil {
		return
	}

	// Cache the response if it's not a server overload
	if respPay.ErrCode != SYS_OVERLOAD_ERR {
		// If there is no space to add to the cache, send a server
		// overload response instead
		if !CacheResponse(msgID, respMsgBytes) {
			log.Println("save into cache")
			respPay.ErrCode = SYS_OVERLOAD_ERR
			sendResponse(clientAddr, msgID, respPay)
		}
	}

	// Send the message back to the client
	_, _ = conn.WriteToUDP(respMsgBytes, clientAddr)
	log.Println("sendResponse", clientAddr.IP, clientAddr.Port)
}

func sayHelloToEveryone(nodeList map[string]*NodeVal) {
	for _, node := range nodeList {
		if node.ipAdr == localIP && node.port == localPort {
			log.Println("It's myself, skip send node")
			continue
		}
		reqPay := pb.KVRequest{ Command: HELLO }
		log.Println("Now say hello to " + node.ipAdr + ":"+node.port)
		port, _ := strconv.Atoi(localPort)
		ip := net.ParseIP(localIP)
		msgID := generateUniqueMsgID(ip[len(ip)-4:], port)
		sendRequestToCorrectNode(*node, reqPay, msgID)
	}
}

// Starts the UDP server. Waits for a message to be received
// and spawns a goroutine to handle the client's request
//
// Arguments:
//		port: port number to listen on
func StartServer(serverListFile string, port int) {
	// Start the cache TTL manager
	go CacheTTLManager()

	// Start the UDP server
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Println("Error resolving server address:", err)
		return
	}

	conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		log.Println("Error setting up the UDP server:", err)
		return
	}

	defer conn.Close()

	log.Println("Server start")

	conn1, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Println("Error: could not get local IP address:", err)
		os.Exit(2)
	}
	localAddr := conn1.LocalAddr().(*net.UDPAddr)
	localIP = localAddr.IP.String()
	log.Println("Local IP:", localIP)
	conn1.Close()

	// generate hash ring for the current node
	initNodeList(serverListFile)
	nodeList := getNodeList()
	consistent.generateHashRing(nodeList, strconv.Itoa(port))

	//defer conn.Close()
	go RepRequestHandler()

	sayHelloToEveryone(nodeList)

	go doGossip()
	go KVReqHandler(port)

	for{}
}
