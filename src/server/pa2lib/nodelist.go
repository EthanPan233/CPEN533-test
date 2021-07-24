package pa2lib

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"pa2/pb/protobuf"
	pb "pa2/pb/protobuf"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

// NodeVal Data type used to store a node
type NodeVal struct {
	ipAdr      string
	port       string
	isOn       bool
	membership string
	time       uint64
}

var listMutex = &sync.Mutex{}

var nodeList = map[string]*NodeVal{}
var startNodeList = map[string]NodeVal{}
var localPort = os.Args[1]

func initNodeList(serverListFile string ) {
	//listMutex.Lock()
	//defer listMutex.Unlock()
	file, err := os.OpenFile(serverListFile, os.O_RDONLY, 0666)
	if err != nil {
		//log.Println("Open file error!", err)
		return
	}
	defer file.Close()

	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println(line, " file read ok!")
				break
			} else {
				//log.Println(line, " read file error!", err)
				return
			}
		}

		line = strings.TrimSpace(line)
		log.Println(line)
		s := strings.Split(line, ":")
		log.Println("Initialize node: " + s[0] + ":" + s[1])
		IP := s[0]
		port := s[1]
		node := NodeVal{ipAdr: IP, port: port, isOn: true, membership: "0", time: 0}
		nodeList[IP + ":" + port] = &node
		startNodeList[IP + ":" + port] = node
	}
}

func GetMemberShipList(clientAddr *net.UDPAddr, msgID []byte, respPay pb.KVResponse) {
	//listMutex.Lock()
	//defer listMutex.Unlock()

	log.Println("send membership list")
	var returnMap = map[string][]byte{}
	for addr, node := range nodeList {
		nodeValPro := pb.NodeVal{IpAdr: node.ipAdr,
											Port: node.port,
											IsOn: node.isOn,
											Membership: node.membership,
											Time: node.time}
		returnMap[addr], _ = proto.Marshal(&nodeValPro)
	}
	respPay.NodeList, respPay.Version, respPay.ErrCode = returnMap, 0, KEY_DNE_ERR

	// Marshal the payload
	respPayBytes, err := proto.Marshal(&respPay)
	if err != nil {
		return
	}

	// Generate a checksum
	checkSum := getChecksum(msgID, respPayBytes)

	// Create the response message
	//port, _ := strconv.Atoi(localPort)
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

	// Send the node list to the requester
	receiverAddr, _ := net.ResolveUDPAddr("udp", clientAddr.IP.String() + ":" + strconv.Itoa(clientAddr.Port + 1))
	_, _ = conn.WriteToUDP(respMsgBytes, receiverAddr)

	log.Println("sendResponse", clientAddr.IP, clientAddr.Port)
}

func requestNodeList(ipAdr string, port int) {
	//log.Println("request ndoe list form", ipAdr, port)

	p, _ := strconv.Atoi(localPort)
	ip := net.ParseIP(localIP)
	msgID := generateUniqueMsgID(ip[len(ip)-4:], p)

	// Get a request message
	reqMessage, err := getGossipRequestMessage(msgID)
	// Send request and get respond
	newNodeList, err := sendReqAndGetRes(reqMessage, msgID, conn, ipAdr, port)

	//listMutex.Lock()
	//defer listMutex.Unlock()
	if err == nil {
		// the gossip succeed, now merge node lists
		log.Println("get node list from", ipAdr, port)
		err = mergeNodeLists(newNodeList)
		log.Printf("Gossip succeed, now merge two node lists.\n")
	} else {
		// if the gossip fail, it mean the target node is dead, so update the list
		log.Println("gossip just failed, check if the target node is on:", nodeList[ipAdr + ":" + strconv.Itoa(port)].isOn)
		foundDeadNode(ipAdr, strconv.Itoa(port))
		//currentPort, _ := strconv.Atoi(os.Args[1])
		log.Printf("%v, Gossip failed, now remove %v:%v from current node list.\n", ipAdr, port)
	}
	if err != nil {
		log.Println(err)
	}
}

func doGossip() {
	// time interval of gossip
	var gossipTime = time.Now()
	var gossipIntvl = time.Since(gossipTime)

	for {
		// gossip in a given frequency
		gossipIntvl = time.Since(gossipTime)
		if gossipIntvl > 3000*time.Millisecond {
			gossipTime = time.Now()
			currentPort, _ := strconv.Atoi(os.Args[1])
			log.Printf("Port # %v, Start gossiping", currentPort)
			// Randomly generate a list of listeners
			var numListeners int
			var activeNodeList = map[string]NodeVal{}
			//listMutex.Lock()
			for key, node := range nodeList {
				if node.isOn == true && (node.ipAdr != localIP || node.port != localPort) {
					activeNodeList[key] = *node
				}
			}
			//listMutex.Unlock()
			if len(activeNodeList) > 4 {
				if len(activeNodeList)/5 < 4 {
					numListeners = 4
				} else {
					numListeners = len(activeNodeList) / 5
				}
			} else {
				numListeners = len(activeNodeList)
			}
			log.Printf("The number of gossip targets is: %v\n", numListeners)
			log.Printf("The length of active node list is: %v\n", len(activeNodeList))
			//log.Println( "node list:", nodeList)


			var list = rand.Perm(len(activeNodeList))

			var listenerList = map[string]NodeVal{}
			i := 0
			for key, val := range activeNodeList {
				if i >= numListeners { break }
				for _, j := range list {
					if i == j {
						listenerList[key] = val
					}
					i++
				}
			}

			// request nodeList from listeners
			for _, listener := range listenerList {
				if listener.ipAdr == localIP && listener.port == localPort {
					continue
				}
				port, _ := strconv.Atoi(listener.port)
				requestNodeList(listener.ipAdr, port)
			}
		}
	}
}

func getGossipRequestMessage(messageID []byte) ([]byte, error) {
	reqPayload := getGossipRequestPayload()
	checksum := crc32.ChecksumIEEE(append(messageID, reqPayload...))
	request := &protobuf.Msg{
		MessageID: messageID,
		Payload:   reqPayload,
		CheckSum:  uint64(checksum),
	}
	reqMessage, err := proto.Marshal(request)
	if err != nil {
		log.Println(err)
	}
	return reqMessage, err
}

func getGossipRequestPayload() []byte {
	requestPayload := &protobuf.KVRequest{
		Command: GET_MEMBERSHIP_LIST,
	}
	reqPayload, err := proto.Marshal(requestPayload)
	if err != nil {
		log.Println(err)
	}
	return reqPayload
}

func getResponseMessage(reply []byte) ([]byte, []byte, uint64) {
	replyMsg := &protobuf.Msg{}
	err := proto.Unmarshal(reply, replyMsg)
	if err != nil {
		log.Println(err)
	}
	return replyMsg.GetMessageID(), replyMsg.GetPayload(), replyMsg.GetCheckSum()
}

func nodeListParseFromByteArray(nodeListPayload map[string][]byte) map[string]NodeVal {
	var newNodeList = map[string]NodeVal{}
	for addr, nodeByte := range nodeListPayload {
		var nodePb = pb.NodeVal{}
		err := proto.Unmarshal(nodeByte, &nodePb)
		if err != nil {
			log.Println("nodeListParseFromByteArray error")
		}
		newNodeList[addr] = NodeVal{ipAdr: nodePb.IpAdr, port: nodePb.Port, isOn: nodePb.IsOn, membership: nodePb.Membership, time: nodePb.Time}
	}
	return newNodeList
}

func sendReqAndGetRes(reqMessage []byte, msgID []byte, connReq *net.UDPConn, ipAdr string, port int) (map[string]NodeVal, error) {
	// Initialize timeout and times of retry
	timeout := 100 // 100ms
	attempts := 0

	receiverAddr, err := net.ResolveUDPAddr("udp", ipAdr + ":" + strconv.Itoa(port))
	localPortInt, _ := strconv.Atoi(localPort)
	localGosAddr, _ := net.ResolveUDPAddr("udp", localIP + ":" + strconv.Itoa(localPortInt + 1))
	connGossip, err := net.ListenUDP("udp", localGosAddr)
	defer connGossip.Close()

	for attempts < 4 {

		// Send request message to the server
		buf := reqMessage
		_, err := connGossip.WriteToUDP(buf, receiverAddr)
		if err != nil {
			log.Println(err)
		}

		// Receive response from the server
		if err != nil {
			log.Printf("Dial fialed: %v:%v\n", receiverAddr.IP, receiverAddr.Port)
			log.Println(err)
		}

		// Defer closing the underlying file discriptor associated with the socket
		err = connGossip.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(timeout)))
		buf = make([]byte, 65535) // clean the buffer
		len, _, err := connGossip.ReadFrom(buf)
		replyMsg := buf[0:len]
		if err != nil {
			// fmt.Println(err)
			log.Println("retry ", attempts, ": sendReqAndGetRes readFrom error!")
			timeout = timeout * 2
			attempts++
		} else {
			// check whether the checksum is corrrect or not
			messageID, msgPayload, checksum := getResponseMessage(replyMsg)
			expectedChecksum := uint64(crc32.ChecksumIEEE(append(messageID, msgPayload...)))
			var resPayload = &protobuf.KVResponse{}
			if checksum == expectedChecksum &&
				hex.EncodeToString(messageID) == hex.EncodeToString(msgID) {
				err := proto.Unmarshal(msgPayload, resPayload)
				if err != nil {
					log.Println(err)
				}
				return nodeListParseFromByteArray(resPayload.NodeList), nil
				//return secretCode, nil
			} else {
				//log.Println("retry: received wrong message")
				//fmt.Println("Corrupt Data")
				timeout = timeout * 2
				attempts++
			}
		}
	}
	err = errors.New("All gossip the retries fail")
	//log.Println(err)
	return nil, err
}

// TODO: consider the situation where removed nodes can be readded
func mergeNodeLists(newNodeList map[string]NodeVal) error {
	//listMutex.Lock()
	//defer listMutex.Unlock()
	log.Println("Start merging two node lists.")
	for addr, _ := range nodeList {
		if  _, v := newNodeList[addr]; v{
		} else {
			return errors.New("!")
		}
		if nodeList[addr].isOn != newNodeList[addr].isOn {
			if nodeList[addr].time < newNodeList[addr].time {
				if nodeList[addr].isOn == true && newNodeList[addr].isOn == false {
					ip := nodeList[addr].ipAdr
					port := nodeList[addr].port
					foundDeadNode(ip, port)
				}
				*nodeList[addr] = newNodeList[addr]
				log.Println("turn off node when merging:", addr)
			}
		}
	}
	return nil
}

// Remove a target node from the nodeList
// it also returns the new nodeList
func turnOffNodeFromList(ipAdr string, port string) error {
	addr := ipAdr + ":" + port
	if val, ok := nodeList[addr]; ok {
		val.isOn = false
		val.time = uint64(time.Now().UnixNano())
	} else {
		//log.Println("Error: can't turn off " + addr + " from node list, because it doesn't exist!", localIP, localPort)
		return errors.New("turnOffNodeFromList error")
	}
	log.Println("Turn off " + addr + " from node list.", localIP, localPort)
	return nil
}

func turnOnNodeFromList(ip string, port int, msgId []byte) error {
	//listMutex.Lock()
	//defer listMutex.Unlock()
	addr := ip + ":" + strconv.Itoa(int(port))
	sentTime := binary.LittleEndian.Uint64(msgId[8:])
	if val, ok := nodeList[addr]; ok {
		if val.isOn {
			//log.Println("node", ip, port, "is already on.")
			return nil
		}
		log.Println("Turn on node: ", addr)
		val.isOn = true
		val.time = sentTime
	} else {
		//log.Println("Error: can't turn on " + addr + " from node list, because it doesn't exist!", localIP, localPort)
		return errors.New("turnOnNodeFromList error")
	}
	log.Println("Turn on " + addr + " from node list.", localIP, localPort)

	//update hashring
	consistent.addNodetoHashringGossip(ip, port)

	//replicate
	welcomeNewNode(*nodeList[ip+":"+strconv.Itoa(port)])

	return nil
}


func getNodeList() map[string]*NodeVal {
	//listMutex.Lock()
	//defer listMutex.Unlock()
	return nodeList
}

func receiveHello(addr *net.UDPAddr, mesId []byte) {
	//listMutex.Lock()
	//defer listMutex.Unlock()
	log.Println("Receive hello from: " + addr.IP.String() + ":", addr.Port)
	//modify nodelist
	err := turnOnNodeFromList(addr.IP.String(), addr.Port, mesId)

	if err != nil {
		log.Println("turn on node", addr.IP.String(), addr.Port, "failed!")
	}
}

func foundDeadNode(ip string, port string) {
	//log.Println("found dead", ip, port)

	//modify nodelist
	turnOffNodeFromList(ip, port)

	//replicate
	//addr := ip + ":" + port
	nodeDieReplicate(*nodeList[ip + ":" + port])

	//update hashring
	consistent.removeNodefromHashring(ip, port)
}