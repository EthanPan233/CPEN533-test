package pa2lib

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	pb "pa2/pb/protobuf"
	"time"

	"github.com/golang/protobuf/proto"
)

func unmarshalKVRequest(buf []byte)(pb.KVRequest, []byte, int) {
	// Unmarshal the request
	msgID, payload,_ := unmarshalMsg(buf)

	// Unmarshal the payload
	reqPay := pb.KVRequest {}
	err := proto.Unmarshal(payload, &reqPay)
	if err != nil {
		return pb.KVRequest{}, nil, -1
	} else {
		return reqPay, msgID, 0
	}
}

// Given a request message from a client, parse the request,
// perform the necessary action for the command of the request
// and return a response.
//
// Arguments:
// 		clientAddr: address to return to the response to
//		msgID: message ID of the request
//		reqPay: unmarshalled payload
func handleKVRequest(clientAddr *net.UDPAddr, msgID []byte, reqPay pb.KVRequest) () {
	log.Println("start handling request")
	log.Println(msgID)
	log.Println("sender IP:", net.IPv4(msgID[0], msgID[1], msgID[2], msgID[3]).String(), ":", binary.LittleEndian.Uint16(msgID[4:6]))
	log.Println("command:", reqPay.Command)
	if reqPay.Addr == nil {

		reqPay.Addr = []byte(clientAddr.String())
	}

	// Try to find the response in the cache
	if respMsgBytes, ok := GetCachedResponse(msgID); ok {
		// Send the message back to the client
		_, _ = conn.WriteToUDP(respMsgBytes, clientAddr)
	} else {
		// Handle the command
		respPay := pb.KVResponse{}

		/*
			If the command is PUT, GET or REMOVE, check whether the key exists in
			this node first. Otherwise,
		*/
		switch reqPay.Command {
		case PUT:
			// respPay.ErrCode = Put(reqPay.Key, reqPay.Value, reqPay.Version)
			if node, existed := checkNode(reqPay.Key); existed {
				respPay.ErrCode = Put(reqPay.Key, reqPay.Value, &reqPay.Version)
				normalReplicate(PUT, reqPay.Key, reqPay.Value, reqPay.Version)
			} else {
				sendRequestToCorrectNode(node, reqPay, msgID)
				return
			}
		case GET:
			// var version int32
			// respPay.Value, version, respPay.ErrCode = Get(reqPay.Key)
			// respPay.Version = &version
			if node, existed := checkNode(reqPay.Key); existed {
				var version int32
				respPay.Value, version, respPay.ErrCode = Get(reqPay.Key)
				respPay.Version = version
			} else {
				sendRequestToCorrectNode(node, reqPay, msgID)
				return
			}
		case REMOVE:
			// respPay.ErrCode = Remove(reqPay.Key)
			if node, existed := checkNode(reqPay.Key); existed {
				respPay.ErrCode = Remove(reqPay.Key)
				normalReplicate(REMOVE, reqPay.Key, reqPay.Value, reqPay.Version)
			} else {
				sendRequestToCorrectNode(node, reqPay,msgID)
				return
			}
		case SHUTDOWN:
			//log.Println("############################################################################")
			//log.Println("########################### SHUT DOWN ! ####################################")
			//log.Println("############################################################################")

			shutdown <- true
			return
		case WIPEOUT:
			respPay.ErrCode = RemoveAll()
			normalReplicate(WIPEOUT, reqPay.Key, reqPay.Value, reqPay.Version)
		case IS_ALIVE:
			respPay.ErrCode = NO_ERR
		case GET_PID:
			pid := int32(os.Getpid())
			respPay.Pid = pid
			respPay.ErrCode = NO_ERR
		case GET_MEMBERSHIP_CNT:
			members := int32(1) // Unused, return 1 for now
			respPay.MembershipCount = members
			respPay.ErrCode = NO_ERR
		case GET_MEMBERSHIP_LIST:
			GetMemberShipList(clientAddr, msgID, respPay)
			return
		//forward request
		case PUT_FORWARD:
			respPay.ErrCode = Put(reqPay.Key, reqPay.Value, &reqPay.Version)
			normalReplicate(PUT, reqPay.Key, reqPay.Value, reqPay.Version)
			clientAddr, _ = net.ResolveUDPAddr("udp", string(reqPay.Addr))

		case GET_FORWARD:
			var version int32
			respPay.Value, version, respPay.ErrCode = Get(reqPay.Key)
			respPay.Version = version
			clientAddr, _ = net.ResolveUDPAddr("udp", string(reqPay.Addr))

		case REMOVE_FORWARD:
			// respPay.ErrCode = Remove(reqPay.Key)
			respPay.ErrCode = Remove(reqPay.Key)
			normalReplicate(REMOVE, reqPay.Key, reqPay.Value, reqPay.Version)
			clientAddr, _ = net.ResolveUDPAddr("udp", string(reqPay.Addr))

		case PUT_REPLICATE_SON:
			PutReplicate(reqPay.Key, reqPay.Value, &reqPay.Version, 0)
			return
		case PUT_REPLICATE_GRANDSON:
			PutReplicate(reqPay.Key, reqPay.Value, &reqPay.Version, 1)
			return
		case REMOVE_REPLICATE_SON:
			RemoveReplicate(reqPay.Key, 0)
			return
		case REMOVE_REPLICATE_GRANDSON:
			RemoveReplicate(reqPay.Key, 1)
			return
		case WIPEOUT_REPLICATE_SON:
			WipeoutReplicate(0)
			return
		case WIPEOUT_REPLICATE_GRANDSON:
			WipeoutReplicate(1)
			return

		case GRANDSON_DIED:
			addr, _ := net.ResolveUDPAddr("udp",string(reqPay.Addr))
			sendNodeDieReplicateRequest(FATHER_DIED, KVStore, addr)
			return
		case SON_DIED:
			addr, _ := net.ResolveUDPAddr("udp",string(reqPay.Addr))
			sendNodeDieReplicateRequest(GRANDFATHER_DIED_1, KVStore, addr)
			return

		case HELLO:
			addr, _ := net.ResolveUDPAddr("udp", string(reqPay.Addr))
			receiveHello(addr, msgID)
			return
		default:
			respPay.ErrCode = UNKNOWN_CMD_ERR
		}

		// Send the response
		sendResponse(clientAddr, msgID, respPay)
	}
}

func KVReqHandler(port int) {

	rcvBuffer := make([]byte, 11000)
	for {
		select {
		case <- shutdown:
			//if err := cmd.Process.Kill(); err != nil {
			//	log.Fatal("failed to kill process: ", err)
			//}
			log.Println("Server shutdown")
			os.Exit(2)
			return
		default:
			// Set up the receive timeout to 100ms
			deadline := time.Now().Add(100*1000*1000)
			_ = conn.SetReadDeadline(deadline)

			numBytes, clientAddr, err := conn.ReadFromUDP(rcvBuffer)
			if err == nil {
				// Unmarshal and handle the request in a different thread
				reqPay, id, err := unmarshalKVRequest(rcvBuffer[:numBytes])
				if err == 0 {
					go handleKVRequest(clientAddr, id, reqPay)
				}
			}
		}
	}
}
