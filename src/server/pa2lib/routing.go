package pa2lib

import (
	"fmt"
	"log"
	"net"
	pb "pa2/pb/protobuf"
	"strconv"

	"github.com/golang/protobuf/proto"
)

// send the request to the correct node
func sendRequestToCorrectNode(node NodeVal, reqPay pb.KVRequest, msgID []byte) {
	switch reqPay.Command {
	case GET:
		reqPay.Command = GET_FORWARD
		break
	case PUT:
		reqPay.Command = PUT_FORWARD
		break
	case REMOVE:
		reqPay.Command = REMOVE_FORWARD
		break
	case HELLO:
		reqPay.Command = HELLO
		break
	case IS_ALIVE:
		reqPay.Command = IS_ALIVE
		break
	default:
		log.Println("Unknown command!")
	}

	newPort, err := strconv.Atoi(node.port)
	if err != nil {
		log.Fatal(err)
	}

	// create UDPAddr which we could transfer request to
	addr := net.UDPAddr{
		Port: newPort,
		IP:   net.ParseIP(node.ipAdr),
	}

	marshaledReqPay, err := proto.Marshal(&reqPay)
	if err != nil {
		log.Fatal(err)
	}
	checkSum := getChecksum(msgID, marshaledReqPay)

	reqMsg := pb.Msg{
		MessageID: msgID,
		Payload:   marshaledReqPay,
		CheckSum:  checkSum,
	}

	marshaledReqMsg, err := proto.Marshal(&reqMsg)

	// send request to correct node
	//conn1, err := net.ListenUDP("udp", &addr)
	//defer conn1.Close()

	_, err = conn.WriteToUDP(marshaledReqMsg, &addr)
	//log.Println("sendRequestToCorrectNode", addr.IP, addr.Port)
	if err != nil {
		fmt.Println("could not write to the correct node", err)
	}
}

