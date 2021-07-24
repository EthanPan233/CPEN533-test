package pa2lib

import (
	"fmt"
	"log"
	"net"
	pb "pa2/pb/protobuf"
	"strconv"

	"github.com/golang/protobuf/proto"
)

func sendNormalReplicateRequest(cmd uint32, key []byte, value []byte, version int32, addr *net.UDPAddr){
	//skip if receiver is myself
	port, _ := strconv.Atoi(localPort)
	if addr.IP.String() == localIP && addr.Port == port{
		return
	}

	req := pb.KVRequest{
		Command: cmd,
		Key: key,
		Value: value,
		Version: version,
	}

	marshaledRequest, err := proto.Marshal(&req)
	if err != nil {
		log.Println("Failed to encode req message:", err)
	}

	MID := generateUniqueMsgID(addr.IP, addr.Port)
	msg := pb.Msg{
		MessageID: MID,
		Payload: marshaledRequest,
		CheckSum: getChecksum(MID, marshaledRequest),
	}

	marshaledReqMsg, _ := proto.Marshal(&msg)

	_, err = conn.WriteToUDP(marshaledReqMsg, addr)
	//log.Println("Replicate request sent to: ", addr.IP,":", addr.Port)
	if err != nil {
		log.Println("could not write to the correct node", err)
	}
}

//This function should be called whenever the node's KV store has been changed
func normalReplicate(cmd uint32, key []byte, value []byte, version int32){
	node := *nodeList[localIP+":"+localPort]
	//log.Println("Start normal replicate")
	//log.Println("I am node: ", node.ipAdr+ ":"+node.port)

	//to son
	son := consistent.getNextNode(node)
	//log.Println("My son is: ", son.ipAdr+":"+son.port)
	port, _ := strconv.Atoi(son.port)

	sonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(son.ipAdr),
	}

	sendNormalReplicateRequest(cmd + 0x25, key, value, version, &sonAddr)

	//to grandson
	grandson := consistent.getNextNode(son)
	//log.Println("My grandson is: ", grandson.ipAdr+":"+grandson.port)
	port, _ = strconv.Atoi(grandson.port)

	grandsonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(grandson.ipAdr),
	}

	sendNormalReplicateRequest(cmd + 0x26, key, value, version, &grandsonAddr)
}

func notifyLowerNodeDie(cmd uint32, addr *net.UDPAddr, dstAddr *net.UDPAddr){
	req := pb.KVRequest{
		Command: cmd,
		Addr: []byte(dstAddr.String()),
	}

	marshaledRequest, err := proto.Marshal(&req)
	if err != nil {
		fmt.Println("Failed to encode req message:", err)
	}

	MID := generateUniqueMsgID(addr.IP, addr.Port)
	msg := pb.Msg{
		MessageID: MID,
		Payload: marshaledRequest,
		CheckSum: getChecksum(MID, marshaledRequest),
	}

	marshaledReqMsg, err := proto.Marshal(&msg)

	_, err = conn.WriteToUDP(marshaledReqMsg, addr)
	if err != nil {
		fmt.Println("could not write to the correct node", err)
	}
}

func nodeDieReplicate(node NodeVal){
	father := consistent.getLastNode(node)
	grandfather := consistent.getLastNode(father)
	son := consistent.getNextNode(node)
	grandson := consistent.getNextNode(son)

	port, _ := strconv.Atoi(grandfather.port)

	grandfatherAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(grandfather.ipAdr),
	}

	port, _ = strconv.Atoi(father.port)

	fatherAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(father.ipAdr),
	}

	port, _ = strconv.Atoi(son.port)

	sonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(son.ipAdr),
	}

	port, _ = strconv.Atoi(grandson.port)

	grandsonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(grandson.ipAdr),
	}

	//to grandfather
	notifyLowerNodeDie(GRANDSON_DIED, &grandfatherAddr, &sonAddr)

	//to father
	notifyLowerNodeDie(SON_DIED, &fatherAddr, &grandsonAddr)
}

//set repreq to node at addr with KVPairs
func sendNodeDieReplicateRequest(cmd uint32, KVPairs []StoreVal, addr *net.UDPAddr){
	//skip if receiver is myself
	//port, _ := strconv.Atoi(localPort)
	//if addr.IP.String() == localIP && addr.Port == port{
	//	return
	//}

	addr.Port = REP_REQUEST_PORT

	 notification := pb.RepRequest{
		Command: cmd,
	 }

	for _, kv := range KVPairs{
		newKVPair := pb.RepRequest_KVPair{
			Key: kv.key,
			Value: kv.value,
			Version: kv.version,
		}
		notification.Kvs = append(notification.Kvs, &newKVPair)
	}

	marshaledNotification, err := proto.Marshal(&notification)
	if err != nil {
		fmt.Println("Failed to encode req message:", err)
	}

	_, err = repConn.WriteToUDP(marshaledNotification, addr)
	//log.Println("sendNodeDieReplicateRequest", addr.IP, addr.Port)
	if err != nil {
		fmt.Println("could not write to the correct node", err)
	}
}

func onFatherDie(KVPairs []*pb.RepRequest_KVPair){
	//append repKVStore[0] to KVStore
	for _, KVPair := range repKVStore[0]{
		KVStore = append(KVStore, KVPair)
	}

	//move repKVStore[1] to repKVStore[0]
	repKVStore[0] = nil
	for _, KVPair := range repKVStore[1]{
		repKVStore[0] = append(repKVStore[0], KVPair)
	}

	//Copy KVPairs from the message to repKVStore[1]
	for _, KVPair := range KVPairs{
		storeVal := StoreVal{key: KVPair.Key, value: KVPair.Value, version: KVPair.Version}
		repKVStore[1] = append(repKVStore[1], storeVal)
	}

	son := consistent.getNextNode(*nodeList[localIP+":"+localPort])

	port, _ := strconv.Atoi(son.port)

	sonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(son.ipAdr),
	}

	sendNodeDieReplicateRequest(GRANDFATHER_DIED_2, KVStore, &sonAddr)
}

//store kvs in req into repKVStore[1]
func ReplicateFromGrandFather(KVPairs []*pb.RepRequest_KVPair){
	//Copy KVPairs from the message to repKVStore[1]
	for _, KVPair := range KVPairs{
		storeVal := StoreVal{key: KVPair.Key, value: KVPair.Value, version: KVPair.Version}
		repKVStore[1] = append(repKVStore[1], storeVal)
	}
}

////store kvs in req into repKVStore[0]
func ReplicateFromFather(KVPairs []*pb.RepRequest_KVPair){
	//Copy KVPairs from the message to repKVStore[0]
	for _, KVPair := range KVPairs{
		storeVal := StoreVal{key: KVPair.Key, value: KVPair.Value, version: KVPair.Version}
		repKVStore[0] = append(repKVStore[0], storeVal)
	}
}

//copy KVPairs in request to KVStore
func ReplicateFromSon(KVPairs []*pb.RepRequest_KVPair){
	for _, KVPair := range KVPairs{
		storeVal := StoreVal{key: KVPair.Key, value: KVPair.Value, version: KVPair.Version}
		KVStore = append(KVStore, storeVal)
	}

	son := consistent.getNextNode(*nodeList[localIP+":"+localPort])

	port, _ := strconv.Atoi(son.port)

	sonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(son.ipAdr),
	}

	sendNodeDieReplicateRequest(I_AM_YOUR_FATHER, KVStore, &sonAddr)

	grandson := consistent.getNextNode(son)
	port, _ = strconv.Atoi(grandson.port)

	grandsonAddr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(grandson.ipAdr),
	}
	sendNodeDieReplicateRequest(I_AM_YOUR_GRANDFATHER, KVStore, &grandsonAddr)
}
