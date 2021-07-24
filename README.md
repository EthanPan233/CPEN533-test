#  Design of Milestone1

## Group member:
Runze Wang, runzw

Li Ju, LiJu21

Yuxuan Pan, EthanPan233

## Design

### Bootstrap
1. A node program takes two arguments: a list containing all the other serve, and a port number it should be listening on. The program can be run by command:
```
go run ./src/server/dht-server.go /path/to/peers.txt <port number>
```
2. After a node starts, it will call ```initNodeList()```, where it reads lines from peers.txt and create a list with all the other nodes, called ```nodeList```.
3. A ```nodeList``` is an array of ```NodeVal```, which represents a single node other than it self and is defined as follow:
```go
type NodeVal struct {
	ipAdr      string
	port       string
	isOn       bool
	membership string
	time       uint64
}
```
4. Now we can say this node has full information of other nodes at the beginning, though some of them may fail to start.


### Gossip
1. Each node has a node list, which contains all the other node that are alive to the best of its knowledge.
2. New command code added: GET_MEMBERSHIP_LIST  = 0x22, a server receives this command will return its own node list.
3. The node list is maintained by gossipping.
4. The way a node does gossip is that it randomly chooses several nodes as targets, and sends command 0x22 to them sequentially. If it failed to receive response from a target, it would retry several. If all retries failed, the target would be regarded as dead, and this node would delete the target from its node list.
4. If a node got the response, which is a node list, from its target, it would merge two node lists.
5. The frequency of gossipping is set to a constant.
6. Each node has an initialized node list.

### Route

1. We decided to use multiple threads to handle different messages including ordinary KV requests, gossip and replication requests.

2. After a client sending a request to a certain node, first the node will check whether the command exists in the cache. If it does, the node will directly reply to the client.

3. If not, the node will handle the request according to the command unmarshaled from message payload.

4. The commands could be handled as KVRequest are shown as follows.

   ```go
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
   	HELLO = 0x40
   )
   ```

5. When the node receives **PUT**, **GET** or **REMOVE**, it will check the hash ring to make sure whether the key should be stored or has already been stored inside it. f not,  it will find the correct node and send client address (ip + port) to that node. When the correct node receive the request, it will handle the request according and directly send back to the client.

6. When the node receives **PUT_FORWARD**, **GET_FORWARD** or **REMOVE_FORWARD**, it means that the current node receives put request forwarding from another node. It will directly put/get/remove kv into storage, then the storage replication will be done once via sending requests including command from 0x26 to 0x2b if the operation will lead to changes to the original kv storage.

7. When the node receives **GRANDSON_DIED** or **SON_DIED**, it will handle the replication on the situation that a node just died. 

8. When the node receives **HELLO**, it means that there is a new node just starts.

### Hashring

1. A struct named Consistent is created to realized functionalities of Hash ring. Consistent struct includes a map data structure mapping differents hash keys to each node alive.
2. Hash key for each node is created by hashing corresponding ip and port.
3. The file includes other related functions such as generateHashRing, getNode, addNodetoHashring, removeNodefromHashring, etc.

### Data Replication
For data replication, we introduce two new concepts:

1. Father, son, grandfather, and grandson node

The **father** of a node is the precursor node of it on the hash ring, and the **son** of a node is the succesor node of it on the hash ring. Similarly, The **grandfather** of a node is the precursor node of its father node on the hash ring, and the **son** of a node is the succesor node of its son node on the hash ring.

2. A new proto called `RepRequest` and `KVPair`
```
message RepRequest{
    uint32 command = 1;

    message KVPair{
      bytes key = 1;
      bytes value = 2;
      int32 version = 3;
    }

    repeated KVPair kvs = 2;
    int32 check = 3;
}
```
`KVPair` represents a key-value pair as well as its version.

`RepRequest` is designed specifically for conveying replication information between two nodes, which has a `repeated KVPair kvs` to contain the whole k-v storage of a node at one time.
#### Replication in normal cases
After a node receives `PUT(_FORWARD)` and `REMOVE(_FORWARD)` requests, its K-V storage will change, which makes it necessary to have its two replicates change as well. Then, the node will change the command code in original requests to `PUT_REPLICATE` and `REMOVE_REPLICATE`, and send modified requests(type: KVRequest) to its son and grandson node to inform them of the change, which will change its replicate of the node after receiving the above two requests. 
#### Replication in node fail/rejoin cases
`fail` : If node A finds that node B fails during gossip, it will send four messages to the father, son, grandfather, and grandson of the dead node to notify them of the failure. After a node receives such message, it will replicate data accordingly to make sure every node's storage has exact two copies when node number is larger than 2.

`rejoin` : As described in gossip part, after a node rejoin the membership, it will immediately send a `hello` message to everyone in its nodelist. After a node receiving the message, it will do the following:
1. Check its relation with the new node, and if they are 'relatives'(it means current node is the father, son, grandfather, or grandson node of the new node), move to step 2, otherwise it will do nothing.
2. Inform the new node of the relation through replicate requests, and pass necessary data to it (For example, if a node finds that the new node is its son, it will send a copy of its kv storage through requests)
