package pa2lib

import (
	"encoding/binary"
	"math"
	"math/rand"
	"time"
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


