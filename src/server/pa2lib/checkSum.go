package pa2lib

import "hash/crc32"

// Get the CRC-32 IEEE checksum of the message ID and payload
//
// Arguments:
//     messageID: unique ID of the message
//     payload:   payload of the message
// Returns:
//     CRC-32 of the message ID concatenated with the payload
func getChecksum(messageID []byte, payload []byte) uint64 {
	return uint64(crc32.ChecksumIEEE(append(messageID, payload...)))
}

