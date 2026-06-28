'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: packet.py
Description: Shared packet format used by every sender and receiver in this
             assignment. Defines the fixed-size header, the packet types, and
             an 8-bit checksum used to detect corruption. This module has no
             main() and is never run on its own; it is imported by the four
             sender/receiver scripts.
Capabilities:
    - Pack a packet (header + payload) into raw bytes for sending over UDP
    - Parse raw bytes back into a Packet and verify its checksum
    - Detect corrupted packets by raising ValueError on a checksum mismatch

Authors:
    Obeidi, Bassil
    Barghouti, Alaa
    Ozog, Philip
    Soja, Max
    Yamin, Noah
'''

import struct

# Header layout, network byte order (big-endian):
#   I = seq_num (4 bytes), I = ack_num (4 bytes), B = ptype (1 byte),
#   B = file_id (1 byte), I = payload_len (4 bytes), B = chksum (1 byte)
# TEMP excludes the checksum field (used while computing the checksum); FULL
# includes it (used for the packet actually sent on the wire).
HEADER_FORMAT_TEMP = '!IIBBI'
HEADER_FORMAT_FULL = '!IIBBIB'

# Total header size in bytes (15), computed from the full format string.
HEADER_SIZE =  struct.calcsize(HEADER_FORMAT_FULL)

class Packet:

    # Packet type codes carried in the 1-byte ptype header field.
    TYPE_DATA = 0    # a chunk of file content
    TYPE_ACK = 1     # acknowledgment (ack_num holds the seq being acked)
    TYPE_START = 2   # notifies the receiver a file transfer is beginning
    TYPE_END = 3     # signals the file transfer is complete

    # Human-readable names, used only for debug printing / __repr__.
    TYPE_NAMES = {
        0: 'DATA',
        1: 'ACK',
        2: 'START',
        3: 'END'
    }

    def __init__(self, seq_num = 0, ack_num = 0, ptype=TYPE_DATA, file_id=0, payload=b'', chksum = 0):
        self.seq_num = seq_num
        self.ack_num = ack_num
        self.ptype = ptype
        self.file_id = file_id        # which file this packet belongs to (parallel transfer)
        self.payload = payload
        self.chksum = chksum

    def compute_checksum(self):
        """
        Compute the 8-bit checksum over the header (excluding the checksum
        field) and the payload. The byte total is folded into 8 bits by
        repeatedly adding any overflow back in, then inverted with XOR 0xFF.
        Returns the checksum as an integer in the range 0-255.
        """
        # Pack every field except the checksum itself.
        temp_header = struct.pack(

            HEADER_FORMAT_TEMP,
            self.seq_num,
            self.ack_num,
            self.ptype,
            self.file_id,
            len(self.payload)

            )
        total = sum(temp_header) + sum(self.payload)

        # Fold the running total down into a single byte (8 bits).
        while total > 0xFF:
            carry = total >> 8        # the overflow bits
            total = (total & 0xFF) + carry  # add them back to the low 8 bits


        return total ^ 0xFF


    def to_bytes(self):
        """
        Serialize this packet into raw bytes ready to send over UDP. The
        checksum is computed last (over the final field values) and packed
        into the header, which is then concatenated with the payload.
        """
        self.chksum = self.compute_checksum()

        header = struct.pack(
            HEADER_FORMAT_FULL,
            self.seq_num,
            self.ack_num,
            self.ptype,
            self.file_id,
            len(self.payload),
            self.chksum

        )

        return header + self.payload

    @classmethod
    def from_bytes(cls, raw_data):
        """
        Parse raw bytes received over UDP back into a Packet object and verify
        its checksum. Raises ValueError if the data is too short or the
        checksum does not match (i.e. the packet was corrupted), which the
        senders/receivers treat as "discard and let the timeout recover it".
        """
        if len(raw_data) < HEADER_SIZE:
            raise ValueError("Data too short for header.")

        header = raw_data[:HEADER_SIZE]

        # Unpack the header fields.
        seq_num, ack_num, ptype, file_id, payload_len, chksum = struct.unpack(HEADER_FORMAT_FULL, header)

        # Guard against an absurd declared payload length (e.g. from corruption).
        MAX_PAYLOAD = 1024
        if payload_len > MAX_PAYLOAD:
            raise ValueError(f"payload_len {payload_len} exceeds maximum {MAX_PAYLOAD}.")

        if len(raw_data) < HEADER_SIZE + payload_len:
            raise ValueError("Data too short for declared payload length.")

        payload = raw_data[HEADER_SIZE:HEADER_SIZE + payload_len]

        # Build the packet from the extracted fields.
        packet = cls(seq_num, ack_num, ptype, file_id, payload, chksum)

        # Recompute the checksum and compare; a mismatch means the packet was corrupted.
        if packet.compute_checksum() != chksum:
            raise ValueError("Checksum mismatch — packet corrupted!")

        return packet


    def __repr__(self):
        """Compact debug representation, e.g. Packet(seq=3, ack=0, type=DATA, ...)."""
        return (f"Packet(seq={self.seq_num}, ack={self.ack_num}, "
                f"type={self.TYPE_NAMES.get(self.ptype, 'UNKNOWN')}, "
                f"file_id={self.file_id}, "
                f"payload_len={len(self.payload)}, chksum={self.chksum})")
