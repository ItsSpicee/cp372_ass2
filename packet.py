import struct

HEADER_FORMAT_TEMP = '!IIbI'
HEADER_FORMAT_FULL = '!IIbIB'

HEADER_SIZE =  struct.calcsize(HEADER_FORMAT_FULL)

class Packet:

    TYPE_DATA = 0
    TYPE_ACK = 1
    TYPE_START = 2
    TYPE_END = 3

    TYPE_NAMES = {
        0: 'DATA',
        1: 'ACK',
        2: 'START',
        3: 'END'
    }

    def __init__(self, seq_num = 0, ack_num = 0, p_type=TYPE_DATA, payload=b'', chksum = 0):
        self.seq_num = seq_num
        self.ack_num = ack_num
        self.p_type = ptype
        self.payload = payload
        self.chksum = chksum
    
    def compute_checksum():
        # Pack everything except chksum
        temp_header = struct.pack(

            HEADER_FORMAT_TEMP, 
            self.seq_num, 
            self.ack_num,
            self.ptype, 
            len(self.payload)

            )
        total = sum(temp_header) + sum(self.payload)

        while total > 0xFF:
            carry = total >> 8        # Get the overflow bits
            total = (total & 0xFF) + carry  # Add them back to the bottom 8 bits

        
        return total ^ 0xFF

        
    def to_bytes(self):

        self.chksum = self.compute_checksum()

        header = struct.pack(
            HEADER_FORMAT_FULL,
            self.seq_num,
            self.ack_num,
            self.p_type,
            len(self.payload),
            self.chksum

        )
        
        return header + self.payload
    
    @classmethod 
    def from_bytes(cls, raw_data):
        if len(raw_data) < HEADER_SIZE:
            raise ValueError("Data too short for header.")
        
        header = raw_data[:HEADER_SIZE]

        #Unpack header
        seq_num, ack_num, ptype, payload_len, chksum = struct.unpack(HEADER_FORMAT_FULL, header)

        payload = raw_data[HEADER_SIZE:HEADER_SIZE + payload_len]

         # Create packet with extracted checksum
        packet = cls(seq_num, ack_num, ptype, payload, chksum)
        
        # Verify checksum
        if packet.compute_checksum() != chksum:
            raise ValueError("Checksum mismatch — packet corrupted!")
        
        return packet


    def __repr__(self):
        return (f"Packet(seq={self.seq_num}, ack={self.ack_num}, "
                f"type={self.TYPE_NAMES.get(self.ptype, 'UNKNOWN')}, "
                f"payload_len={len(self.payload)}, chksum={self.chksum})")
