import socket
import os

import packet

SERVER_ADDRESS = "localhost"
SERVER_PORT = 6969
TIMEOUT = 1.0 
BUFFER_SIZE = 2048
LOSS_RATE = 0.0  # Change to 0.1, 0.2, 0.3 for testing

if __name__ == "__main__":
    main() 

def setup_connection():
    sock = socket.socket(sock.AF_INET, sock.SOCK_DGRAM)
    sock.settimeout(TIMEOUT) 

    return sock

def send_ack(sock, addr, seq_num):
    """Send an ACK for the given seq_num."""
    ack_pkt = Packet(seq_num=0, ack_num=seq_num, ptype=Packet.TYPE_ACK)
    sock.sendto(ack_pkt.to_bytes(), addr)
    print(f"Sent ACK for seq={seq_num}")

def receiver_loop(sock):
    expected_seq_number = 0
    file_name = None
    file_handle = None
    transfer_active = False

    while True:

        data, address = sock.recvfrom(BUFFER_SIZE)

        # Simulate packet loss
        if random.random() < LOSS_RATE:
            print("Simulated packet loss!")
            continue

        try:
            pkt = Packet.from_bytes(data)
        except ValueError as e:
            print(f"Corrupted packet, discarding: {e}")
            # Don't ACK corrupted packets — sender will timeout and retransmit
            continue
        
        print(f"Received: seq={pkt.seq_num}, type={pkt.ptype}")

        # === TYPE_START: Begin file transfer ===
        if pkt.ptype == Packet.TYPE_START:
            file_name = pkt.payload.decode()
            print(f"Starting transfer: '{file_name}'")
            
            # Create/truncate file
            file_handle = open(file_name, "wb")
            transfer_active = True

            # ACK the START packet
            send_ack(sock, addr, pkt.seq_num)
            
            # Set expected_seq to the NEXT sequence number
            expected_seq = 1 - pkt.seq_num  # Flip: 0→1 or 1→0

        # === TYPE_DATA: File chunk ===
        elif pkt.ptype == Packet.TYPE_DATA:
            if not transfer_active:
                print("DATA before START, ignoring")
                continue
            
            if pkt.seq_num == expected_seq:
                # In-order packet — write to file
                file_handle.write(pkt.payload)
                print(f"Wrote {len(pkt.payload)} bytes")
                
                # ACK it
                send_ack(sock, addr, pkt.seq_num)
                
                # Flip expected sequence number
                expected_seq = 1 - expected_seq
                
            else:
                # Duplicate packet (sender didn't get our ACK, retransmitted)
                print(f"Unexpected seq={pkt.seq_num}, expected={expected_seq}")
                print("Duplicate packet — re-sending ACK for last valid packet")
                
                # CRITICAL: Re-send ACK for the LAST VALID packet
                # This is what the sender is waiting for!
                last_valid = 1 - expected_seq
                send_ack(sock, addr, last_valid)
        
        # === TYPE_END: Finish transfer ===
        elif pkt.ptype == Packet.TYPE_END:
            if not transfer_active:
                print("END before START, ignoring")
                continue
            
            # Close file
            if file_handle:
                file_handle.close()
                file_handle = None
            
            print(f"Transfer complete: '{file_name}'")
            
            # ACK the END packet
            send_ack(sock, addr, pkt.seq_num)
            
            # Reset for next transfer
            transfer_active = False
            file_name = None
            expected_seq = 0  # Reset for next file
        
        # === TYPE_ACK: Shouldn't receive ACKs ===
        elif pkt.ptype == Packet.TYPE_ACK:
            print("Received ACK on receiver? Ignoring.")





def main():
    print("Setting up UDP Socket...")
    
    receiver_socket = setup_connection()
    receiver_loop(receive_socket)
    receiver_socket.close()