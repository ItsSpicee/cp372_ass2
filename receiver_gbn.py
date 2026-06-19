import socket
import random
from packet import Packet

HOST = "localhost"
PORT = 6969
INPUT_BUFFER_SIZE = 2048
LOSS_RATE = 0.0  # Change to 0.1, 0.2, 0.3 for testing

def setup_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    print(f"GBN Receiver listening on {HOST}:{PORT}")
    return sock

def send_ack(sock, addr, seq_num):
    ack_pkt = Packet(seq_num=0, ack_num=seq_num, ptype=Packet.TYPE_ACK)
    sock.sendto(ack_pkt.to_bytes(), addr)
    print(f"Sent cumulative ACK up to {seq_num}")

def receiver_loop(sock):
    expected_seq = 0
    file_handle = None
    file_name = None

    while True:
        data, addr = sock.recvfrom(INPUT_BUFFER_SIZE)

        # Simulate packet loss
        if random.random() < LOSS_RATE:
            print("Simulated packet loss!")
            continue

        # Parse packet (handle corruption)
        try:
            pkt = Packet.from_bytes(data)
        except ValueError as e:
            print(f"Bad Packet: {e}")
            continue

        print(f"Received: seq={pkt.seq_num}, type={pkt.ptype}")

        if pkt.seq_num == expected_seq:
            # In-order packet — process by type
            if pkt.ptype == Packet.TYPE_START:
                file_name = pkt.payload.decode()
                print(f"Starting transfer: '{file_name}'")
                file_handle = open(file_name, "wb")

            elif pkt.ptype == Packet.TYPE_DATA:
                if file_handle is None:
                    print("DATA before START, ignoring")
                    continue
                file_handle.write(pkt.payload)
                print(f"Wrote {len(pkt.payload)} bytes")

            elif pkt.ptype == Packet.TYPE_END:
                if file_handle:
                    file_handle.close()
                    file_handle = None
                print(f"Transfer complete: '{file_name}'")
                send_ack(sock, addr, pkt.seq_num)
                expected_seq = 0
                file_name = None
                continue  # Wait for next transfer

            # Send ACK and advance expected sequence
            send_ack(sock, addr, pkt.seq_num)
            expected_seq += 1

        else:
            # Out-of-order — drop and re-ACK last valid
            print(f"Out-of-order: got {pkt.seq_num}, expected {expected_seq}")
            if expected_seq > 0:
                send_ack(sock, addr, expected_seq - 1)
            # If expected_seq == 0, nothing valid received yet — don't ACK

def main():
    sock = setup_socket()
    try:
        receiver_loop(sock)
    except KeyboardInterrupt:
        print("\nShutting down receiver...")
    finally:
        sock.close()

if __name__ == "__main__":
    main()