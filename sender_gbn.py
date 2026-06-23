import socket
import os
import time
import select
import random
import argparse
from packet import Packet, HEADER_SIZE

SERVER_ADDRESS = "localhost"
SERVER_PORT = 6970
TIMEOUT = 0.3
CHUNK_SIZE = 1024
INPUT_BUFFER_SIZE = 2048
WINDOW_SIZE = 8
MAX_SEQ = 256

retransmission_count = 0

class InvalidFileName(Exception):
    pass

def setup_connection():
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def check_file_name(file_name):
    if '.' not in os.path.basename(file_name):
        raise InvalidFileName("File must have an extension")

def send_file_gbn(sock, file_name, server_addr, corruption_rate):
    global retransmission_count
    retransmission_count = 0

    base = 0
    nextseqnum = 0
    buffer = {}
    timer_start = None
    eof_reached = False
    started = False  # tracks whether START packet has been queued

    with open(file_name, "rb") as f:

        while base != nextseqnum or not eof_reached:

            # Send new packets while window allows
            while ((nextseqnum - base) % MAX_SEQ) < WINDOW_SIZE:
                if not started:
                    chunk = file_name.encode()
                    ptype = Packet.TYPE_START
                    started = True
                elif not eof_reached:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        eof_reached = True
                        chunk = b''
                        ptype = Packet.TYPE_END
                    else:
                        ptype = Packet.TYPE_DATA
                else:
                    break  # EOF reached, END already sent

                pkt = Packet(seq_num=nextseqnum, ptype=ptype, payload=chunk)
                sock.sendto(pkt.to_bytes(), server_addr)
                buffer[nextseqnum] = pkt

                print(f"Sent packet {nextseqnum} (type={ptype})")

                if base == nextseqnum:
                    timer_start = time.time()

                nextseqnum = (nextseqnum + 1) % MAX_SEQ

            # Check for ACKs (non-blocking)
            ready, _, _ = select.select([sock], [], [], 0.01)

            if ready:
                try:
                    data, addr = sock.recvfrom(INPUT_BUFFER_SIZE)

                    # Simulate bit-level corruption of the ACK
                    if len(data) > HEADER_SIZE and random.random() < corruption_rate:
                        data = bytearray(data)
                        idx = random.randint(HEADER_SIZE, len(data) - 1)
                        bit = random.randint(0, 7)
                        data[idx] ^= (1 << bit)
                        data = bytes(data)

                    ack_pkt = Packet.from_bytes(data)

                    ack_num = ack_pkt.ack_num
                    if ack_pkt.ptype == Packet.TYPE_ACK and ((ack_num - base) % MAX_SEQ) < WINDOW_SIZE:
                        new_base = (ack_num + 1) % MAX_SEQ
                        print(f"Cumulative ACK up to {ack_num}")

                        # Free acked packets (with wraparound)
                        seq = base
                        while seq != new_base:
                            buffer.pop(seq, None)
                            seq = (seq + 1) % MAX_SEQ

                        base = new_base

                        if base == nextseqnum:
                            timer_start = None
                        else:
                            timer_start = time.time()

                except ValueError as e:
                    print(f"Bad Packet: {e}")
                    continue  # Corrupted ACK, ignore

            # Check timeout
            if timer_start is not None and time.time() - timer_start > TIMEOUT:
                seq = base
                count = 0
                while seq != nextseqnum:
                    count += 1
                    if seq in buffer:
                        sock.sendto(buffer[seq].to_bytes(), server_addr)
                    seq = (seq + 1) % MAX_SEQ
                print(f"Timeout! Retransmitting {count} packet(s) from base={base}")
                retransmission_count += count
                timer_start = time.time()

    print("File transfer complete!")
    print(f"RETRANSMISSIONS: {retransmission_count}")

def user_loop(sock, corruption_rate, host, port):
    server_addr = (host, port)

    while True:
        try:
            file_name = input("Enter the file name to be transferred (with extension): ")
        except EOFError:
            print("\nExiting...")
            break

        try:
            check_file_name(file_name)

            if not os.path.exists(file_name):
                print(f"File '{file_name}' not found.")
                continue

            send_file_gbn(sock, file_name, server_addr, corruption_rate)

        except InvalidFileName as e:
            print(e)
        except KeyboardInterrupt:
            print("\nExiting...")
            break

def parse_args():
    parser = argparse.ArgumentParser(description="Pure Go-Back-N UDP Sender")
    parser.add_argument("--loss-rate", type=float, default=0.0,
                         help="Accepted for CLI uniformity with the receiver; "
                              "packet loss is simulated on the receiver side only.")
    parser.add_argument("--corruption-rate", type=float, default=0.0)
    parser.add_argument("--host", type=str, default=SERVER_ADDRESS)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    return parser.parse_args()

def main():
    args = parse_args()
    print("Setting up Pure Go-Back-N UDP Sender...")
    sender_socket = setup_connection()
    try:
        user_loop(sender_socket, args.corruption_rate, args.host, args.port)
    finally:
        sender_socket.close()

if __name__ == "__main__":
    main()
