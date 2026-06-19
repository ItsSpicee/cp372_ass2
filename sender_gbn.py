import socket
import os
import time
import select
from packet import Packet

SERVER_ADDRESS = "localhost"
SERVER_PORT = 6969
TIMEOUT = 1.0
CHUNK_SIZE = 1024
INPUT_BUFFER_SIZE = 2048
WINDOW_SIZE = 4

class InvalidFileName(Exception):
    pass

def setup_connection():
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def check_file_name(file_name):
    parts = file_name.split(".")
    if len(parts) != 2:
        raise InvalidFileName("File name must be of form xxx.yy")

def send_file_gbn(sock, file_name, server_addr):
    base = 0
    nextseqnum = 0
    buffer = {}          # Only current window cached
    timer_start = None
    eof_reached = False  # True when f.read() returns empty

    with open(file_name, "rb") as f:

        while base < nextseqnum or not eof_reached:

            # Send new packets while window allows
            while nextseqnum < base + WINDOW_SIZE:
                if nextseqnum == 0:
                    chunk = file_name.encode()
                    ptype = Packet.TYPE_START
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

                nextseqnum += 1

            # Check for ACKs (non-blocking)
            ready, _, _ = select.select([sock], [], [], 0.01)

            if ready:
                try:
                    data, addr = sock.recvfrom(INPUT_BUFFER_SIZE)
                    ack_pkt = Packet.from_bytes(data)

                    if ack_pkt.ptype == Packet.TYPE_ACK and ack_pkt.ack_num >= base:
                        new_base = ack_pkt.ack_num + 1
                        print(f"Cumulative ACK up to {ack_pkt.ack_num}")

                        # Free old packets that left the window
                        for seq in range(base, new_base):
                            buffer.pop(seq, None)

                        base = new_base

                        if base == nextseqnum:
                            timer_start = None
                        else:
                            timer_start = time.time()

                except ValueError:
                    print(f"Bad Packet: {e}")
                    continue  # Corrupted ACK, ignore

            # Check timeout
            if timer_start is not None and time.time() - timer_start > TIMEOUT:
                print(f"Timeout! Retransmitting {base} to {nextseqnum - 1}")
                for seq in range(base, nextseqnum):
                    if seq in buffer:
                        sock.sendto(buffer[seq].to_bytes(), server_addr)
                timer_start = time.time()

    print("File transfer complete!")

def user_loop(sock):
    server_addr = (SERVER_ADDRESS, SERVER_PORT)

    while True:
        file_name = input("Enter the file name to be transferred (with extension): ")

        try:
            check_file_name(file_name)

            if not os.path.exists(file_name):
                print(f"File '{file_name}' not found.")
                continue

            send_file_gbn(sock, file_name, server_addr)

        except InvalidFileName as e:
            print(e)
        except KeyboardInterrupt:
            print("\nExiting...")
            break

def main():
    print("Setting up Pure Go-Back-N UDP Sender...")
    sender_socket = setup_connection()
    user_loop(sender_socket)
    sender_socket.close()

if __name__ == "__main__":
    main()