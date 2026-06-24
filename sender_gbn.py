import socket
import os
import time
import select
import random
import argparse
import threading
from packet import Packet, HEADER_SIZE

SERVER_ADDRESS = "localhost"
SERVER_PORT = 6970
TIMEOUT = 0.3
CHUNK_SIZE = 1024
INPUT_BUFFER_SIZE = 2048
WINDOW_SIZE = 8
MAX_SEQ = 256

class InvalidFileName(Exception):
    pass

def setup_connection():
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def check_file_name(file_name):
    if '.' not in os.path.basename(file_name):
        raise InvalidFileName("File must have an extension")

def send_file_gbn(sock, sock_lock, file_name, server_addr, corruption_rate, file_id, ack_states):
    retransmission_count = 0

    base = 0
    nextseqnum = 0
    buffer = {}
    timer_start = None
    eof_reached = False
    started = False

    state = ack_states[file_id]
    cond = state['condition']

    tag = f"[file_id={file_id}]"

    with open(file_name, "rb") as f:

        while base != nextseqnum or not eof_reached:

            # Send new packets while window allows
            while ((nextseqnum - base) % MAX_SEQ) < WINDOW_SIZE:
                if not started:
                    chunk = os.path.basename(file_name).encode()
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

                pkt = Packet(seq_num=nextseqnum, ptype=ptype, file_id=file_id, payload=chunk)
                with sock_lock:
                    sock.sendto(pkt.to_bytes(), server_addr)
                buffer[nextseqnum] = pkt

                print(f"{tag} Sent packet {nextseqnum} (type={ptype})")

                if base == nextseqnum:
                    timer_start = time.time()

                nextseqnum = (nextseqnum + 1) % MAX_SEQ

            # Wait for ACKs or timeout
            with cond:
                cond.wait(timeout=0.01)

            # Process any queued ACKs
            with state['lock']:
                acks = list(state['ack_queue'])
                state['ack_queue'].clear()

            for ack_num in acks:
                if ((ack_num - base) % MAX_SEQ) < WINDOW_SIZE:
                    new_base = (ack_num + 1) % MAX_SEQ
                    print(f"{tag} Cumulative ACK up to {ack_num}")

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

            # Check timeout
            if timer_start is not None and time.time() - timer_start > TIMEOUT:
                seq = base
                count = 0
                while seq != nextseqnum:
                    count += 1
                    if seq in buffer:
                        with sock_lock:
                            sock.sendto(buffer[seq].to_bytes(), server_addr)
                    seq = (seq + 1) % MAX_SEQ
                print(f"{tag} Timeout! Retransmitting {count} packet(s) from base={base}")
                retransmission_count += count
                timer_start = time.time()

    print(f"{tag} File transfer complete!")
    print(f"{tag} RETRANSMISSIONS: {retransmission_count}")

def ack_receiver(sock, corruption_rate, ack_states, stop_event):
    while not stop_event.is_set():
        ready, _, _ = select.select([sock], [], [], 0.05)
        if not ready:
            continue
        try:
            data = sock.recvfrom(INPUT_BUFFER_SIZE)[0]

            # Simulate bit-level corruption of the ACK
            if len(data) > HEADER_SIZE and random.random() < corruption_rate:
                data = bytearray(data)
                idx = random.randint(HEADER_SIZE, len(data) - 1)
                bit = random.randint(0, 7)
                data[idx] ^= (1 << bit)
                data = bytes(data)

            ack_pkt = Packet.from_bytes(data)

            if ack_pkt.ptype == Packet.TYPE_ACK:
                fid = ack_pkt.file_id
                if fid in ack_states:
                    state = ack_states[fid]
                    with state['lock']:
                        state['ack_queue'].append(ack_pkt.ack_num)
                    with state['condition']:
                        state['condition'].notify()
                else:
                    print(f"[ack_receiver] Unknown file_id={fid}, ignoring ACK")

        except ValueError as e:
            print(f"Bad Packet: {e}")
            continue

def user_loop(sock, corruption_rate, host, port):
    server_addr = (host, port)

    while True:
        try:
            n_str = input("Enter the number of files to transfer: ")
            n = int(n_str)
            if n < 1 or n > 255:
                print("Number of files must be between 1 and 255.")
                continue

            file_names = []
            for i in range(n):
                file_names.append(input(f"Enter file {i + 1} to transfer: "))
        except EOFError:
            print("\nExiting...")
            break
        except ValueError:
            print("Invalid number.")
            continue

        try:
            valid = True
            for fname in file_names:
                check_file_name(fname)
                if not os.path.exists(fname):
                    print(f"File '{fname}' not found.")
                    valid = False
                    break
            if not valid:
                continue

            sock_lock = threading.Lock()

            ack_states = {}
            for fid in range(n):
                lock = threading.Lock()
                ack_states[fid] = {
                    'lock': lock,
                    'condition': threading.Condition(lock),
                    'ack_queue': [],
                }

            stop_event = threading.Event()

            ack_thread = threading.Thread(
                target=ack_receiver,
                args=(sock, corruption_rate, ack_states, stop_event),
                daemon=True
            )
            ack_thread.start()

            threads = [
                threading.Thread(
                    target=send_file_gbn,
                    args=(sock, sock_lock, fname, server_addr, corruption_rate, fid, ack_states)
                )
                for fid, fname in enumerate(file_names)
            ]

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            stop_event.set()
            ack_thread.join(timeout=1.0)

            print(f"All {n} file transfer(s) complete!")

        except InvalidFileName as e:
            print(e)
        except KeyboardInterrupt:
            print("\nExiting...")
            break

def _rate_arg(x):
    x = float(x)
    if not 0.0 <= x <= 1.0:
        raise argparse.ArgumentTypeError("Rate must be between 0.0 and 1.0")
    return x

def parse_args():
    parser = argparse.ArgumentParser(description="Pure Go-Back-N UDP Sender")
    parser.add_argument("--loss-rate", type=_rate_arg, default=0.0,
                         help="Accepted for CLI uniformity with the receiver; "
                              "packet loss is simulated on the receiver side only.")
    parser.add_argument("--corruption-rate", type=_rate_arg, default=0.0)
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
