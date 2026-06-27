'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: sender_gbn.py
Description: Go-Back-N sender (Part C) with the parallel multi-file transfer
             bonus. A single file is sent using a sliding window: several packets
             are in flight at once, a single timer covers the oldest unacked
             packet, and a timeout retransmits the whole window. For the bonus,
             multiple files can be sent at the same time over one UDP socket,
             each by its own thread, distinguished by the packet's file_id.
Capabilities:
    - Go-Back-N sliding window with cumulative ACKs and one base timer
    - Retransmit the full window on timeout
    - Transfer 1..255 files concurrently over a single socket (parallel bonus)
    - Optionally simulate corruption of incoming ACKs (bonus testing)

Authors:
    Obeidi, Bassil
    Barghouti, Alaa
    Ozog, Philip
    Soja, Max
    Yamin, Noah
'''

import socket
import os
import time
import select
import random
import argparse
import threading
from packet import Packet, HEADER_SIZE

# Receiver location and protocol constants.
SERVER_ADDRESS = "localhost"
SERVER_PORT = 6970
TIMEOUT = 0.3            # seconds before the base packet's timer fires
CHUNK_SIZE = 1024        # bytes of file content per DATA packet
INPUT_BUFFER_SIZE = 2048
WINDOW_SIZE = 8          # max number of unacknowledged packets in flight
MAX_SEQ = 256            # sequence numbers wrap modulo this value

class InvalidFileName(Exception):
    pass

def setup_connection():
    """Create and return the UDP socket used for sending."""
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def check_file_name(file_name):
    """Reject file names without an extension (must contain a '.')."""
    if '.' not in os.path.basename(file_name):
        raise InvalidFileName("File must have an extension")

def send_file_gbn(sock, sock_lock, file_name, server_addr, corruption_rate, file_id, ack_states):
    """
    Transfer a single file using Go-Back-N. Run one of these per file (each in
    its own thread for the parallel bonus); all threads share one socket guarded
    by sock_lock, and each reads its ACKs from its own slot in ack_states keyed
    by file_id. Maintains base/nextseqnum, sends while the window has room, and
    retransmits the whole window on timeout.
    """
    retransmission_count = 0

    base = 0              # oldest unacknowledged sequence number
    nextseqnum = 0        # next sequence number to assign
    buffer = {}           # seq_num -> sent Packet, kept until acknowledged
    timer_start = None    # wall-clock time the base timer was started
    eof_reached = False
    started = False       # whether the START packet has been sent yet

    # This file's shared ACK state (filled by the ack_receiver thread).
    state = ack_states[file_id]
    cond = state['condition']

    tag = f"[file_id={file_id}]"

    with open(file_name, "rb") as f:

        # Keep going until everything sent has been acknowledged.
        while base != nextseqnum or not eof_reached:

            # Send new packets while window allows
            while ((nextseqnum - base) % MAX_SEQ) < WINDOW_SIZE:
                if not started:
                    # First packet is START, carrying the file name.
                    chunk = os.path.basename(file_name).encode()
                    ptype = Packet.TYPE_START
                    started = True
                elif not eof_reached:
                    # Read the next file chunk; an empty read means EOF -> END.
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        eof_reached = True
                        chunk = b''
                        ptype = Packet.TYPE_END
                    else:
                        ptype = Packet.TYPE_DATA
                else:
                    break  # EOF reached, END already sent

                # Build and send the packet, then cache it for possible resend.
                pkt = Packet(seq_num=nextseqnum, ptype=ptype, file_id=file_id, payload=chunk)
                with sock_lock:
                    sock.sendto(pkt.to_bytes(), server_addr)
                buffer[nextseqnum] = pkt

                print(f"{tag} Sent packet {nextseqnum} (type={ptype})")

                # Start the timer when sending the first packet of the window.
                if base == nextseqnum:
                    timer_start = time.time()

                nextseqnum = (nextseqnum + 1) % MAX_SEQ

            # Wait briefly to be notified of ACKs (or to re-check the timeout).
            with cond:
                cond.wait(timeout=0.01)

            # Drain any ACKs the ack_receiver thread has queued for us.
            with state['lock']:
                acks = list(state['ack_queue'])
                state['ack_queue'].clear()

            for ack_num in acks:
                # Only act on ACKs that fall inside the current window.
                if ((ack_num - base) % MAX_SEQ) < WINDOW_SIZE:
                    new_base = (ack_num + 1) % MAX_SEQ
                    print(f"{tag} Cumulative ACK up to {ack_num}")

                    # Free acked packets (with wraparound)
                    seq = base
                    while seq != new_base:
                        buffer.pop(seq, None)
                        seq = (seq + 1) % MAX_SEQ

                    base = new_base

                    # Stop the timer if the window is now empty, else restart it.
                    if base == nextseqnum:
                        timer_start = None
                    else:
                        timer_start = time.time()

            # Check timeout: if the base packet hasn't been acked in time,
            # go back and retransmit every packet still in the window.
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
    """
    Shared ACK-demultiplexing thread. Reads every incoming ACK from the socket
    and routes it to the correct file's state (by file_id), waking that file's
    sender thread. Runs until stop_event is set. This is what lets many
    concurrent Go-Back-N transfers share a single UDP socket.
    """
    while not stop_event.is_set():
        # Poll with a short timeout so we can notice stop_event promptly.
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
                # Route the ACK to the sender thread responsible for this file.
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
            # Corrupted ACK failed the checksum; ignore and keep going.
            print(f"Bad Packet: {e}")
            continue

def user_loop(sock, corruption_rate, host, port):
    """
    Interactive loop: ask how many files to send and their names, then transfer
    them all concurrently. One sender thread runs send_file_gbn per file, and a
    single ack_receiver thread feeds ACKs back to each by file_id.
    """
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
            # Validate every file exists before starting any transfer.
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

            # Build one ACK-state slot per file (lock + condition + ACK queue).
            ack_states = {}
            for fid in range(n):
                lock = threading.Lock()
                ack_states[fid] = {
                    'lock': lock,
                    'condition': threading.Condition(lock),
                    'ack_queue': [],
                }

            stop_event = threading.Event()

            # Start the shared ACK receiver before any sender thread.
            ack_thread = threading.Thread(
                target=ack_receiver,
                args=(sock, corruption_rate, ack_states, stop_event),
                daemon=True
            )
            ack_thread.start()

            # One Go-Back-N sender thread per file, all sharing the socket.
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

            # All files done: stop the ACK receiver.
            stop_event.set()
            ack_thread.join(timeout=1.0)

            print(f"All {n} file transfer(s) complete!")

        except InvalidFileName as e:
            print(e)
        except KeyboardInterrupt:
            print("\nExiting...")
            break

def _rate_arg(x):
    """argparse helper: ensure a probability argument is within [0.0, 1.0]."""
    x = float(x)
    if not 0.0 <= x <= 1.0:
        raise argparse.ArgumentTypeError("Rate must be between 0.0 and 1.0")
    return x

def parse_args():
    """Parse command-line options (rates plus optional host/port override)."""
    parser = argparse.ArgumentParser(description="Pure Go-Back-N UDP Sender")
    parser.add_argument("--loss-rate", type=_rate_arg, default=0.0,
                         help="Accepted for CLI uniformity with the receiver; "
                              "packet loss is simulated on the receiver side only.")
    parser.add_argument("--corruption-rate", type=_rate_arg, default=0.0)
    parser.add_argument("--host", type=str, default=SERVER_ADDRESS)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    return parser.parse_args()

def main():
    """Entry point: create the socket and run the interactive send loop."""
    args = parse_args()
    print("Setting up Pure Go-Back-N UDP Sender...")
    sender_socket = setup_connection()
    try:
        user_loop(sender_socket, args.corruption_rate, args.host, args.port)
    finally:
        sender_socket.close()

if __name__ == "__main__":
    main()
