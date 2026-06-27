'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: sender_stopwait.py
Description: Stop-and-Wait sender (Part A). Reads a file, splits it into 1024-byte
             packets, and sends them one at a time over UDP, waiting for an ACK
             after each packet before sending the next. A packet is retransmitted
             whenever its ACK does not arrive before the timeout.
Capabilities:
    - Notify the receiver of an incoming file (START packet)
    - Send file content one packet at a time and wait for each ACK
    - Retransmit on timeout using 1-bit alternating sequence numbers
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
import random
import argparse
from packet import Packet, HEADER_SIZE

# Receiver location and protocol constants.
SERVER_ADDRESS = "localhost"
SERVER_PORT = 6969
TIMEOUT = 0.3        # seconds to wait for an ACK before retransmitting
CHUNK_SIZE = 1024    # bytes of file content per DATA packet
MAX_RETRIES = 20     # give up on a packet after this many consecutive timeouts

# Running count of retransmissions, reported at the end of a transfer.
retransmission_count = 0


class InvalidFileName(Exception):
    pass

def setup_connection():
    """Create the UDP socket and apply the default ACK timeout."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT)

    return sock

def check_file_name(file_name):
    """Reject file names without an extension (must contain a '.')."""
    if '.' not in os.path.basename(file_name):
        raise InvalidFileName("File must have an extension")

def notify_server(sock, file_name, server_addr, seq_num, corruption_rate):
    """Send a START packet (carrying the file name) and wait for its ACK."""
    start_pkt = Packet(seq_num=seq_num, ptype=Packet.TYPE_START, payload=file_name.encode())

    return send_and_wait_for_ack(sock, start_pkt, server_addr, seq_num, corruption_rate)


def send_and_wait_for_ack(sock, packet, server_addr, expected_ack_num, corruption_rate):
    """
    Core Stop-and-Wait step: send one packet, then wait for the matching ACK.
    Retransmits the packet on each timeout (up to MAX_RETRIES). Returns True
    once the expected ACK is received, or False if retries are exhausted.
    """
    global retransmission_count
    retries = 0

    while True:
        # (Re)send the packet and arm the timeout.
        sock.sendto(packet.to_bytes(), server_addr)
        sock.settimeout(TIMEOUT)

        while True:
            try:
                data, addr = sock.recvfrom(2048)

                # Simulate bit-level corruption of the ACK
                if len(data) > HEADER_SIZE and random.random() < corruption_rate:
                    data = bytearray(data)
                    idx = random.randint(HEADER_SIZE, len(data) - 1)
                    bit = random.randint(0, 7)
                    data[idx] ^= (1 << bit)
                    data = bytes(data)

                try:
                    ack_pkt = Packet.from_bytes(data)
                except ValueError as e:
                    # Corrupted/short ACK; ignore it and keep waiting for a good one.
                    print(f"Bad packet received: {e}")
                    continue  # Stay in inner loop, keep waiting

                # Correct ACK for the packet we sent -> success.
                if ack_pkt.ptype == Packet.TYPE_ACK and ack_pkt.ack_num == expected_ack_num:
                    return True

                # Wrong ACK — keep waiting, don't count as retry
                print(f"Stale ACK (ack_num={ack_pkt.ack_num}), still waiting...")

            except socket.timeout:
                # No ACK in time: count a retransmission and resend (outer loop).
                retries += 1
                retransmission_count += 1
                print(f"Timeout #{retries}, retransmitting...")
                if retries >= MAX_RETRIES:
                    print("Max retries reached, giving up.")
                    return False
                break  # Break inner, retransmit in outer

def send_file(sock, file_name, server_addr, corruption_rate):
    """Send the actual file data in chunks."""
    global retransmission_count
    retransmission_count = 0

    seq_num = 0  # Start with 0, will flip to 1 and back

    # Step 1: Notify server (START)
    if not notify_server(sock, file_name, server_addr, seq_num, corruption_rate):
        print("Failed to start transfer")
        return
    seq_num = 1 - seq_num  # Flip: 0 → 1

    # Step 2: Send file chunks, one packet at a time.
    with open(file_name, "rb") as file:
        while True:
            chunk = file.read(CHUNK_SIZE)
            if not chunk:
                break  # End of file

            data_pkt = Packet(
                seq_num=seq_num,
                ptype=Packet.TYPE_DATA,
                payload=chunk
            )

            # Block until this chunk is acknowledged (or the transfer fails).
            if not send_and_wait_for_ack(sock, data_pkt, server_addr, seq_num, corruption_rate):
                print("Transfer failed")
                return

            seq_num = 1 - seq_num  # Flip for next packet: 0<->1

    # Step 3: Send END packet to signal completion.
    end_pkt = Packet(
        seq_num=seq_num,
        ptype=Packet.TYPE_END,
        payload=b''  # Empty payload
    )
    if not send_and_wait_for_ack(sock, end_pkt, server_addr, seq_num, corruption_rate):
        print("Unable to notify server of transfer completion.")
    else:
        print("File transfer complete. :)")

    print(f"RETRANSMISSIONS: {retransmission_count}")


def user_loop(sock, corruption_rate, host, port):
    """Prompt the user for file names and transfer each one until EOF/Ctrl+C."""
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

            send_file(sock, file_name, server_addr, corruption_rate)

        except InvalidFileName as e:
            print(e)
        except KeyboardInterrupt:
            print("\nExiting...")
            break


def parse_args():
    """Parse command-line options (rates plus optional host/port override)."""
    parser = argparse.ArgumentParser(description="Stop-and-Wait UDP Sender")
    parser.add_argument("--loss-rate", type=float, default=0.0,
                         help="Accepted for CLI uniformity with the receiver; "
                              "packet loss is simulated on the receiver side only.")
    parser.add_argument("--corruption-rate", type=float, default=0.0)
    parser.add_argument("--host", type=str, default=SERVER_ADDRESS)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    return parser.parse_args()


def main():
    """Entry point: create the socket and run the interactive send loop."""
    args = parse_args()
    print("Setting up UDP Socket...")
    sender_socket = setup_connection()
    try:
        user_loop(sender_socket, args.corruption_rate, args.host, args.port)
    finally:
        sender_socket.close()

if __name__ == "__main__":
    main()
