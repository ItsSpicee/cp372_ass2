import socket
import os
import random
import argparse

from packet import Packet, HEADER_SIZE

SERVER_ADDRESS = "localhost"
SERVER_PORT = 6969
BUFFER_SIZE = 2048

def setup_connection(host=SERVER_ADDRESS, port=SERVER_PORT):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    return sock

def send_ack(sock, addr, seq_num):
    """Send an ACK for the given seq_num."""
    ack_pkt = Packet(seq_num=0, ack_num=seq_num, ptype=Packet.TYPE_ACK)
    sock.sendto(ack_pkt.to_bytes(), addr)
    print(f"Sent ACK for seq={seq_num}")

def receiver_loop(sock, loss_rate, corruption_rate):
    expected_seq = 0
    file_name = None
    file_handle = None
    transfer_active = False

    while True:

        data, addr = sock.recvfrom(BUFFER_SIZE)

        # Simulate packet loss
        if random.random() < loss_rate:
            print("Simulated packet loss!")
            continue

        # Simulate bit-level corruption
        if len(data) > HEADER_SIZE and random.random() < corruption_rate:
            data = bytearray(data)
            idx = random.randint(HEADER_SIZE, len(data) - 1)
            bit = random.randint(0, 7)
            data[idx] ^= (1 << bit)
            data = bytes(data)

        try:
            pkt = Packet.from_bytes(data)
        except ValueError as e:
            print(f"Corrupted packet, discarding: {e}")
            # Don't ACK corrupted packets — sender will timeout and retransmit
            continue

        print(f"Received: seq={pkt.seq_num}, type={pkt.ptype}")

        # === TYPE_START: Begin file transfer ===
        if pkt.ptype == Packet.TYPE_START:
            file_name = os.path.basename(pkt.payload.decode())
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


def parse_args():
    parser = argparse.ArgumentParser(description="Stop-and-Wait UDP Receiver")
    parser.add_argument("--loss-rate", type=float, default=0.0)
    parser.add_argument("--corruption-rate", type=float, default=0.0)
    parser.add_argument("--host", type=str, default=SERVER_ADDRESS)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    return parser.parse_args()


def main():
    args = parse_args()

    print("Setting up UDP Socket...")

    receiver_socket = setup_connection(args.host, args.port)
    receiver_loop(receiver_socket, args.loss_rate, args.corruption_rate)
    receiver_socket.close()


if __name__ == "__main__":
    main()
