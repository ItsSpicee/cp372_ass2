import socket
import os
import re
import random
import argparse
from packet import Packet, HEADER_SIZE

HOST = "localhost"
PORT = 6970
INPUT_BUFFER_SIZE = 2048
MAX_SEQ = 256

def setup_socket(host=HOST, port=PORT):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    print(f"GBN Receiver listening on {host}:{port}")
    return sock

def send_ack(sock, addr, seq_num, file_id):
    ack_pkt = Packet(seq_num=0, ack_num=seq_num, ptype=Packet.TYPE_ACK, file_id=file_id)
    sock.sendto(ack_pkt.to_bytes(), addr)
    print(f"[file_id={file_id}] Sent cumulative ACK up to {seq_num}")

def receiver_loop(sock, loss_rate, corruption_rate):
    # Per-file state keyed by file_id
    file_states = {}

    def get_state(fid):
        if fid not in file_states:
            file_states[fid] = {
                'expected_seq': 0,
                'file_handle': None,
                'file_name': None,
            }
        return file_states[fid]

    try:
        while True:
            data, addr = sock.recvfrom(INPUT_BUFFER_SIZE)

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

            # Parse packet (handle corruption)
            try:
                pkt = Packet.from_bytes(data)
            except ValueError as e:
                print(f"Bad Packet: {e}")
                continue

            fid = pkt.file_id
            tag = f"[file_id={fid}]"
            st = get_state(fid)

            print(f"{tag} Received: seq={pkt.seq_num}, type={pkt.ptype}")

            if pkt.seq_num == st['expected_seq']:
                # In-order packet — process by type
                if pkt.ptype == Packet.TYPE_START:
                    raw_name = os.path.basename(pkt.payload.decode('utf-8', errors='replace'))
                    if not re.fullmatch(r'[a-zA-Z0-9_\-]+(\.[a-zA-Z0-9_]+)+', raw_name):
                        print(f"{tag} Rejected unsafe filename: '{raw_name}'")
                        continue
                    if st['file_handle']:
                        st['file_handle'].close()
                    st['file_name'] = raw_name
                    print(f"{tag} Starting transfer: '{st['file_name']}'")
                    st['file_handle'] = open(st['file_name'], "wb")

                elif pkt.ptype == Packet.TYPE_DATA:
                    if st['file_handle'] is None:
                        print(f"{tag} DATA before START, ignoring")
                        continue
                    st['file_handle'].write(pkt.payload)
                    print(f"{tag} Wrote {len(pkt.payload)} bytes")

                elif pkt.ptype == Packet.TYPE_END:
                    if st['file_handle']:
                        st['file_handle'].close()
                        st['file_handle'] = None
                    print(f"{tag} Transfer complete: '{st['file_name']}'")
                    send_ack(sock, addr, pkt.seq_num, fid)
                    # Reset state for this file_id so it can be reused
                    st['expected_seq'] = 0
                    st['file_name'] = None
                    continue  # Wait for next packet

                # Send ACK and advance expected sequence
                send_ack(sock, addr, pkt.seq_num, fid)
                st['expected_seq'] = (st['expected_seq'] + 1) % MAX_SEQ

            else:
                # Out-of-order — drop and re-ACK last valid
                print(f"{tag} Out-of-order: got {pkt.seq_num}, expected {st['expected_seq']}")
                if st['expected_seq'] > 0:
                    send_ack(sock, addr, (st['expected_seq'] - 1) % MAX_SEQ, fid)
                # If expected_seq == 0, nothing valid received yet — don't ACK

    finally:
        for fid, st in file_states.items():
            if st['file_handle']:
                st['file_handle'].close()

def _rate_arg(x):
    x = float(x)
    if not 0.0 <= x <= 1.0:
        raise argparse.ArgumentTypeError("Rate must be between 0.0 and 1.0")
    return x

def parse_args():
    parser = argparse.ArgumentParser(description="Go-Back-N UDP Receiver")
    parser.add_argument("--loss-rate", type=_rate_arg, default=0.0)
    parser.add_argument("--corruption-rate", type=_rate_arg, default=0.0)
    parser.add_argument("--host", type=str, default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    return parser.parse_args()

def main():
    args = parse_args()
    sock = setup_socket(args.host, args.port)
    try:
        receiver_loop(sock, args.loss_rate, args.corruption_rate)
    except KeyboardInterrupt:
        print("\nShutting down receiver...")
    finally:
        sock.close()

if __name__ == "__main__":
    main()
