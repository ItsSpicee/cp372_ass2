import socket
import os
import packet

SERVER_ADDRESS = "localhost"
SERVER_PORT = 6969
TIMEOUT = 1.0 
MAX_RETRIES = 5


class InvalidFileName(Exception):
    pass

def setup_connection():
    sock = socket.socket(sock.AF_INET, sock.SOCK_DGRAM)
    sock.settimeout(TIMEOUT) 

    return sock

def check_file_name(file_name):
    parts = file_name.split(".")

    if len(parts) != 2: raise InvalidFileName("File name must of form xxx.yy")

def notify_server(file_name):
    start_pkt = Packet(seq_num=0, ptype=Packet.TYPE_START, payload=file_name.encode())

    return send_and_wait_for_ack(sock, start_pkt, server_addr, seq_num)


def send_and_wait_for_ack(sock, packet, server_addr, expected_ack_num):
    retries = 0
    
    while retries < MAX_RETRIES:
        sock.sendto(packet.to_bytes(), server_addr)
        sock.settimeout(TIMEOUT)
        
        while True:
            try:
                data, addr = sock.recvfrom(2048)

                try:
                    ack_pkt = Packet.from_bytes(data)
                except ValueError as e:
                    print(f"CBad packet received: {e}")
                    continue  # Stay in inner loop, keep waiting
                
                if ack_pkt.ptype == Packet.TYPE_ACK and ack_pkt.ack_num == expected_ack_num:
                    return True
                
                # Wrong ACK — keep waiting, don't count as retry
                print(f"Stale ACK (ack_num={ack_pkt.ack_num}), still waiting...")
                
            except socket.timeout:
                retries += 1
                print(f"Timeout #{retries}, retransmitting...")
                break  # Break inner, retransmit in outer
    
    print("Max retries exceeded, giving up")
    return False

def send_file(sock, file_name, server_addr):
    """Send the actual file data in chunks."""
    seq_num = 0  # Start with 0, will flip to 1 and back
    
    # Step 1: Notify server (START)
    if not notify_server(sock, file_name, server_addr, seq_num):
        print("Failed to start transfer")
        return
    seq_num = 1 - seq_num  # Flip: 0 → 1
    
    # Step 2: Send file chunks
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
            
            if not send_and_wait_for_ack(sock, data_pkt, server_addr, seq_num):
                print("Transfer failed")
                return
            
            seq_num = 1 - seq_num  # Flip for next packet: 0↔1
    
    # Step 3: Send END packet
    end_pkt = Packet(
        seq_num=seq_num,
        ptype=Packet.TYPE_END,
        payload=b''  # Empty payload
    )
    if not send_and_wait_for_ack(sock, data_pkt, server_addr, seq_num):
        print("Unable to notify server of transfer completion.")
    else:
        print("File transfer complete. :)")


def user_loop(sock):
    server_addr = (SERVER_ADDRESS, SERVER_PORT)
    
    while True:
        file_name = input("Enter the file name to be transferred (with extension): ")
        
        try:
            check_file_name(file_name)
            
            if not os.path.exists(file_name):
                print(f"File '{file_name}' not found.")
                continue
            
            send_file(sock, file_name, server_addr)
            
        except InvalidFileName as e:
            print(e)
        except KeyboardInterrupt:
            print("\nExiting...")
            break
 



def main():
    print("Setting up UDP Socket...")
    sender_socket = setup_connection():
    user_loop():
    sender_socket.close()




if __name__ == "__main__":
    main() 