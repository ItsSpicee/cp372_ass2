import os
import sys
import time
import socket
import threading
import hashlib
import tempfile
import shutil
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from packet import Packet, HEADER_SIZE
from sender_gbn import send_file_gbn, ack_receiver, check_file_name, InvalidFileName
from receiver_gbn import receiver_loop


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestPacketFileId(unittest.TestCase):

    def test_file_id_roundtrip(self):
        for fid in (0, 1, 255):
            pkt = Packet(seq_num=7, ack_num=3, ptype=Packet.TYPE_DATA, file_id=fid, payload=b"hello")
            raw = pkt.to_bytes()
            restored = Packet.from_bytes(raw)
            self.assertEqual(restored.file_id, fid)
            self.assertEqual(restored.seq_num, 7)
            self.assertEqual(restored.payload, b"hello")

    def test_file_id_in_checksum(self):
        pkt_a = Packet(seq_num=1, file_id=0, payload=b"test")
        pkt_b = Packet(seq_num=1, file_id=1, payload=b"test")
        self.assertNotEqual(pkt_a.compute_checksum(), pkt_b.compute_checksum())

    def test_ack_carries_file_id(self):
        ack = Packet(seq_num=0, ack_num=5, ptype=Packet.TYPE_ACK, file_id=1)
        raw = ack.to_bytes()
        restored = Packet.from_bytes(raw)
        self.assertEqual(restored.file_id, 1)
        self.assertEqual(restored.ptype, Packet.TYPE_ACK)
        self.assertEqual(restored.ack_num, 5)

    def test_default_file_id_zero(self):
        pkt = Packet(seq_num=0, payload=b"x")
        self.assertEqual(pkt.file_id, 0)


class TestParallelDuplexTransfer(unittest.TestCase):

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.orig_dir = os.getcwd()
        self.file1_path = os.path.join(self.work_dir, "small1.txt")
        self.file2_path = os.path.join(self.work_dir, "small2.txt")
        with open(self.file1_path, "wb") as f:
            f.write(b"A" * 5000)
        with open(self.file2_path, "wb") as f:
            f.write(b"B" * 3000)
        self.recv_dir = os.path.join(self.work_dir, "received")
        os.makedirs(self.recv_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def _run_transfer_files(self, file_paths, loss_rate=0.0, corruption_rate=0.0):
        port = find_free_port()
        host = "127.0.0.1"

        recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_sock.bind((host, port))

        os.chdir(self.recv_dir)

        recv_thread = threading.Thread(
            target=receiver_loop,
            args=(recv_sock, loss_rate, corruption_rate),
            daemon=True
        )
        recv_thread.start()

        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_addr = (host, port)
        sock_lock = threading.Lock()

        n = len(file_paths)
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
            args=(send_sock, corruption_rate, ack_states, stop_event),
            daemon=True
        )
        ack_thread.start()

        threads = [
            threading.Thread(
                target=send_file_gbn,
                args=(send_sock, sock_lock, fpath, server_addr, corruption_rate, fid, ack_states)
            )
            for fid, fpath in enumerate(file_paths)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        stop_event.set()
        ack_thread.join(timeout=2)

        time.sleep(0.2)
        send_sock.close()
        recv_sock.close()

    def _run_transfer(self, loss_rate=0.0, corruption_rate=0.0):
        self._run_transfer_files(
            [self.file1_path, self.file2_path],
            loss_rate=loss_rate,
            corruption_rate=corruption_rate,
        )

    def test_both_files_received(self):
        self._run_transfer()
        recv1 = os.path.join(self.recv_dir, os.path.basename(self.file1_path))
        recv2 = os.path.join(self.recv_dir, os.path.basename(self.file2_path))
        self.assertTrue(os.path.exists(recv1), f"File 1 not received: {recv1}")
        self.assertTrue(os.path.exists(recv2), f"File 2 not received: {recv2}")

    def test_file_integrity(self):
        self._run_transfer()
        recv1 = os.path.join(self.recv_dir, os.path.basename(self.file1_path))
        recv2 = os.path.join(self.recv_dir, os.path.basename(self.file2_path))
        self.assertEqual(md5(self.file1_path), md5(recv1))
        self.assertEqual(md5(self.file2_path), md5(recv2))

    def test_files_not_mixed(self):
        self._run_transfer()
        recv1 = os.path.join(self.recv_dir, os.path.basename(self.file1_path))
        recv2 = os.path.join(self.recv_dir, os.path.basename(self.file2_path))
        with open(recv1, "rb") as f:
            data1 = f.read()
        with open(recv2, "rb") as f:
            data2 = f.read()
        self.assertTrue(all(b == ord('A') for b in data1), "File 1 contains foreign bytes")
        self.assertTrue(all(b == ord('B') for b in data2), "File 2 contains foreign bytes")

    def test_with_packet_loss(self):
        self._run_transfer(loss_rate=0.1)
        recv1 = os.path.join(self.recv_dir, os.path.basename(self.file1_path))
        recv2 = os.path.join(self.recv_dir, os.path.basename(self.file2_path))
        self.assertEqual(md5(self.file1_path), md5(recv1))
        self.assertEqual(md5(self.file2_path), md5(recv2))


class TestNFileTransfer(unittest.TestCase):

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.orig_dir = os.getcwd()
        self.recv_dir = os.path.join(self.work_dir, "received")
        os.makedirs(self.recv_dir)
        self.files = []
        for i in range(4):
            path = os.path.join(self.work_dir, f"file{i}.txt")
            byte_val = ord('A') + i
            with open(path, "wb") as f:
                f.write(bytes([byte_val]) * (2000 + i * 500))
            self.files.append(path)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def _run(self, file_paths, loss_rate=0.0, corruption_rate=0.0):
        port = find_free_port()
        host = "127.0.0.1"

        recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_sock.bind((host, port))
        os.chdir(self.recv_dir)

        threading.Thread(
            target=receiver_loop,
            args=(recv_sock, loss_rate, corruption_rate),
            daemon=True
        ).start()

        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_addr = (host, port)
        sock_lock = threading.Lock()
        n = len(file_paths)

        ack_states = {}
        for fid in range(n):
            lock = threading.Lock()
            ack_states[fid] = {'lock': lock, 'condition': threading.Condition(lock), 'ack_queue': []}

        stop_event = threading.Event()
        threading.Thread(
            target=ack_receiver,
            args=(send_sock, 0.0, ack_states, stop_event),
            daemon=True
        ).start()

        threads = [
            threading.Thread(
                target=send_file_gbn,
                args=(send_sock, sock_lock, fp, server_addr, corruption_rate, fid, ack_states)
            )
            for fid, fp in enumerate(file_paths)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        stop_event.set()
        time.sleep(0.2)
        send_sock.close()
        recv_sock.close()

    def test_four_files_received(self):
        self._run(self.files)
        for f in self.files:
            recv = os.path.join(self.recv_dir, os.path.basename(f))
            self.assertTrue(os.path.exists(recv), f"Missing: {recv}")

    def test_four_files_integrity(self):
        self._run(self.files)
        for f in self.files:
            recv = os.path.join(self.recv_dir, os.path.basename(f))
            self.assertEqual(md5(f), md5(recv), f"Integrity fail: {f}")

    def test_four_files_not_mixed(self):
        self._run(self.files)
        for i, f in enumerate(self.files):
            recv = os.path.join(self.recv_dir, os.path.basename(f))
            with open(recv, "rb") as fh:
                data = fh.read()
            expected_byte = ord('A') + i
            self.assertTrue(
                all(b == expected_byte for b in data),
                f"file{i}.txt contains foreign bytes"
            )

    def test_single_file(self):
        self._run([self.files[0]])
        recv = os.path.join(self.recv_dir, os.path.basename(self.files[0]))
        self.assertTrue(os.path.exists(recv))
        self.assertEqual(md5(self.files[0]), md5(recv))


class TestFileNameValidation(unittest.TestCase):

    def test_valid_name(self):
        check_file_name("test.txt")

    def test_no_extension_raises(self):
        with self.assertRaises(InvalidFileName):
            check_file_name("noextension")

    def test_hidden_with_extension(self):
        check_file_name(".hidden.txt")


if __name__ == "__main__":
    unittest.main()
