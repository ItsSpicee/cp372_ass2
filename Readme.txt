CP372 Assignment 2 - Reliable Data Transfer over UDP
=====================================================


REQUIREMENTS
------------
- Python 3.x (developed and tested on Python 3.14)
- Standard library only. No third-party packages are required to run the
  protocols. (visualize.py uses matplotlib ONLY to draw the graphs for the
  report; the protocols themselves do not need it.)


FILES
-----
  packet.py              Shared packet format + 8-bit checksum logic.
                         Imported by the senders/receivers; never run directly.
  sender_stopwait.py     Stop-and-Wait sender (Part A)
  receiver_stopwait.py   Stop-and-Wait receiver (Parts A and B)
  sender_gbn.py          Go-Back-N sender (Part C) + parallel multi-file bonus
  receiver_gbn.py        Go-Back-N receiver (Part C)
  run_experiments.py     Automates Part D (drives sender/receiver across file
                         sizes, loss rates and corruption rates, 5 trials each)
  visualize.py           Builds summary.csv + plots from run_experiments output
  test_parallel_multiplex.py   Unit tests for the parallel multi-file bonus


PORTS
-----
  Stop-and-Wait receiver listens on UDP port 6969
  Go-Back-N receiver listens on UDP port 6970
Both sender and receiver are assumed to run on localhost. To use different
machines, change the host/port at the top of the scripts or pass --host/--port.


HOW TO RUN A SINGLE TRANSFER
----------------------------
Open two terminals. Start the RECEIVER first, then the SENDER.
The received copy of the file is written into the receiver's current directory,
so run the receiver from wherever you want the output file to land.

  Stop-and-Wait
  -------------
  Terminal 1:  python receiver_stopwait.py
  Terminal 2:  python sender_stopwait.py
               Enter the file name to be transferred (with extension): myfile.txt

  Go-Back-N
  ---------
  Terminal 1:  python receiver_gbn.py
  Terminal 2:  python sender_gbn.py
               Enter the number of files to transfer: 1
               Enter file 1 to transfer: myfile.txt


PARALLEL MULTI-FILE TRANSFER (BONUS)
------------------------------------
The Go-Back-N sender can transfer several files at once over a single UDP
socket. When prompted for the number of files, enter a value from 1 to 255:

  Terminal 1:  python receiver_gbn.py
  Terminal 2:  python sender_gbn.py
               Enter the number of files to transfer: 3
               Enter file 1 to transfer: a.txt
               Enter file 2 to transfer: b.txt
               Enter file 3 to transfer: c.txt

Each file is sent by its own thread; a single field in the packet header
(file_id) keeps the streams separate so they are reassembled into the correct
files on the receiver. See Bonus.txt for details.


SIMULATING PACKET LOSS AND CORRUPTION (Parts B and bonus)
---------------------------------------------------------
Both flags are passed to the RECEIVER:

  python receiver_gbn.py --loss-rate 0.2 --corruption-rate 0.05

  --loss-rate FLOAT         probability [0.0-1.0] each incoming packet is dropped
  --corruption-rate FLOAT   probability [0.0-1.0] one random bit is flipped in
                            an incoming packet (caught by the checksum)

The senders also accept --corruption-rate (applied to incoming ACKs). The
senders accept --loss-rate too, but it is a no-op: loss is simulated on the
receiver side only.


REPRODUCING THE EXPERIMENTS (Part D)
------------------------------------
  1.  python run_experiments.py
        Generates test files, runs both protocols across all file sizes /
        loss rates / corruption rates (5 trials each), writes results.csv.
        Add --quick for a fast smoke test, or --skip-bonus to skip the
        corruption matrix.
  2.  python visualize.py
        Reads results.csv, writes summary.csv (averaged numbers used for the
        report tables) and 4 charts into the plots/ folder.

Generated automatically (not committed): test_files/, received_files/,
results.csv, summary.csv, plots/.


ASSUMPTIONS
-----------
- Sender and receiver run on localhost.
- File names must contain an extension (exactly one dot, e.g. myfile.txt).
- CHUNK_SIZE = 1024 bytes and TIMEOUT = 0.3s on both protocols.
- Go-Back-N uses WINDOW_SIZE = 8 and sequence numbers modulo 256.
- Stop-and-Wait uses 1-bit alternating (0/1) sequence numbers and retries
  on timeout without a retry cap.
- A packet that fails its checksum (corrupted) is dropped silently and no ACK
  is sent; the existing timeout/retransmit logic recovers it.
- 50MB and 100MB file sizes are commented out in run_experiments.py to keep
  runtime reasonable; re-enable those lines for the full size sweep.
