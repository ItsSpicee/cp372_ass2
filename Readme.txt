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
The repo root holds only the files being submitted/marked; supporting code is
organised into folders.

  Root (submission files + the code that runs them)
    sender_stopwait.py     Stop-and-Wait sender (Part A)
    receiver_stopwait.py   Stop-and-Wait receiver (Parts A and B)
    sender_gbn.py          Go-Back-N sender (Part C) + parallel multi-file bonus
    receiver_gbn.py        Go-Back-N receiver (Part C)
    packet.py              Shared packet format + 8-bit checksum logic.
                           Imported by the senders/receivers; never run directly.
    gbn_trace.py           Optional tracing hooks for the GBN sender; a near-zero
                           no-op unless the visualizer attaches.
    report.pdf             Project report
    Readme.txt             This file
    Bonus.txt              Description of the bonus work

Supporting material (not required to run the protocols) is kept in folders:
  tests/
    test_parallel_multiplex.py   Unit tests for the parallel multi-file bonus
  experiments/ Part D automation + report figures:
    run_experiments.py     Drives sender/receiver across file sizes, loss rates
                           and corruption rates (5 trials each)
    visualize.py           Builds summary.csv + plots from run_experiments output
    visualize_gbn_threads.py   Live animation of the GBN sender's threads
  media/       Pre-rendered figures (gbn_threads.png / .gif)

The four protocol scripts run straight from the repo root with no setup
(e.g. `python sender_gbn.py`).


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
  --corruption-rate FLOAT   probability [0.0-1.0] one random bit is flipped in an
                            incoming packet's payload (caught by the checksum)

The senders accept --loss-rate and --corruption-rate too for CLI uniformity, but
both effects are simulated on the receiver side.


REPRODUCING THE EXPERIMENTS (Part D)
------------------------------------
  1.  python experiments/run_experiments.py
        Generates test files, runs both protocols across all file sizes /
        loss rates / corruption rates (5 trials each), writes results.csv.
        Add --quick for a fast smoke test, or --skip-bonus to skip the
        corruption matrix.
  2.  python experiments/visualize.py
        Reads results.csv, writes summary.csv (averaged numbers used for the
        report tables) and 4 charts into the repo-root media/ folder.

Generated automatically (not committed): experiments/test_files/,
experiments/received_files/, experiments/results.csv, experiments/summary.csv,
and the 4 charts in the repo-root media/ folder.


ASSUMPTIONS
-----------
- Sender and receiver run on localhost.
- File names must contain an extension, i.e. at least one dot (e.g. myfile.txt).
  Multiple extensions such as archive.tar.gz are also accepted.
- CHUNK_SIZE = 1024 bytes and TIMEOUT = 0.3s on both protocols.
- Go-Back-N uses WINDOW_SIZE = 8 and sequence numbers modulo 256.
- Stop-and-Wait uses 1-bit alternating (0/1) sequence numbers and retransmits on
  timeout for up to MAX_RETRIES (20) consecutive timeouts before giving up on a
  packet. Go-Back-N retransmits its window on timeout without a retry cap.
- A packet that fails its checksum (corrupted) is dropped and no ACK is sent;
  the existing timeout/retransmit logic recovers it.
- run_experiments.py runs all nine file sizes (10KB through 100MB); the largest
  sizes dominate the runtime, so use --quick for a fast reduced run.
