'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: run_experiments.py
Description: Part D automation. Drives the Stop-and-Wait and Go-Back-N
             sender/receiver scripts across every file size, loss rate and
             corruption rate (5 trials each), measuring transfer time,
             throughput and retransmissions, and writes the raw per-trial
             results to results.csv for visualize.py to summarise.
Capabilities:
    - Generate random test files of each required size
    - Run both protocols over the full size / loss / corruption matrix
    - Record per-trial transfer time, throughput and retransmissions to CSV

Authors:
    Obeidi, Bassil
    Barghouti, Alaa
    Ozog, Philip
    Soja, Max
    Yamin, Noah
'''

import os
import sys
import csv
import re
import time
import argparse
import subprocess

# This script lives in experiments/; the sender/receiver scripts are in the
# repo root (its parent). Generated artifacts are kept inside experiments/ so
# the repo root stays limited to the files being marked.
EXPERIMENTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(EXPERIMENTS_DIR)
TEST_FILES_DIR = os.path.join(EXPERIMENTS_DIR, "test_files")
RECEIVED_DIR = os.path.join(EXPERIMENTS_DIR, "received_files")
RESULTS_CSV = os.path.join(EXPERIMENTS_DIR, "results.csv")

SENDER_SCRIPTS = {
    "stopwait": os.path.join(PROJECT_ROOT, "sender_stopwait.py"),
    "gbn": os.path.join(PROJECT_ROOT, "sender_gbn.py"),
}
RECEIVER_SCRIPTS = {
    "stopwait": os.path.join(PROJECT_ROOT, "receiver_stopwait.py"),
    "gbn": os.path.join(PROJECT_ROOT, "receiver_gbn.py"),
}

FILE_SIZES = {
    "10KB": 10 * 1024,
    "50KB": 50 * 1024,
    "100KB": 100 * 1024,
    "500KB": 500 * 1024,
    "1MB": 1 * 1024 * 1024,
    "5MB": 5 * 1024 * 1024,
    "10MB": 10 * 1024 * 1024,
    "50MB": 50 * 1024 * 1024,
    "100MB": 100 * 1024 * 1024,
}

LOSS_RATES = [0.0, 0.1, 0.2, 0.3]
CORRUPTION_RATES_BONUS = [0.0, 0.05, 0.10, 0.15]
TRIALS = 5
PROTOCOLS = ["stopwait", "gbn"]

RETRANS_RE = re.compile(r"RETRANSMISSIONS:\s*(\d+)")
BIND_WAIT = 0.2

# The per-trial sender timeout is a safety net against a deadlocked or
# livelocked transfer (dead receiver, stuck bind, infinite retransmission), not
# a performance target. A single flat value cannot serve a 10,000x file-size
# range: 120s is fine for 10KB but truncates a legitimately slow 1MB/0.3-loss
# transfer, poisoning its measurement. Instead we size the timeout to the
# worst-case expected throughput for the link condition, so only a genuinely
# stuck transfer ever hits it while a healthy-but-slow one is allowed to finish.
#
# Throughput floors (bytes/sec) are read off the measured 1MB results: the
# slowest healthy transfer observed at each degradation level. Loss and
# corruption are swept independently, so the effective floor is whichever of the
# two is lower.
LOSS_THROUGHPUT_FLOOR = {0.0: 1_000_000, 0.1: 25_000, 0.2: 10_000, 0.3: 5_000}
CORRUPTION_THROUGHPUT_FLOOR = {0.0: 1_000_000, 0.05: 55_000, 0.10: 25_000, 0.15: 18_000}
TIMEOUT_SAFETY_FACTOR = 3.0   # headroom over the expected completion time
MIN_TIMEOUT = 60.0            # floor so tiny files still get a real kill switch


def _throughput_floor(rate, table):
    # Exact match when the rate is one of the swept values; otherwise fall back
    # to the floor of the nearest-or-worse degradation (the conservative choice,
    # since a lower floor yields a longer, safer timeout).
    if rate in table:
        return table[rate]
    worse_or_equal = [v for k, v in table.items() if k >= rate]
    return min(worse_or_equal) if worse_or_equal else min(table.values())


def compute_timeout(size, loss_rate, corruption_rate):
    floor = min(_throughput_floor(loss_rate, LOSS_THROUGHPUT_FLOOR),
                _throughput_floor(corruption_rate, CORRUPTION_THROUGHPUT_FLOOR))
    return max(MIN_TIMEOUT, (size / floor) * TIMEOUT_SAFETY_FACTOR)


def generate_test_files(sizes):
    os.makedirs(TEST_FILES_DIR, exist_ok=True)
    for label, size in sizes.items():
        path = os.path.join(TEST_FILES_DIR, f"file_{label}.bin")
        if not os.path.exists(path) or os.path.getsize(path) != size:
            print(f"Generating file_{label}.bin ({size} bytes)...")
            with open(path, "wb") as f:
                remaining = size
                while remaining > 0:
                    n = min(1024 * 1024, remaining)
                    f.write(os.urandom(n))
                    remaining -= n


def start_receiver(protocol, loss_rate, corruption_rate):
    os.makedirs(RECEIVED_DIR, exist_ok=True)
    cmd = [
        sys.executable, RECEIVER_SCRIPTS[protocol],
        "--loss-rate", str(loss_rate),
        "--corruption-rate", str(corruption_rate),
    ]
    proc = subprocess.Popen(
        cmd, cwd=RECEIVED_DIR,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(BIND_WAIT)
    if proc.poll() is not None:
        raise RuntimeError(f"Receiver ({protocol}) exited immediately — port may be in use")
    return proc


def stop_receiver(proc):
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def run_sender(protocol, file_name, loss_rate, corruption_rate, timeout):
    cmd = [
        sys.executable, SENDER_SCRIPTS[protocol],
        "--loss-rate", str(loss_rate),
        "--corruption-rate", str(corruption_rate),
    ]
    start = time.time()
    try:
        stdin_input = f"1\n{file_name}\n" if protocol == "gbn" else f"{file_name}\n"
        result = subprocess.run(
            cmd, cwd=TEST_FILES_DIR, input=stdin_input,
            capture_output=True, text=True, timeout=timeout,
        )
        stdout = result.stdout
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout or ""
        print(f"    WARNING: sender timed out after {timeout}s")
    elapsed = time.time() - start

    match = RETRANS_RE.search(stdout)
    retransmissions = int(match.group(1)) if match else None
    success = "complete" in stdout.lower()
    return elapsed, retransmissions, success


def run_trial(protocol, label, size, loss_rate, corruption_rate, trial, writer, csv_file, timeout_override):
    file_name = f"file_{label}.bin"
    # A flat --timeout override (if given) wins; otherwise scale the safety-net
    # timeout to this trial's size and link condition.
    timeout = (timeout_override if timeout_override is not None
               else compute_timeout(size, loss_rate, corruption_rate))
    receiver = start_receiver(protocol, loss_rate, corruption_rate)
    try:
        elapsed, retransmissions, success = run_sender(
            protocol, file_name, loss_rate, corruption_rate, timeout
        )
    finally:
        stop_receiver(receiver)

    throughput = size / elapsed if elapsed > 0 else 0
    print(
        f"  [{protocol}] size={label} loss={loss_rate} corrupt={corruption_rate} "
        f"trial={trial} time={elapsed:.3f}s throughput={throughput:.0f}B/s "
        f"retrans={retransmissions} success={success}"
    )

    writer.writerow({
        "protocol": protocol,
        "file_size": size,
        "loss_rate": loss_rate,
        "corruption_rate": corruption_rate,
        "trial": trial,
        "transfer_time": elapsed,
        "throughput": throughput,
        "retransmissions": retransmissions if retransmissions is not None else "",
    })
    csv_file.flush()


def main():
    parser = argparse.ArgumentParser(description="RDT experiment automation")
    parser.add_argument("--quick", action="store_true",
                         help="Run a small reduced matrix for smoke-testing")
    parser.add_argument("--timeout", type=float, default=None,
                         help="Flat per-trial sender timeout override in seconds. "
                              "If omitted, the timeout is scaled per trial from the "
                              "file size and link condition (see compute_timeout).")
    parser.add_argument("--skip-bonus", action="store_true",
                         help="Skip the corruption-rate bonus matrix")
    args = parser.parse_args()

    sizes = FILE_SIZES
    loss_rates = LOSS_RATES
    trials = TRIALS
    corruption_rates_bonus = CORRUPTION_RATES_BONUS

    if args.quick:
        sizes = {"10KB": 10 * 1024, "100KB": 100 * 1024}
        loss_rates = [0.0, 0.2]
        trials = 1
        corruption_rates_bonus = [0.0, 0.1]

    generate_test_files(sizes)

    fieldnames = [
        "protocol", "file_size", "loss_rate", "corruption_rate",
        "trial", "transfer_time", "throughput", "retransmissions",
    ]

    with open(RESULTS_CSV, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        csv_file.flush()

        print("=== Main experiment matrix (loss rates, corruption_rate=0.0) ===")
        for protocol in PROTOCOLS:
            for label, size in sizes.items():
                for loss_rate in loss_rates:
                    for trial in range(1, trials + 1):
                        run_trial(
                            protocol, label, size, loss_rate, 0.0, trial,
                            writer, csv_file, args.timeout,
                        )

        if not args.skip_bonus:
            print("=== Bonus matrix (corruption rates, loss_rate=0.0) ===")
            for protocol in PROTOCOLS:
                for label, size in sizes.items():
                    for corruption_rate in corruption_rates_bonus:
                        for trial in range(1, trials + 1):
                            run_trial(
                                protocol, label, size, 0.0, corruption_rate, trial,
                                writer, csv_file, args.timeout,
                            )

    print(f"Done. Results written to {RESULTS_CSV}")


if __name__ == "__main__":
    main()
