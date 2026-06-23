import os
import sys
import csv
import re
import time
import argparse
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
TEST_FILES_DIR = os.path.join(PROJECT_ROOT, "test_files")
RECEIVED_DIR = os.path.join(PROJECT_ROOT, "received_files")
RESULTS_CSV = os.path.join(PROJECT_ROOT, "results.csv")

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
        result = subprocess.run(
            cmd, cwd=TEST_FILES_DIR, input=file_name + "\n",
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


def run_trial(protocol, label, size, loss_rate, corruption_rate, trial, writer, csv_file, timeout):
    file_name = f"file_{label}.bin"
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
    parser.add_argument("--timeout", type=float, default=120.0,
                         help="Per-trial sender timeout in seconds (safety net)")
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
