'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: visualize.py
Description: Reads the results.csv produced by run_experiments.py, averages each
             configuration over its trials, writes summary.csv (the numbers used
             for the report tables) and renders the comparison charts into the
             repo-root media/ folder.
Capabilities:
    - Average transfer time, throughput and retransmissions per configuration
    - Write summary.csv and print the report tables
    - Render transfer-time, throughput, retransmission and corruption charts

Authors:
    Obeidi, Bassil
    Barghouti, Alaa
    Ozog, Philip
    Soja, Max
    Yamin, Noah
'''

import csv
import os
import statistics
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# CSV artifacts are produced/consumed inside experiments/ (see
# run_experiments.py); rendered charts go to the repo-root media/ folder
# alongside the other figures used in the report.
EXPERIMENTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(EXPERIMENTS_DIR)
RESULTS_CSV = os.path.join(EXPERIMENTS_DIR, "results.csv")
SUMMARY_CSV = os.path.join(EXPERIMENTS_DIR, "summary.csv")
MEDIA_DIR = os.path.join(PROJECT_ROOT, "media")

SIZE_LABELS = {
    10 * 1024: "10KB",
    50 * 1024: "50KB",
    100 * 1024: "100KB",
    500 * 1024: "500KB",
    1 * 1024 * 1024: "1MB",
    5 * 1024 * 1024: "5MB",
    10 * 1024 * 1024: "10MB",
    50 * 1024 * 1024: "50MB",
    100 * 1024 * 1024: "100MB",
}

PROTOCOL_LABELS = {"stopwait": "S&W", "gbn": "GBN"}
LOSS_RATES = [0.0, 0.1, 0.2, 0.3]
REPRESENTATIVE_SIZES = [100 * 1024, 1 * 1024 * 1024, 10 * 1024 * 1024]


def format_size(num_bytes):
    if num_bytes in SIZE_LABELS:
        return SIZE_LABELS[num_bytes]
    if num_bytes >= 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):g}MB"
    if num_bytes >= 1024:
        return f"{num_bytes / 1024:g}KB"
    return f"{num_bytes}B"


def load_results():
    rows = []
    with open(RESULTS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "protocol": row["protocol"],
                "file_size": int(row["file_size"]),
                "loss_rate": float(row["loss_rate"]),
                "corruption_rate": float(row["corruption_rate"]),
                "trial": int(row["trial"]),
                "transfer_time": float(row["transfer_time"]),
                "throughput": float(row["throughput"]),
                "retransmissions": int(row["retransmissions"]) if row["retransmissions"] != "" else None,
            })
    return rows


def average_by_config(rows):
    groups = defaultdict(list)
    for row in rows:
        key = (row["protocol"], row["file_size"], row["loss_rate"], row["corruption_rate"])
        groups[key].append(row)

    averaged = {}
    for key, group in groups.items():
        times = [r["transfer_time"] for r in group]
        throughputs = [r["throughput"] for r in group]
        retrans = [r["retransmissions"] for r in group if r["retransmissions"] is not None]
        averaged[key] = {
            "protocol": key[0],
            "file_size": key[1],
            "loss_rate": key[2],
            "corruption_rate": key[3],
            "avg_transfer_time": statistics.mean(times),
            "avg_throughput": statistics.mean(throughputs),
            "avg_retransmissions": statistics.mean(retrans) if retrans else None,
            "n_trials": len(group),
        }
    return averaged


def write_summary(averaged):
    fieldnames = ["protocol", "file_size", "loss_rate", "corruption_rate",
                  "avg_transfer_time", "avg_throughput", "avg_retransmissions", "n_trials"]
    with open(SUMMARY_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for key in sorted(averaged.keys()):
            row = dict(averaged[key])
            if row["avg_retransmissions"] is None:
                row["avg_retransmissions"] = ""
            writer.writerow(row)


def sizes_present(averaged, protocols=("stopwait", "gbn")):
    sizes = {key[1] for key in averaged if key[0] in protocols}
    return sorted(sizes)


def print_table(averaged, protocol, metric, sizes):
    label = "Transfer Time (s)" if metric == "avg_transfer_time" else "Throughput (B/s)"
    print(f"\nTable: {PROTOCOL_LABELS.get(protocol, protocol)} {label}")
    header = "Loss Rate".ljust(12) + "".join(format_size(s).rjust(12) for s in sizes)
    print(header)
    for loss_rate in LOSS_RATES:
        row_label = f"{loss_rate * 100:.0f}%".ljust(12)
        cells = []
        for size in sizes:
            value = averaged.get((protocol, size, loss_rate, 0.0))
            if value is None:
                cells.append("N/A".rjust(12))
            elif metric == "avg_transfer_time":
                cells.append(f"{value[metric]:.3f}".rjust(12))
            else:
                cells.append(f"{value[metric]:.0f}".rjust(12))
        print(row_label + "".join(cells))


def plot_metric_vs_size(averaged, sizes, metric, ylabel, title, filename):
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=True)
    axes = axes.flatten()
    for ax, loss_rate in zip(axes, LOSS_RATES):
        has_data = False
        for protocol in ("stopwait", "gbn"):
            xs, ys = [], []
            for size in sizes:
                value = averaged.get((protocol, size, loss_rate, 0.0))
                if value is not None:
                    xs.append(size)
                    ys.append(value[metric])
            if xs:
                ax.plot(xs, ys, marker="o", label=PROTOCOL_LABELS[protocol])
                has_data = True
        ax.set_xscale("log")
        ax.set_title(f"Loss Rate {loss_rate * 100:.0f}%")
        ax.set_xlabel("File Size (bytes)")
        ax.set_ylabel(ylabel)
        if has_data:
            ax.legend()
        ax.grid(True, which="both", alpha=0.3)
    fig.suptitle(title)
    fig.tight_layout()
    os.makedirs(MEDIA_DIR, exist_ok=True)
    fig.savefig(os.path.join(MEDIA_DIR, filename))
    plt.close(fig)


def plot_retransmissions(averaged, sizes):
    rep_sizes = [s for s in REPRESENTATIVE_SIZES if s in sizes] or sizes[:3]
    if not rep_sizes:
        return

    fig, axes = plt.subplots(1, len(rep_sizes), figsize=(5 * len(rep_sizes), 5), squeeze=False)
    axes = axes.flatten()
    bar_width = 0.35

    for ax, size in zip(axes, rep_sizes):
        x = range(len(LOSS_RATES))
        for offset, protocol in zip((-bar_width / 2, bar_width / 2), ("stopwait", "gbn")):
            ys = []
            for loss_rate in LOSS_RATES:
                value = averaged.get((protocol, size, loss_rate, 0.0))
                ys.append(value["avg_retransmissions"] if value and value["avg_retransmissions"] is not None else 0)
            ax.bar([xi + offset for xi in x], ys, width=bar_width, label=PROTOCOL_LABELS[protocol])
        ax.set_xticks(list(x))
        ax.set_xticklabels([f"{lr * 100:.0f}%" for lr in LOSS_RATES])
        ax.set_xlabel("Loss Rate")
        ax.set_ylabel("Avg Retransmissions")
        ax.set_title(format_size(size))
        ax.legend()
        ax.grid(True, axis="y", alpha=0.3)

    fig.suptitle("Average Retransmissions vs Loss Rate")
    fig.tight_layout()
    os.makedirs(MEDIA_DIR, exist_ok=True)
    fig.savefig(os.path.join(MEDIA_DIR, "plot_retransmissions.png"))
    plt.close(fig)


def plot_corruption(averaged):
    corruption_keys = {key for key in averaged if key[3] > 0.0}
    if not corruption_keys:
        print("No corruption-rate data found, skipping plot_corruption.png")
        return

    corruption_rates = sorted({0.0} | {key[3] for key in corruption_keys})
    sizes = sorted({key[1] for key in averaged if key[2] == 0.0})
    rep_sizes = [s for s in REPRESENTATIVE_SIZES if s in sizes] or sizes[:3]
    if not rep_sizes:
        return

    fig, axes = plt.subplots(1, len(rep_sizes), figsize=(5 * len(rep_sizes), 5), squeeze=False)
    axes = axes.flatten()

    for ax, size in zip(axes, rep_sizes):
        for protocol in ("stopwait", "gbn"):
            xs, ys = [], []
            for cr in corruption_rates:
                value = averaged.get((protocol, size, 0.0, cr))
                if value is not None:
                    xs.append(cr)
                    ys.append(value["avg_transfer_time"])
            if xs:
                ax.plot(xs, ys, marker="o", label=PROTOCOL_LABELS[protocol])
        ax.set_xlabel("Corruption Rate")
        ax.set_ylabel("Avg Transfer Time (s)")
        ax.set_title(format_size(size))
        ax.legend()
        ax.grid(True, alpha=0.3)

    fig.suptitle("Transfer Time vs Corruption Rate")
    fig.tight_layout()
    os.makedirs(MEDIA_DIR, exist_ok=True)
    fig.savefig(os.path.join(MEDIA_DIR, "plot_corruption.png"))
    plt.close(fig)


def main():
    rows = load_results()
    if not rows:
        print(f"No data found in {RESULTS_CSV}")
        return

    averaged = average_by_config(rows)
    write_summary(averaged)

    sizes = sizes_present(averaged)

    print_table(averaged, "stopwait", "avg_transfer_time", sizes)
    print_table(averaged, "gbn", "avg_transfer_time", sizes)
    print_table(averaged, "stopwait", "avg_throughput", sizes)
    print_table(averaged, "gbn", "avg_throughput", sizes)

    plot_metric_vs_size(averaged, sizes, "avg_transfer_time", "Avg Transfer Time (s)",
                         "Transfer Time vs File Size", "plot_transfer_time.png")
    plot_metric_vs_size(averaged, sizes, "avg_throughput", "Avg Throughput (B/s)",
                         "Throughput vs File Size", "plot_throughput.png")
    plot_retransmissions(averaged, sizes)
    plot_corruption(averaged)

    print(f"\nSummary written to {SUMMARY_CSV}")
    print(f"Plots written to {MEDIA_DIR}")


if __name__ == "__main__":
    main()
