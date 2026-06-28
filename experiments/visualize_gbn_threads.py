'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: visualize_gbn_threads.py
Description: Live visualization of the real Go-Back-N sender's multithreading. It
             imports the actual send_file_gbn / ack_receiver (sender_gbn.py) and
             receiver_loop (receiver_gbn.py) and runs them in-process over a temp
             UDP port, lightly instrumented via gbn_trace, drawing every thread's
             SEND / WAIT / PROCESS_ACK / RETRANSMIT / RECV / NOTIFY events as a
             swimlane / Gantt timeline.
Capabilities:
    - Record real per-thread protocol events with timestamps
    - Draw a live animated timeline, or render headless to a GIF/PNG
    - Pace concurrency so the genuinely-concurrent threads are watchable

Authors:
    Obeidi, Bassil
    Barghouti, Alaa
    Ozog, Philip
    Soja, Max
    Yamin, Noah
'''

# What you see:
#   * one lane per thread: ack_receiver on top, then sender 0..N-1,
#   * colored bars showing what each thread is doing over time,
#   * orange arrows = the ack_receiver waking a specific sender's Condition,
#   * green SEND bars across senders are serialized by the shared socket lock.
#
# Modes (run from the repo root):
#   python experiments/visualize_gbn_threads.py                 # live animated window
#   python experiments/visualize_gbn_threads.py --files 4
#   python experiments/visualize_gbn_threads.py --gif out.gif   # headless: record a GIF
#
# --pace (default 0.05s) slows each traced step so concurrency is watchable; the
# threads are genuinely running concurrently, just paced. Set --pace 0 for the
# raw, full-speed timeline.

import argparse
import os
import socket
import sys
import tempfile
import threading
import time
import contextlib

# This visualizer lives in experiments/; the protocol code (sender_gbn.py,
# receiver_gbn.py, gbn_trace.py, packet.py) is in the repo root, so put the
# repo root on the import path.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import gbn_trace

# ---- Activity colors -------------------------------------------------------
COLORS = {
    "SEND":        "#2e7d32",  # green  - packet on the wire (held the socket lock)
    "WAIT":        "#cfd8dc",  # grey   - cond.wait(), idle waiting for ACKs
    "PROCESS_ACK": "#1565c0",  # blue   - sliding the window forward on a cumulative ACK
    "RETRANSMIT":  "#c62828",  # red    - timeout, Go-Back-N
    "RECV":        "#6a1b9a",  # purple - ack_receiver pulled an ACK off the socket
    "NOTIFY":      "#ef6c00",  # orange - ack_receiver notified a sender's Condition
}
# Human-readable legend text for each color (the swatch carries the color, so
# the words describe what the thread is actually doing — no color names needed).
LEGEND_LABELS = {
    "SEND":        "Thread: transmitting a packet (holds the shared socket lock)",
    "WAIT":        "Thread: idle — blocked in cond.wait() until an ACK arrives",
    "PROCESS_ACK": "Thread: got a cumulative ACK — sliding the window forward",
    "RETRANSMIT":  "Thread: timeout — Go-Back-N resend of the whole window",
    "RECV":        "Receiver: pulled an ACK packet off the socket",
    "NOTIFY":      "Receiver: woke the matching thread's Condition",
}


def lane_label(name):
    """Map an internal lane key to its intuitive display label."""
    if name == "ack_receiver":
        return "Receiver"
    if name.startswith("sender "):
        return f"Thread {name.split()[1]}"
    return name

# Sender lanes are drawn as continuous state-fill; the receiver lane is drawn as
# discrete ticks (it is mostly idle in select()).
RECEIVER_EVENTS = {"RECV", "NOTIFY"}

# Shared, append-only event log. CPython list.append is atomic, so the trace
# callback (running in worker threads) and the renderer (main thread) need no
# extra locking for this simple producer/consumer.
EVENTS = []          # list of (t_rel, event, file_id, info)
T0 = None            # perf_counter at transfer start


def _attach_tracer(pace):
    def emit(event, file_id, info):
        EVENTS.append((time.perf_counter() - T0, event, file_id, info))
    gbn_trace.EMIT = emit
    gbn_trace.PACE = pace


# ---------------------------------------------------------------------------
# Driving the REAL protocol (mirrors test_parallel_multiplex._run_transfer)
# ---------------------------------------------------------------------------
def _run_receiver(recv_sock, loss_rate, corruption_rate):
    from receiver_gbn import receiver_loop
    while True:
        try:
            receiver_loop(recv_sock, loss_rate, corruption_rate)
            break
        except socket.timeout:
            continue
        except OSError:
            break


def run_transfer(file_paths, recv_dir, loss_rate, corruption_rate, verbose, done_flag):
    """Spin up a real receiver + real sender threads and run a full transfer."""
    from sender_gbn import send_file_gbn, ack_receiver

    host = "127.0.0.1"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind((host, 0))
        port = probe.getsockname()[1]

    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind((host, port))
    recv_sock.settimeout(1.0)

    # The receiver writes incoming files into the current directory.
    os.chdir(recv_dir)

    quiet = contextlib.nullcontext() if verbose else contextlib.redirect_stdout(
        open(os.devnull, "w")
    )

    with quiet:
        threading.Thread(
            target=_run_receiver,
            args=(recv_sock, loss_rate, corruption_rate),
            daemon=True,
        ).start()

        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_addr = (host, port)
        sock_lock = threading.Lock()
        n = len(file_paths)

        ack_states = {}
        for fid in range(n):
            lock = threading.Lock()
            ack_states[fid] = {
                "lock": lock,
                "condition": threading.Condition(lock),
                "ack_queue": [],
            }

        stop_event = threading.Event()
        threading.Thread(
            target=ack_receiver,
            args=(send_sock, corruption_rate, ack_states, stop_event),
            daemon=True,
        ).start()

        workers = [
            threading.Thread(
                target=send_file_gbn,
                args=(send_sock, sock_lock, fp, server_addr,
                      corruption_rate, fid, ack_states),
            )
            for fid, fp in enumerate(file_paths)
        ]
        for t in workers:
            t.start()
        for t in workers:
            t.join(timeout=60)

        stop_event.set()
        time.sleep(0.2)
        send_sock.close()
        recv_sock.close()

    done_flag["t_end"] = time.perf_counter() - T0
    done_flag["done"] = True


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _sender_spans(evts, t_now):
    """Turn a sender lane's point events into contiguous state-fill spans."""
    spans = []
    for i, (t, state, _info) in enumerate(evts):
        # DONE is just a terminator marker: it ends the previous span (because
        # `end` below points at it) and nothing is drawn from it onward.
        if state == "DONE":
            continue
        end = evts[i + 1][0] if i + 1 < len(evts) else t_now
        spans.append((t, max(end - t, 0.0005), state))
    return spans


def draw_frame(ax, lanes, lane_y, t_now, t_end, done, x_max=None):
    import matplotlib.patches as mpatches

    ax.clear()
    bar_h = 0.62
    tick_w = 0.02  # fixed width so receiver ticks don't resize as time advances

    # Bucket events by lane, revealing ONLY what has happened by t_now. This is
    # what makes each frame a true snapshot of the transfer's progress: events
    # in the future (t > t_now) are not drawn yet.
    by_lane = {name: [] for name in lanes}
    notifies = []
    for t, event, fid, info in list(EVENTS):
        if t > t_now:
            continue
        if event in RECEIVER_EVENTS:
            by_lane["ack_receiver"].append((t, event, info))
            if event == "NOTIFY":
                notifies.append((t, fid))
        else:
            by_lane[f"sender {fid}"].append((t, event, info))

    # Sender lanes: continuous state-fill
    for name in lanes:
        if name == "ack_receiver":
            continue
        y = lane_y[name]
        for t, width, state in _sender_spans(by_lane[name], t_now):
            ax.broken_barh([(t, width)], (y - bar_h / 2, bar_h),
                           facecolors=COLORS.get(state, "#999"),
                           edgecolors="white", linewidth=0.4)

    # Receiver lane: discrete ticks
    y = lane_y["ack_receiver"]
    for t, event, _info in by_lane["ack_receiver"]:
        ax.broken_barh([(t, tick_w)], (y - bar_h / 2, bar_h),
                       facecolors=COLORS[event], edgecolors="white", linewidth=0.3)

    # Fan-out arrows: ack_receiver -> the sender it woke
    y_from = lane_y["ack_receiver"]
    for t, fid in notifies:
        to_lane = f"sender {fid}"
        if to_lane not in lane_y:
            continue
        ax.annotate(
            "", xy=(t, lane_y[to_lane] + bar_h / 2),
            xytext=(t, y_from - bar_h / 2),
            arrowprops=dict(arrowstyle="-|>", color="#ff8f00",
                            alpha=0.35, lw=0.8, shrinkA=0, shrinkB=0),
        )

    # "now" marker
    if not done:
        ax.axvline(t_now, color="#222", lw=1.0, alpha=0.5)

    ax.set_yticks([lane_y[n] for n in lanes])
    ax.set_yticklabels([lane_label(n) for n in lanes])
    ax.set_ylim(-0.7, len(lanes) - 0.3)
    ax.set_xlim(0, (x_max if x_max else max(t_now, t_end, 0.5)) * 1.02)
    ax.set_xlabel("time (seconds since transfer start)")
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)

    n_senders = len(lanes) - 1
    status = "complete" if done else "running…"
    ax.figure.suptitle(
        "Go-Back-N — Concurrent Sender Threads Over Time",
        fontsize=14, fontweight="bold", y=0.955,
    )
    ax.set_title(
        f"1 Receiver thread routing ACKs to {n_senders} sender Threads  ·  transfer {status}\n"
        f"each row = one thread    ·    time runs left → right    ·    "
        f"arrows = Receiver waking the Thread an ACK belongs to",
        fontsize=9.5, color="#555", pad=10,
    )

    legend = [mpatches.Patch(facecolor=COLORS[k], label=v) for k, v in LEGEND_LABELS.items()]
    legend.append(mpatches.Patch(
        facecolor="#ff8f00", alpha=0.35,
        label="Receiver → Thread arrow: ACK routed to its file by file_id"))
    ax.legend(handles=legend, ncol=2, loc="upper center",
              bbox_to_anchor=(0.5, -0.14), frameon=False, fontsize=8.5,
              handlelength=1.6, columnspacing=2.5, labelspacing=0.7)


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------
def make_temp_files(work_dir, n, size):
    paths = []
    for i in range(n):
        p = os.path.join(work_dir, f"vis_file{i}.txt")
        # distinct, printable content per file so it's obviously not mixed
        byte = bytes([ord("A") + (i % 26)])
        with open(p, "wb") as f:
            f.write(byte * (size + i * 777))
        paths.append(p)
    return paths


def main():
    global T0
    ap = argparse.ArgumentParser(description="Visualize real GBN sender multithreading")
    ap.add_argument("--files", type=int, default=3, help="concurrent file-sender threads")
    ap.add_argument("--size", type=int, default=9000, help="approx bytes per file")
    ap.add_argument("--loss-rate", type=float, default=0.08,
                    help="receiver-side packet loss (drives retransmissions)")
    ap.add_argument("--pace", type=float, default=0.05,
                    help="seconds of pacing per traced step (0 = full speed)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--verbose", action="store_true", help="show raw protocol logs")
    ap.add_argument("--gif", type=str, default=None,
                    help="headless: record the animation to this .gif instead of showing")
    ap.add_argument("--png", type=str, default="gbn_threads.png",
                    help="path for the final still snapshot")
    args = ap.parse_args()

    import random
    random.seed(args.seed)

    # Backend: Agg for headless gif/png-only, default interactive for live.
    import matplotlib
    if args.gif:
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation

    orig_dir = os.getcwd()
    work_dir = tempfile.mkdtemp(prefix="gbn_vis_")
    recv_dir = os.path.join(work_dir, "received")
    os.makedirs(recv_dir)
    file_paths = make_temp_files(work_dir, args.files, args.size)
    png_path = os.path.abspath(args.png)
    gif_path = os.path.abspath(args.gif) if args.gif else None

    lanes = ["ack_receiver"] + [f"sender {i}" for i in range(args.files)]
    lane_y = {name: i for i, name in enumerate(reversed(lanes))}

    _attach_tracer(args.pace)
    done_flag = {"done": False, "t_end": 0.0}

    fig, ax = plt.subplots(figsize=(14, 1.8 + 0.85 * len(lanes)))
    # Reserve room: top for the bold title + grey subtitle, bottom for the legend.
    fig.subplots_adjust(top=0.80, bottom=0.30, left=0.08, right=0.985)

    try:
        if gif_path:
            # ---- Headless: run the transfer fully, then replay to a GIF ----
            T0 = time.perf_counter()
            run_transfer(file_paths, recv_dir, args.loss_rate, 0.0,
                         args.verbose, done_flag)
            os.chdir(orig_dir)
            t_end = done_flag["t_end"]
            fps = 20
            x_max = t_end + 0.6
            n_frames = int((t_end + 0.6) * fps) + 1

            def update(i):
                t = i / fps
                draw_frame(ax, lanes, lane_y, t, t_end, done=t >= t_end, x_max=x_max)

            anim = FuncAnimation(fig, update, frames=n_frames, interval=1000 / fps)
            from matplotlib.animation import PillowWriter
            anim.save(gif_path, writer=PillowWriter(fps=fps))
            draw_frame(ax, lanes, lane_y, t_end, t_end, done=True)
            fig.savefig(png_path, dpi=130, bbox_inches="tight")
            print(f"Saved animation : {gif_path}")
            print(f"Saved snapshot  : {png_path}")
        else:
            # ---- Live: run the transfer concurrently with the animation ----
            T0 = time.perf_counter()
            driver = threading.Thread(
                target=run_transfer,
                args=(file_paths, recv_dir, args.loss_rate, 0.0,
                      args.verbose, done_flag),
                daemon=True,
            )
            driver.start()

            def update(_frame):
                done = done_flag["done"]
                t_now = done_flag["t_end"] if done else time.perf_counter() - T0
                draw_frame(ax, lanes, lane_y, t_now, done_flag["t_end"], done)
                if done and not getattr(update, "_saved", False):
                    os.chdir(orig_dir)
                    fig.savefig(png_path, dpi=130, bbox_inches="tight")
                    print(f"Transfer complete. Saved snapshot: {png_path}")
                    update._saved = True
                return []

            anim = FuncAnimation(fig, update, interval=80, cache_frame_data=False)
            fig._keep_anim = anim  # keep a reference alive
            plt.show()
    finally:
        os.chdir(orig_dir)
        gbn_trace.EMIT = None
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)

    # Brief textual summary
    from collections import Counter
    counts = Counter(e[1] for e in EVENTS)
    print("Traced events:", dict(counts))


if __name__ == "__main__":
    main()
