'''
CP372 - Computer Networks, Spring 2026
Assignment 2: Reliable Data Transfer over UDP

Script Name: gbn_trace.py
Description: Lightweight, optional tracing hooks for the Go-Back-N sender. By
             default trace() is a near-zero-overhead no-op, so the protocol runs
             and the unit tests are completely unaffected. A visualizer attaches
             by setting EMIT (a callback) and, optionally, PACE (seconds to sleep
             per event so concurrent thread activity is slow enough to watch).
             This module has no main() and is never run on its own; it is
             imported by sender_gbn.py and the GBN thread visualizer.
Capabilities:
    - Emit a trace event to an attached callback, or do nothing if none is set
    - Pace each traced event for live animation without distorting lock timing

Authors:
    Obeidi, Bassil
    Barghouti, Alaa
    Ozog, Philip
    Soja, Max
    Yamin, Noah
'''

import time

EMIT = None   # callback(event, file_id, info), or None when no visualizer is attached
PACE = 0.0    # seconds to sleep per traced event (visual pacing); 0 = full speed


def trace(event, file_id, **info):
    """Forward one trace event to the attached callback, then optionally pace."""
    cb = EMIT
    if cb is None:
        return
    cb(event, file_id, info)
    if PACE:
        time.sleep(PACE)
