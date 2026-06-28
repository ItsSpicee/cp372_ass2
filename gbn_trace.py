"""Lightweight, optional tracing hooks for the GBN sender.

By default `trace(...)` is a near-zero-overhead no-op, so production runs and the
unit tests are completely unaffected. A visualizer attaches by setting:

    gbn_trace.EMIT = callback        # callback(event:str, file_id:int, info:dict)
    gbn_trace.PACE = 0.05            # optional: seconds to sleep per event so the
                                     # concurrent thread activity is slow enough
                                     # to watch in a live animation.

`PACE` only takes effect while a callback is attached, and it deliberately runs
*outside* the shared socket lock (because the trace calls in sender_gbn.py are
placed after the `with sock_lock:` blocks), so pacing slows the timeline without
distorting which thread holds the lock when.
"""

import time

EMIT = None   # callable(event, file_id, info) or None
PACE = 0.0    # seconds to sleep per traced event (visual pacing); 0 = full speed


def trace(event, file_id, **info):
    cb = EMIT
    if cb is None:
        return
    cb(event, file_id, info)
    if PACE:
        time.sleep(PACE)
