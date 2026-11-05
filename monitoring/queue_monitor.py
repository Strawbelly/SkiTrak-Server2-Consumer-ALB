#!/usr/bin/env python3
"""
Samples RabbitMQ resort-queue metrics into a CSV file.

Each row covers one sampling interval, summed over the resort queues (skitrak.resort.*):
  - depth:        messages in the queues (ready + unacknowledged); the official depth,
                  as RabbitMQ's console shows it
  - ready:        messages waiting to be delivered (real backlog)
  - unacked:      messages delivered to consumers but not yet acknowledged (prefetched)
  - publish_rate: producer rate, messages/s published by the ingest servers
  - ack_rate:     consumer rate, messages/s acknowledged by the consumer
  - est_drain_ms: ESTIMATED time to drain the current backlog, depth / ack_rate.
                  This is not measured enqueue-to-consumption latency; it is only an
                  indication of how far the consumer is behind.

Runs until interrupted (Ctrl-C or SIGTERM).

Usage:
  queue_monitor.py --rabbit http://HOST:15672 --out queue.csv
"""
import argparse
import base64
import csv
import json
import signal
import sys
import time
import urllib.parse
import urllib.request

RESORT_PREFIX = "skitrak.resort."  # consumer.Topology.RESORT_QUEUE_PREFIX
DLQ = "skitrak.dlq"

FIELDS = ["ts", "elapsed_s", "depth", "ready", "unacked", "publish_rate", "ack_rate", "est_drain_ms"]

stop = False


def on_signal(*_):
    global stop
    stop = True


def basic_auth(user, password):
    return base64.b64encode(f"{user}:{password}".encode()).decode()


def get_json(url, auth=None, timeout=3):
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + auth)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # The consumer returns 503 with a JSON body when it is unhealthy.
        return json.loads(e.read())


def rate(q, key):
    return q.get("message_stats", {}).get(key + "_details", {}).get("rate", 0.0)


def sample_rabbit(base, auth, vhost):
    """Resort-queue totals, plus the dead-letter queue depth used by the swipe-loss check."""
    queues = get_json(base + "/api/queues/" + urllib.parse.quote(vhost, safe=""), auth)
    row = {"depth": 0, "ready": 0, "unacked": 0, "publish_rate": 0.0, "ack_rate": 0.0, "dlq_depth": 0}
    for q in queues:
        if q["name"].startswith(RESORT_PREFIX):
            row["depth"] += q.get("messages", 0)
            row["ready"] += q.get("messages_ready", 0)
            row["unacked"] += q.get("messages_unacknowledged", 0)
            row["publish_rate"] += rate(q, "publish")
            row["ack_rate"] += rate(q, "ack")
        elif q["name"] == DLQ:
            row["dlq_depth"] = q.get("messages", 0)
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rabbit", required=True, help="management API base, e.g. http://10.0.0.5:15672")
    p.add_argument("--user", default="guest")
    p.add_argument("--password", default="guest")
    p.add_argument("--vhost", default="/")
    # RabbitMQ refreshes queue statistics every 5 s (collect_statistics_interval), so
    # sampling faster only repeats values.
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--out", required=True)
    p.add_argument("--quiet", action="store_true", help="do not print a status line per sample")
    args = p.parse_args()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    auth = basic_auth(args.user, args.password)
    start = time.time()

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        while not stop:
            t = time.time()
            try:
                row = sample_rabbit(args.rabbit, auth, args.vhost)
                # Estimate only: time to drain the backlog at the current ack rate.
                if row["ack_rate"] > 0:
                    row["est_drain_ms"] = f"{1000.0 * row['depth'] / row['ack_rate']:.1f}"
                elif row["depth"] == 0:
                    row["est_drain_ms"] = "0"
                row.update(ts=f"{t:.3f}", elapsed_s=f"{t - start:.1f}")
                w.writerow(row)
                f.flush()
                if not args.quiet:
                    print(f"[{row['elapsed_s']:>7}s] depth={row['depth']:>6} pub={row['publish_rate']:8.1f}/s "
                          f"ack={row['ack_rate']:8.1f}/s est_drain~{row.get('est_drain_ms', '?')}ms", flush=True)
            except Exception as e:
                print(f"rabbit sample failed: {e}", file=sys.stderr)
            # Sleep in short steps so a stop signal is handled promptly.
            deadline = t + args.interval
            while not stop and time.time() < deadline:
                time.sleep(0.1)


if __name__ == "__main__":
    main()
