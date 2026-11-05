#!/usr/bin/env python3
"""
Summarizes a run directory written by run_test.py into summary.json, and rebuilds
results/summary.csv with one row per run, so runs can be compared side by side.

Sources (each metric comes from exactly one of them):
  client.log        runtime, throughput, failures, retries, ingest latency, leaderboard updates
  queue.csv         queue depth time series (5 s samples), producer rate, consumer rate
  run.json          before/after consumer counters (loss check, retries, ack failures),
                    measured consumer lag of the main phase, POSTs per ingest server
  hosts/<name>.csv  CPU, memory, network per host

Queue and host statistics cover the load window, from client start until the queues
drain. Averages of producer rate and queue depth use only samples where swipes were
being published, and the consumer-rate average only samples where messages were being
acknowledged, so idle time (JVM start, subscriber connect, drain tail) does not dilute them.

Consumer lag (lag_*_ms) is measured: the consumer records processing start minus the
ingest server's publish time (x-publish-ms header) per message, for messages published
in the main phase only (warmup excluded by publish time). est_drain_*_ms (depth / ack
rate) is only an estimate of backlog drain time and is never used as lag.

Part 4 targets, per run: target_depth (queue_depth_max < 1000), target_lag
(lag_p99_ms < 100; NOT MEASURED without samples or if more than 1% of processed messages
had no publish header), target_loss (loss_check PASS). targets_overall is PASS only if
all three pass. Queue depth is sampled every 5 s (RabbitMQ's statistics interval), so a
spike shorter than that can be missed.

--compare writes results/comparison.csv and picks the best run per round. The round is
the label prefix before the first "-" (e.g. r1-c064-k10 -> r1). Rule:
  1. qualifying runs: all three targets PASS, client_failed 0, consumer threads confirmed
  2. highest client_throughput wins
  3. among runs within 5% of it: in round r2 the fewest consumer threads, then the
     lowest latency_p99_ms, then the lowest queue_depth_max
  If no run qualifies, the best run that only fails targets is shown, marked as such.

Usage:
  summarize.py results/<label> [results/<label2> ...]   # recompute these runs
  summarize.py --all results                            # recompute every run in results/
  summarize.py --compare results                        # comparison table and best runs
"""
import argparse
import csv
import glob
import json
import math
import os
import re

QUEUE_DEPTH_TARGET = 1000
LAG_P99_TARGET_MS = 100
TIE_MARGIN = 0.05


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def stats(values):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return {"avg": None, "max": None, "p95": None}
    p95 = vals[max(0, math.ceil(0.95 * len(vals)) - 1)]
    return {"avg": round(sum(vals) / len(vals), 2), "max": round(vals[-1], 2), "p95": round(p95, 2)}


def read_csv(path):
    if not os.path.isfile(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def in_window(rows, start, end):
    return [r for r in rows if num(r.get("ts")) is not None and start <= num(r["ts"]) <= end]


def write_distribution(run_dir, posts):
    total = sum(posts.values())
    with open(os.path.join(run_dir, "distribution.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["server", "post_requests", "share_pct"])
        for name in sorted(posts):
            w.writerow([name, posts[name], f"{100.0 * posts[name] / total:.1f}" if total else ""])


def parse_client(path):
    if not os.path.isfile(path):
        return {}
    text = open(path).read()
    c = {}

    def grab(pattern, key, cast=float):
        m = re.search(pattern, text, re.M)
        if m:
            c[key] = cast(m.group(1))

    grab(r"^Subscribers connected: (\d+)/", "subscribers_connected", int)
    grab(r"^Successful swipes:\s+(\d+)", "client_success", int)
    grab(r"^Failed swipes:\s+(\d+)", "client_failed", int)
    c["client_retries"] = sum(int(x) for x in re.findall(r"^Retries:\s+(\d+)", text, re.M))
    grab(r"^Total runtime:\s+([\d.]+) s", "client_runtime_s")
    grab(r"^Overall throughput:\s+([\d.]+)", "client_throughput")

    lat = re.search(r"=== Ingest Latency.*?^mean ([\d.]+), median ([\d.]+), p95 ([\d.]+), p99 ([\d.]+)",
                    text, re.S | re.M)
    if lat:
        for i, k in enumerate(["mean", "median", "p95", "p99"], 1):
            c[f"latency_{k}_ms"] = float(lat.group(i))

    # Confirms subscribers receive real leaderboard updates.
    grab(r"^Total leaderboard updates delivered: (\d+)", "leaderboard_updates", int)
    sub = text.split("=== Subscribe Results ===", 1)
    if len(sub) == 2:
        # Rows: subscriber, resort, updates, connect(ms), drops, reconnects
        updates = [int(u) for u in re.findall(r"^\s*\d+\s+\d+\s+(\d+)\s+\S+\s+\d+\s+\d+\s*$", sub[1], re.M)]
        c["subscribers_without_updates"] = sum(1 for u in updates if u == 0)
    return c


def depth_profile(depths, publishing_depths):
    """Queue profile label. FROZEN: change only on request.

    All thresholds are relative to the peak depth while swipes were being published.
      flat      publishing peak below 200 messages, whatever the shape
      sawtooth  peak >= 200 and the depth rose above 50% of the peak and fell below 20%
                of it at least twice (counted over the whole load window)
      plateau   peak >= 200 and at least half of the publishing samples are above 50%
                of the peak (a sustained level)
      flat      anything else
    "+spike" is added to flat when exactly one publishing sample is above 50% of a
    publishing peak of at least 50 messages.
    """
    peak = max(publishing_depths, default=0)
    above = sum(d > 0.5 * peak for d in publishing_depths)
    flat = "flat+spike" if peak >= 50 and above == 1 else "flat"
    if peak < 200:
        return flat
    cycles, high = 0, False
    for d in depths:
        if not high and d > 0.5 * peak:
            high = True
        elif high and d < 0.2 * peak:
            high = False
            cycles += 1
    if cycles >= 2:
        return "sawtooth"
    if above >= len(publishing_depths) / 2:
        return "plateau"
    return flat


def write_depth_plot(run_dir, rows, run, profile):
    """queue_depth.svg: total resort-queue depth over time, with the 1000-message target."""
    start = run.get("client_start")
    pts = [(num(r["ts"]) - start, num(r["depth"])) for r in rows
           if start is not None and num(r.get("ts")) is not None and num(r.get("depth")) is not None]
    if not pts:
        return
    w, h, left, bottom, top, right = 720, 300, 60, 40, 30, 20
    tmax = max(t for t, _ in pts) or 1
    ymax = max(max(d for _, d in pts), QUEUE_DEPTH_TARGET) * 1.1

    def x(t):
        return left + (w - left - right) * max(t, 0) / tmax

    def y(d):
        return h - bottom - (h - bottom - top) * d / ymax

    line = " ".join(f"{x(t):.1f},{y(d):.1f}" for t, d in pts)
    title = f"{run.get('label')} - queue depth ({profile}), sampled every {run.get('interval_s')} s"
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" font-family="sans-serif" font-size="11">',
           f'<rect width="{w}" height="{h}" fill="white"/>',
           f'<text x="{left}" y="18" font-size="13">{title}</text>',
           f'<line x1="{left}" y1="{h - bottom}" x2="{w - right}" y2="{h - bottom}" stroke="#333"/>',
           f'<line x1="{left}" y1="{top}" x2="{left}" y2="{h - bottom}" stroke="#333"/>',
           f'<line x1="{left}" y1="{y(QUEUE_DEPTH_TARGET):.1f}" x2="{w - right}" y2="{y(QUEUE_DEPTH_TARGET):.1f}" '
           f'stroke="#c0392b" stroke-dasharray="4 3"/>',
           f'<text x="{w - right}" y="{y(QUEUE_DEPTH_TARGET) - 4:.1f}" text-anchor="end" fill="#c0392b">target 1000</text>',
           f'<polyline points="{line}" fill="none" stroke="#2e6fbd" stroke-width="1.5"/>',
           f'<text x="{(w + left) / 2}" y="{h - 8}" text-anchor="middle">seconds since client start</text>',
           f'<text x="14" y="{(h - bottom + top) / 2}" transform="rotate(-90 14 {(h - bottom + top) / 2})" '
           f'text-anchor="middle">messages</text>']
    for frac in (0, 0.5, 1):
        svg.append(f'<text x="{left - 6}" y="{y(ymax * frac / 1.1) + 4:.1f}" text-anchor="end">{ymax * frac / 1.1:.0f}</text>')
        svg.append(f'<text x="{x(tmax * frac):.1f}" y="{h - bottom + 14}" text-anchor="middle">{tmax * frac:.0f}</text>')
    svg.append('</svg>')
    with open(os.path.join(run_dir, "queue_depth.svg"), "w") as f:
        f.write("\n".join(svg) + "\n")


def summarize_queue(rows, run):
    start, client_end = run.get("client_start"), run.get("client_end")
    end = run.get("drain_end", client_end)
    if not rows or start is None:
        return {}
    load = in_window(rows, start, end)
    client = in_window(rows, start, client_end)
    publishing = [r for r in client if (num(r.get("publish_rate")) or 0) > 0]
    acking = [r for r in load if (num(r.get("ack_rate")) or 0) > 0]
    depths = [num(r["depth"]) for r in load if num(r.get("depth")) is not None]
    drain = stats(num(r.get("est_drain_ms")) for r in client)
    q = {
        "queue_samples": len(depths),
        "queue_depth_max": max(depths, default=None),
        "queue_depth_p95": stats(depths)["p95"],
        # Split of the total: real backlog (ready) vs prefetched to consumers (unacked).
        # None for runs recorded before these columns existed.
        "queue_ready_max": stats(num(r.get("ready")) for r in load)["max"],
        "queue_unacked_max": stats(num(r.get("unacked")) for r in load)["max"],
        "queue_depth_avg": stats(num(r.get("depth")) for r in publishing)["avg"],
        "queue_depth_profile": depth_profile(
            depths, [num(r["depth"]) for r in publishing if num(r.get("depth")) is not None]) if depths else None,
        "producer_rate_avg": stats(num(r.get("publish_rate")) for r in publishing)["avg"],
        "consumer_rate_avg": stats(num(r.get("ack_rate")) for r in acking)["avg"],
        "est_drain_avg_ms": drain["avg"],
        "est_drain_p95_ms": drain["p95"],
    }
    if client_end and end:
        q["drain_time_s"] = round(end - client_end, 1)
    return q


def summarize_loss(run, client):
    before, after = run.get("before", {}), run.get("after", {})

    def delta(key):
        a, b = num(after.get(key)), num(before.get(key))
        return None if a is None or b is None else int(a - b)

    processed = delta("swipes_processed")
    out = {
        "consumer_processed": processed,
        "consumer_duplicates": delta("duplicates"),
        "consumer_dead_lettered": delta("dead_lettered"),
        "consumer_retries": delta("retries"),
        "consumer_ack_failures": delta("ack_failures"),
        "dlq_added": delta("dlq_depth"),
        "queue_depth_after": after.get("queue_depth"),
    }
    success = client.get("client_success")
    if processed is not None and processed < 0:
        out["loss_check"] = "consumer restarted during run; counters reset"
    elif processed is not None and success is not None:
        # Positive: acknowledged by the server but never applied by the consumer.
        out["swipes_missing"] = success - processed
        out["loss_check"] = ("PASS" if success - processed <= 0 and not out["dlq_added"]
                             and not out["queue_depth_after"] else "FAIL")
    return out


def summarize_lag(run):
    """Measured lag of the main phase, from the consumer's /lag window query."""
    lag = run.get("lag") or {}
    before, after = run.get("before", {}), run.get("after", {})
    out = {
        "lag_samples": lag.get("samples"),
        "lag_p50_ms": lag.get("p50Ms"),
        "lag_p99_ms": lag.get("p99Ms"),
        "lag_max_ms": lag.get("maxMs"),
    }
    for key in ("lag_missing_header", "lag_negative"):
        a, b = num(after.get(key)), num(before.get(key))
        out[key] = None if a is None or b is None else int(a - b)
    return out


def targets(summary):
    """PASS/FAIL per Part 4 target; lag is NOT MEASURED when it cannot be trusted."""
    depth = summary.get("queue_depth_max")
    t_depth = "NOT MEASURED" if depth is None else ("PASS" if depth < QUEUE_DEPTH_TARGET else "FAIL")

    p99, n = summary.get("lag_p99_ms"), summary.get("lag_samples") or 0
    missing, processed = summary.get("lag_missing_header"), summary.get("consumer_processed") or 0
    if p99 is None or n == 0 or missing is None or (processed and missing > 0.01 * processed):
        t_lag = "NOT MEASURED"
    else:
        t_lag = "PASS" if p99 < LAG_P99_TARGET_MS else "FAIL"

    t_loss = "PASS" if summary.get("loss_check") == "PASS" else "FAIL"
    results = (t_depth, t_lag, t_loss)
    overall = "PASS" if all(r == "PASS" for r in results) else "FAIL"
    return {"target_depth": t_depth, "target_lag": t_lag, "target_loss": t_loss, "targets_overall": overall}


def summarize_hosts(run_dir, run):
    start, end = run.get("client_start"), run.get("drain_end", run.get("client_end"))
    h = {}
    if start is None:
        return h
    for path in sorted(glob.glob(os.path.join(run_dir, "hosts", "*.csv"))):
        name = os.path.splitext(os.path.basename(path))[0]
        rows = in_window(read_csv(path), start, end)
        if not rows:
            continue
        cpu = stats(num(r["cpu_pct"]) for r in rows)
        h[f"{name}_cpu_avg"], h[f"{name}_cpu_max"] = cpu["avg"], cpu["max"]
        h[f"{name}_steal_max"] = stats(num(r["steal_pct"]) for r in rows)["max"]
        h[f"{name}_mem_used_pct_max"] = stats(num(r["mem_used_pct"]) for r in rows)["max"]
        h[f"{name}_net_rx_kbps_avg"] = stats(num(r["net_rx_kbps"]) for r in rows)["avg"]
        h[f"{name}_net_tx_kbps_avg"] = stats(num(r["net_tx_kbps"]) for r in rows)["avg"]
    return h


def summarize_run(run_dir):
    with open(os.path.join(run_dir, "run.json")) as f:
        run = json.load(f)
    client = parse_client(os.path.join(run_dir, "client.log"))
    summary = {
        "label": run["label"], "notes": run.get("notes", ""),
        "ingest_servers": len(run.get("ingest_servers", [])), "threads": run["threads"], "swipes": run["swipes"],
    }
    before_t = run.get("before", {}).get("consumer_threads")
    after_t = run.get("after", {}).get("consumer_threads")
    expected = run.get("consumer_threads_expected")
    summary["consumer_threads"] = before_t
    # Confirmed when the live count was the same at start and end, and matched the intended one.
    summary["consumer_threads_ok"] = (before_t is not None and before_t == after_t
                                      and (expected is None or before_t == expected))
    summary.update(client)
    queue_rows = read_csv(os.path.join(run_dir, "queue.csv"))
    summary.update(summarize_queue(queue_rows, run))
    summary.update(summarize_loss(run, client))
    summary.update(summarize_lag(run))
    summary.update(targets(summary))
    if summary.get("queue_depth_profile"):
        write_depth_plot(run_dir, queue_rows, run, summary["queue_depth_profile"])

    posts = run.get("posts_per_server") or {}
    total = sum(posts.values())
    for name, n in sorted(posts.items()):
        summary[f"{name}_requests"] = n
        summary[f"{name}_share_pct"] = round(100.0 * n / total, 1) if total else None
    summary.update(summarize_hosts(run_dir, run))

    with open(os.path.join(run_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[summarize] {run['label']}: throughput={summary.get('client_throughput')}/s "
          f"failed={summary.get('client_failed')} depth_max={summary.get('queue_depth_max')} "
          f"(ready_max={summary.get('queue_ready_max')} unacked_max={summary.get('queue_unacked_max')}) "
          f"({summary.get('queue_depth_profile')}) lag_p99={summary.get('lag_p99_ms')}ms "
          f"targets depth={summary['target_depth']} lag={summary['target_lag']} loss={summary['target_loss']} "
          f"-> {summary['targets_overall']}")
    return summary


def rebuild_index(results_dir):
    """Rewrites results/summary.csv from every run's summary.json."""
    rows = []
    for path in sorted(glob.glob(os.path.join(results_dir, "*", "summary.json")), key=os.path.getmtime):
        with open(path) as f:
            rows.append(json.load(f))
    if not rows:
        return
    # Runs monitor different host sets, so the header is the union of all keys in first-seen order.
    fields = []
    for r in rows:
        fields += [k for k in r if k not in fields]
    out = os.path.join(results_dir, "summary.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"[summarize] {len(rows)} runs written to {out}")


COMPARE_FIELDS = [
    "round", "label", "notes", "threads", "consumer_threads", "qualified", "not_qualified_because",
    "targets_overall", "target_depth", "target_lag", "target_loss",
    "client_success", "client_failed", "client_retries", "client_runtime_s", "client_throughput",
    "latency_mean_ms", "latency_p95_ms", "latency_p99_ms",
    "producer_rate_avg", "consumer_rate_avg",
    "queue_depth_max", "queue_ready_max", "queue_unacked_max", "queue_depth_p95", "queue_depth_avg",
    "queue_depth_profile",
    "lag_samples", "lag_p50_ms", "lag_p99_ms", "lag_max_ms",
    "consumer_processed", "consumer_duplicates", "consumer_retries", "consumer_ack_failures",
    "consumer_dead_lettered", "dlq_added", "loss_check", "leaderboard_updates",
    "est_drain_p95_ms",
]


def qualification(r, require_targets=True):
    reasons = []
    if require_targets and r.get("targets_overall") != "PASS":
        reasons.append("targets: depth={} lag={} loss={}".format(
            r.get("target_depth"), r.get("target_lag"), r.get("target_loss")))
    if r.get("loss_check") != "PASS":
        reasons.append(f"loss_check={r.get('loss_check')}")
    if r.get("client_failed") != 0:
        reasons.append(f"client_failed={r.get('client_failed')}")
    if not r.get("consumer_threads_ok"):
        reasons.append("consumer threads not confirmed")
    return reasons


def best_run(rows, round_name, require_targets=True):
    ok = [r for r in rows if not qualification(r, require_targets) and r.get("client_throughput")]
    if not ok:
        return None
    top = max(r["client_throughput"] for r in ok)
    close = [r for r in ok if r["client_throughput"] >= (1 - TIE_MARGIN) * top]
    big = float("inf")
    return min(close, key=lambda r: (
        r.get("consumer_threads") or big if round_name == "r2" else 0,
        r.get("latency_p99_ms") if r.get("latency_p99_ms") is not None else big,
        r.get("queue_depth_max") if r.get("queue_depth_max") is not None else big))


def compare(results_dir):
    """Writes results/comparison.csv and prints the best run per round."""
    rows = []
    for path in glob.glob(os.path.join(results_dir, "*", "summary.json")):
        with open(path) as f:
            rows.append(json.load(f))
    if not rows:
        print(f"[compare] no runs in {results_dir}")
        return
    for r in rows:
        r["round"] = r["label"].split("-", 1)[0]
    rows.sort(key=lambda r: (r["round"], r.get("threads") or 0, r.get("consumer_threads") or 0, r["label"]))

    # Host CPU and memory columns differ per run set, so add whichever are present.
    host_cols = sorted({k for r in rows for k in r if k.endswith(("_cpu_avg", "_cpu_max", "_mem_used_pct_max"))})
    out = os.path.join(results_dir, "comparison.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COMPARE_FIELDS + host_cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            reasons = qualification(r)
            w.writerow(dict(r, qualified=not reasons, not_qualified_because="; ".join(reasons)))
    print(f"[compare] {len(rows)} runs written to {out}")

    for round_name in sorted({r["round"] for r in rows}):
        group = [r for r in rows if r["round"] == round_name]
        print(f"\n=== {round_name} ===")
        print(f"{'label':22s} {'client':>6s} {'cons':>4s} {'swipes/s':>9s} {'p95/p99 ms':>11s} "
              f"{'fail':>4s} {'retry':>5s} {'pub/s':>8s} {'ack/s':>8s} {'dmax':>6s} {'dp95':>6s} "
              f"{'profile':>8s} {'lag99':>6s}  depth/lag/loss")
        for r in group:
            reasons = qualification(r)
            lat = f"{r.get('latency_p95_ms')}/{r.get('latency_p99_ms')}"
            print(f"{r['label']:22s} {r.get('threads')!s:>6s} {r.get('consumer_threads')!s:>4s} "
                  f"{r.get('client_throughput')!s:>9s} {lat:>11s} "
                  f"{r.get('client_failed')!s:>4s} {r.get('client_retries')!s:>5s} "
                  f"{r.get('producer_rate_avg')!s:>8s} {r.get('consumer_rate_avg')!s:>8s} "
                  f"{r.get('queue_depth_max')!s:>6s} {r.get('queue_depth_p95')!s:>6s} "
                  f"{r.get('queue_depth_profile')!s:>8s} {r.get('lag_p99_ms')!s:>6s}  "
                  f"{r.get('target_depth')}/{r.get('target_lag')}/{r.get('target_loss')}"
                  + (f"  (excluded: {'; '.join(reasons)})" if reasons else ""))
        best = best_run(group, round_name)
        if best:
            print(f"best: {best['label']} (client threads {best.get('threads')}, "
                  f"consumer threads {best.get('consumer_threads')}, {best.get('client_throughput')} swipes/s, "
                  f"all three targets PASS)")
        else:
            fallback = best_run(group, round_name, require_targets=False)
            print("best: none met all three targets" + (
                f"; best run without the target requirement: {fallback['label']} "
                f"({fallback.get('client_throughput')} swipes/s)" if fallback else ""))
    print("\nlag_* is measured (processing start - publish time, main phase only). "
          "est_drain_p95_ms is only an ESTIMATE of backlog drain time, not lag.")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("runs", nargs="*", help="run directories")
    p.add_argument("--all", metavar="RESULTS_DIR", help="summarize every run under this directory")
    p.add_argument("--compare", metavar="RESULTS_DIR", help="write comparison.csv and print the best run per round")
    args = p.parse_args()

    if args.compare:
        compare(args.compare)
        return

    dirs = args.runs
    if args.all:
        dirs = sorted(os.path.dirname(x) for x in glob.glob(os.path.join(args.all, "*", "run.json")))
    if not dirs:
        p.error("pass run directories or --all RESULTS_DIR")
    for d in dirs:
        summarize_run(d)
    for parent in {os.path.dirname(os.path.abspath(d)) for d in dirs}:
        rebuild_index(parent)


if __name__ == "__main__":
    main()
