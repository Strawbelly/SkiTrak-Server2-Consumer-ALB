#!/usr/bin/env python3
"""
Runs one load test and collects client, queue, and system metrics into results/<label>/.

Steps:
  1. Record baselines: consumer /health counters, dead-letter queue depth, and the
     POST count in each ingest server's Tomcat access log.
  2. Start host_monitor.py on every host over SSH, and queue_monitor.py locally.
  3. Run the client jar (stdout saved to client.log; per-swipe CSV and chart are
     written to the run directory because the client writes to ./results).
  4. After the client exits, keep monitoring until the resort queues drain, so the
     consumer's catch-up and the final swipe count are captured.
  5. Stop the monitors, copy host CSVs back, record per-server request counts,
     fetch CloudWatch metrics (if the aws CLI is available), and summarize.

Output in results/<label>/:
  run.json            parameters, timestamps, baseline and final counters
  client.log          client stdout (throughput, failures, retries, latency, subscribers)
  results/            the client's own per_swipe_metrics.csv and throughput chart
  queue.csv           queue_monitor.py samples
  hosts/<name>.csv    host_monitor.py samples for each host
  distribution.csv    POST requests handled by each ingest server
  cloudwatch.csv      ALB-side metrics (ELB/target 5XX, healthy targets, response time)
  summary.json        one-row summary (also merged into results/summary.csv)

Usage:
  run_test.py --config test_config.json --label lb4-500k \
      --base-url http://<alb-dns>/ski-servlet --threads 256 --swipes 532000 \
      --ingest server1,server2,server3,server4
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

# The client always sends this many warmup swipes first; they are excluded from lag.
WARMUP_SWIPES = 32_000
sys.path.insert(0, HERE)
import queue_monitor  # noqa: E402
import summarize  # noqa: E402


class Remote:
    def __init__(self, cfg):
        s = cfg.get("ssh", {})
        self.user = s.get("user", "ec2-user")
        self.key = os.path.expanduser(s["key"]) if s.get("key") else None
        self.options = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10"] + s.get("options", [])
        self.remote_dir = cfg.get("remote_dir", "/tmp/skitrak-monitor")

    def _base(self, prog):
        cmd = [prog] + self.options
        if self.key:
            cmd += ["-i", self.key]
        return cmd

    def run(self, host, command, check=True):
        cmd = self._base("ssh") + [f"{host.get('user', self.user)}@{host['address']}", command]
        return subprocess.run(cmd, check=check, capture_output=True, text=True, timeout=60).stdout.strip()

    def put(self, host, local, remote):
        cmd = self._base("scp") + [local, f"{host.get('user', self.user)}@{host['address']}:{remote}"]
        subprocess.run(cmd, check=True, capture_output=True, timeout=60)

    def get(self, host, remote, local, recursive=False):
        cmd = self._base("scp") + (["-r"] if recursive else []) + [
            f"{host.get('user', self.user)}@{host['address']}:{remote}", local]
        subprocess.run(cmd, check=True, capture_output=True, timeout=300)

    def popen(self, host, command, **kwargs):
        """Starts a remote command and returns the local ssh process (for streaming output)."""
        cmd = self._base("ssh") + [f"{host.get('user', self.user)}@{host['address']}", command]
        return subprocess.Popen(cmd, **kwargs)


def log(msg):
    print(f"[run_test] {msg}", flush=True)


def counters(cfg, auth):
    """Snapshot of the counters used for the swipe-loss check.
    This is the only place consumer /health is read; queue_monitor.py reads RabbitMQ only."""
    snap = {"ts": time.time()}
    try:
        r = queue_monitor.sample_rabbit(cfg["rabbit"]["api"], auth, cfg["rabbit"].get("vhost", "/"))
        snap.update(queue_depth=r["depth"], dlq_depth=r["dlq_depth"])
    except Exception as e:
        log(f"RabbitMQ snapshot failed: {e}")
    if cfg.get("consumer_health"):
        try:
            h = queue_monitor.get_json(cfg["consumer_health"] + "/health")
            snap.update(swipes_processed=h.get("swipesProcessed"), duplicates=h.get("duplicates"),
                        dead_lettered=h.get("deadLettered"), consumer_threads=h.get("threads"),
                        retries=h.get("retries"), ack_failures=h.get("ackFailures"),
                        lag_missing_header=h.get("lagMissingHeader"), lag_negative=h.get("lagNegative"))
        except Exception as e:
            log(f"consumer snapshot failed: {e}")
    return snap


def main_phase_window(run_dir):
    """Publish-time window of the main phase, from the client's per-swipe CSV.

    Rows are written warmup first, then main. The window starts one second after the
    first main send, so no warmup message falls in it even with a small clock offset
    between this machine and the servers, and ends one second after the last main
    response, so the final partial second is included.
    """
    path = os.path.join(run_dir, "results", "per_swipe_metrics.csv")
    first = last = None
    with open(path, newline="") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if i < WARMUP_SWIPES:
                continue
            sent = int(row["timestamp"])
            done = sent + float(row["latency"])
            first = sent if first is None else min(first, sent)
            last = done if last is None else max(last, done)
    if first is None:
        return None
    return first + 1000, int(last) + 1000


def post_counts(remote, hosts, glob):
    """Total POST lines across each ingest server's Tomcat access logs."""
    counts = {}
    for h in hosts:
        try:
            # grep -c exits 1 when there are no matches; "|| true" keeps the count of 0.
            out = remote.run(h, f"cat {glob} 2>/dev/null | grep -c '\"POST ' || true")
            counts[h["name"]] = int(out or 0)
        except Exception as e:
            log(f"access log count failed on {h['name']}: {e}")
    return counts


def start_host_monitors(remote, hosts, label, interval):
    pids = {}
    script = os.path.join(HERE, "host_monitor.py")
    for h in hosts:
        try:
            remote.run(h, f"mkdir -p {remote.remote_dir}")
            remote.put(h, script, f"{remote.remote_dir}/host_monitor.py")
            out = remote.run(h, f"nohup python3 {remote.remote_dir}/host_monitor.py "
                                f"--interval {interval} --out {remote.remote_dir}/{label}.csv "
                                f"> {remote.remote_dir}/{label}.err 2>&1 < /dev/null & echo $!")
            pids[h["name"]] = int(out.splitlines()[-1])
            log(f"host monitor started on {h['name']} (pid {pids[h['name']]})")
        except Exception as e:
            log(f"could not start host monitor on {h['name']}: {e}")
    return pids


def stop_host_monitors(remote, hosts, pids, label, run_dir):
    os.makedirs(os.path.join(run_dir, "hosts"), exist_ok=True)
    for h in hosts:
        if h["name"] not in pids:
            continue
        try:
            remote.run(h, f"kill {pids[h['name']]}", check=False)
            time.sleep(0.5)
            remote.get(h, f"{remote.remote_dir}/{label}.csv", os.path.join(run_dir, "hosts", h["name"] + ".csv"))
        except Exception as e:
            log(f"could not collect host metrics from {h['name']}: {e}")


def wait_for_drain(cfg, auth, timeout, interval=2.0):
    """Waits until the resort queues are empty and the processed count stops changing."""
    deadline = time.time() + timeout
    last, stable = None, 0
    while time.time() < deadline:
        snap = counters(cfg, auth)
        processed = snap.get("swipes_processed")
        # Without a consumer endpoint, processed stays None and only the depth is checked.
        if snap.get("queue_depth") == 0 and processed == last:
            stable += 1
            if stable >= 2:
                return True
        else:
            stable = 0
        last = processed
        time.sleep(interval)
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", required=True)
    p.add_argument("--label", required=True, help="run name, used as the results directory")
    p.add_argument("--base-url", required=True, help="e.g. http://<alb-dns>/ski-servlet")
    p.add_argument("--threads", type=int, required=True, help="client main-phase threads")
    p.add_argument("--swipes", type=int, required=True, help="total swipes incl. the 32,000 warmup")
    p.add_argument("--ingest", help="comma-separated ingest servers in the target group (default: all)")
    p.add_argument("--consumer-threads", type=int,
                   help="intended consumer thread count, checked against the consumer's /health")
    p.add_argument("--notes", default="", help="free text stored in run.json, e.g. consumer settings")
    p.add_argument("--results-dir", default="results")
    p.add_argument("--interval", type=float, default=5.0,
                   help="sampling interval in seconds (RabbitMQ refreshes queue statistics every 5 s)")
    p.add_argument("--drain-timeout", type=float, default=300)
    p.add_argument("--no-ssh", action="store_true", help="skip host monitors and access-log counts")
    p.add_argument("--no-cloudwatch", action="store_true")
    p.add_argument("--force", action="store_true", help="overwrite an existing run directory")
    args = p.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)
    config_dir = os.path.dirname(os.path.abspath(args.config))
    # With "client_host", the client runs on that instance over SSH; otherwise locally.
    client_host = cfg.get("client_host")
    jar = None
    if not client_host:
        jar = os.path.normpath(os.path.join(config_dir, os.path.expanduser(cfg["client_jar"])))
        if not os.path.isfile(jar):
            sys.exit(f"client jar not found: {jar} (run mvn package in client-part2)")

    run_dir = os.path.abspath(os.path.join(args.results_dir, args.label))
    if os.path.exists(run_dir):
        if not args.force:
            sys.exit(f"{run_dir} exists; choose another --label or pass --force")
        shutil.rmtree(run_dir)
    os.makedirs(run_dir)

    hosts = cfg.get("hosts", [])
    ingest_names = set(args.ingest.split(",")) if args.ingest else {h["name"] for h in hosts if h["role"] == "ingest"}
    ingest = [h for h in hosts if h["role"] == "ingest" and h["name"] in ingest_names]
    # Monitor only the ingest servers in this run, plus the consumer and RabbitMQ.
    monitored = [h for h in hosts if h["role"] != "ingest" or h["name"] in ingest_names]

    auth = queue_monitor.basic_auth(cfg["rabbit"].get("user", "guest"), cfg["rabbit"].get("password", "guest"))
    remote = Remote(cfg)
    run = {
        "label": args.label, "base_url": args.base_url, "threads": args.threads, "swipes": args.swipes,
        "ingest_servers": sorted(ingest_names), "notes": args.notes, "interval_s": args.interval,
        "consumer_threads_expected": args.consumer_threads,
    }

    run["before"] = counters(cfg, auth)
    if run["before"].get("queue_depth"):
        log(f"WARNING: resort queues already hold {run['before']['queue_depth']} messages")
    live = run["before"].get("consumer_threads")
    if args.consumer_threads is not None and live != args.consumer_threads:
        log(f"WARNING: consumer runs {live} threads, expected {args.consumer_threads}")
    posts_before = {} if args.no_ssh else post_counts(remote, ingest, cfg["access_log_glob"])
    pids = {} if args.no_ssh else start_host_monitors(remote, monitored, args.label, args.interval)

    qm_cmd = [sys.executable, os.path.join(HERE, "queue_monitor.py"), "--quiet",
              "--rabbit", cfg["rabbit"]["api"], "--user", cfg["rabbit"].get("user", "guest"),
              "--password", cfg["rabbit"].get("password", "guest"), "--vhost", cfg["rabbit"].get("vhost", "/"),
              "--interval", str(args.interval), "--out", os.path.join(run_dir, "queue.csv")]
    qm = subprocess.Popen(qm_cmd)

    exit_code = None
    try:
        # A few idle samples before the load starts give the queue plot a baseline.
        time.sleep(2 * args.interval)
        log(f"starting client: {args.threads} threads, {args.swipes} swipes -> {args.base_url}")
        run["client_start"] = time.time()
        client_args = f"{args.base_url} {args.threads} {args.swipes}"
        with open(os.path.join(run_dir, "client.log"), "w") as out:
            if client_host:
                # The client writes ./results, so each run gets its own remote directory.
                remote_run = f"{client_host['dir']}/runs/{args.label}"
                client = remote.popen(
                    client_host,
                    f"rm -rf {remote_run} && mkdir -p {remote_run} && cd {remote_run} && "
                    f"exec java -Djava.awt.headless=true -jar ~/{client_host['dir']}/{client_host['jar']} {client_args}",
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            else:
                client = subprocess.Popen(["java", "-jar", jar] + client_args.split(),
                                          cwd=run_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in client.stdout:
                sys.stdout.write(line)
                out.write(line)
            exit_code = client.wait()
        run["client_end"] = time.time()
        run["client_exit_code"] = exit_code
        if client_host:
            run["client_host"] = client_host["address"]
            try:
                remote.get(client_host, f"{client_host['dir']}/runs/{args.label}/results", run_dir, recursive=True)
            except Exception as e:
                log(f"WARNING: could not copy the client's results back: {e}")
        log(f"client finished (exit {exit_code}); waiting for queues to drain")

        run["drained"] = wait_for_drain(cfg, auth, args.drain_timeout, args.interval)
        run["drain_end"] = time.time()
        if not run["drained"]:
            log(f"WARNING: queues did not drain within {args.drain_timeout:.0f} s")

        # Measured consumer lag of messages published in the main phase.
        if cfg.get("consumer_health"):
            try:
                window = main_phase_window(run_dir)
                if window:
                    run["lag_window_ms"] = list(window)
                    run["lag"] = queue_monitor.get_json(
                        f"{cfg['consumer_health']}/lag?from={window[0]}&to={window[1]}")
                    log(f"main-phase lag: {run['lag']}")
            except Exception as e:
                log(f"WARNING: could not read main-phase lag: {e}")
        # Trailing samples show the queue back at rest.
        time.sleep(2 * args.interval)
    except KeyboardInterrupt:
        log("interrupted; collecting what was recorded so far")
        run["interrupted"] = True
        run.setdefault("client_end", time.time())
        run.setdefault("drain_end", time.time())
    finally:
        qm.terminate()
        qm.wait(timeout=10)
        run["after"] = counters(cfg, auth)
        if run["after"].get("consumer_threads") != run["before"].get("consumer_threads"):
            log(f"WARNING: consumer threads changed during the run: "
                f"{run['before'].get('consumer_threads')} -> {run['after'].get('consumer_threads')}")
        if not args.no_ssh:
            stop_host_monitors(remote, monitored, pids, args.label, run_dir)
            posts_after = post_counts(remote, ingest, cfg["access_log_glob"])
            run["posts_per_server"] = {n: posts_after[n] - posts_before[n]
                                       for n in posts_after if n in posts_before}
            summarize.write_distribution(run_dir, run["posts_per_server"])
        with open(os.path.join(run_dir, "run.json"), "w") as f:
            json.dump(run, f, indent=2)

    if not args.no_cloudwatch and "client_start" in run:
        cw_cmd = [sys.executable, os.path.join(HERE, "cloudwatch_fetch.py"),
                  "--start", str(int(run["client_start"])), "--end", str(int(run["drain_end"])),
                  "--region", cfg.get("region", "us-west-2"), "--alb", cfg["alb_name"],
                  "--tg", cfg["target_group"], "--out", run_dir]
        # CloudWatch publishes with a 1-2 minute delay, so save the command to rerun later.
        with open(os.path.join(run_dir, "cloudwatch_cmd.txt"), "w") as f:
            f.write(" ".join(cw_cmd) + "\n")
        if shutil.which("aws"):
            log("fetching CloudWatch metrics (rerun cloudwatch_cmd.txt later for the final minutes)")
            subprocess.run(cw_cmd)
        else:
            log("aws CLI not found; run the command in cloudwatch_cmd.txt from a machine that has it")

    summarize.summarize_run(run_dir)
    summarize.rebuild_index(os.path.abspath(args.results_dir))
    sys.exit(exit_code or 0)


if __name__ == "__main__":
    main()
