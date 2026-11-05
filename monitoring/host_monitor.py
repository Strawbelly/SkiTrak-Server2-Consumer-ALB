#!/usr/bin/env python3
"""
Samples CPU, memory, and network usage on a Linux host into a CSV file, using /proc only.

Copy this file to each EC2 instance (ingest servers, consumer, RabbitMQ). run_test.py
starts it over SSH. CPU steal is recorded because a t3 instance that has used up its CPU
credits is throttled by the hypervisor, and that shows up as steal, not as busy time.

Runs until interrupted (Ctrl-C or SIGTERM).

Usage:
  host_monitor.py --out host.csv [--interval 2]
"""
import argparse
import csv
import signal
import time

FIELDS = ["ts", "elapsed_s", "cpu_pct", "steal_pct", "mem_used_mb", "mem_used_pct", "net_rx_kbps", "net_tx_kbps"]

stop = False


def on_signal(*_):
    global stop
    stop = True


def read_cpu():
    # cpu  user nice system idle iowait irq softirq steal
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:9]]
    return {"idle": v[3], "steal": v[7], "total": sum(v)}


def read_mem():
    m = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            m[k] = int(v.split()[0])  # kB
    return m["MemTotal"], m.get("MemAvailable", m["MemFree"])


def read_net():
    rx = tx = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            iface, data = line.split(":", 1)
            if iface.strip() == "lo":
                continue
            d = data.split()
            rx += int(d[0])
            tx += int(d[8])
    return rx, tx


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    start = time.time()
    prev_cpu, prev_net, prev_t = read_cpu(), read_net(), start

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        while not stop:
            deadline = prev_t + args.interval
            while not stop and time.time() < deadline:
                time.sleep(0.1)
            if stop:
                break

            t, cpu, net = time.time(), read_cpu(), read_net()
            dt_cpu = max(1, cpu["total"] - prev_cpu["total"])
            total_kb, avail_kb = read_mem()
            dt = t - prev_t
            w.writerow({
                "ts": f"{t:.3f}",
                "elapsed_s": f"{t - start:.1f}",
                "cpu_pct": f"{100.0 * (1 - (cpu['idle'] - prev_cpu['idle']) / dt_cpu):.1f}",
                "steal_pct": f"{100.0 * (cpu['steal'] - prev_cpu['steal']) / dt_cpu:.1f}",
                "mem_used_mb": f"{(total_kb - avail_kb) / 1024:.0f}",
                "mem_used_pct": f"{100.0 * (total_kb - avail_kb) / total_kb:.1f}",
                "net_rx_kbps": f"{(net[0] - prev_net[0]) / 1024 / dt:.1f}",
                "net_tx_kbps": f"{(net[1] - prev_net[1]) / 1024 / dt:.1f}",
            })
            f.flush()
            prev_cpu, prev_net, prev_t = cpu, net, t


if __name__ == "__main__":
    main()
