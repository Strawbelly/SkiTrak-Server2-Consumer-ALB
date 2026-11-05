# Load-test monitoring

Python 3 scripts (standard library only) that run one load test and save client, queue, and system metrics as CSV. Each metric has exactly one source.

| Script | Runs on | Responsibility |
|---|---|---|
| `run_test.py` | the client machine | Runs one test: before/after counters (consumer `/health`, DLQ, Tomcat POST counts), starts and stops the monitors, runs the client, waits for the queues to drain |
| `queue_monitor.py` | the client machine | RabbitMQ resort queues: depth, producer rate (publish), consumer rate (ack), estimated drain time → `queue.csv` |
| `host_monitor.py` | each EC2 host (copied and started over SSH) | CPU (incl. steal), memory, network from `/proc` → `hosts/<name>.csv` |
| `cloudwatch_fetch.py` | anywhere with the `aws` CLI | ALB-only metrics: ELB and target 5XX, healthy targets, target response time → `cloudwatch.csv` |
| `summarize.py` | anywhere | `summary.json` per run and `results/summary.csv` with one row per run |

## Setup

1. Copy `test_config.example.json` to `test_config.json` and fill in the SSH key and RabbitMQ credentials. Private IPs only work from inside the VPC; from a laptop, use public IPs.
2. The machine running `run_test.py` needs:
   - port 15672 open to RabbitMQ
   - port 8081 open to the consumer
   - SSH access to every host
   - `java`, and the client jar built with `mvn package` in `client-part2`
3. Each host needs `python3`; Amazon Linux 2023 includes it. Tomcat's default access log (`localhost_access_log.*.txt`) must be enabled, because per-server request counts come from it.

## Running

```bash
python3 run_test.py --config test_config.json --label lb4-500k \
    --base-url http://<alb-dns>/ski-servlet --threads 256 --swipes 532000 \
    --ingest server1,server2,server3,server4 --notes "consumer threads=10 prefetch=100"
```

- `--swipes` includes the client's 32,000 warmup swipes.
- List in `--ingest` only the servers registered in the target group for that run.
- Record consumer and server settings in `--notes`; the scripts do not read them.
- CloudWatch publishes 1–2 minutes late, so rerun `results/<label>/cloudwatch_cmd.txt` a few minutes after the run.
- After rerunning the CloudWatch fetch or editing a run, refresh the summaries with `python3 summarize.py --all results`.
- **Recorded runs:** every recorded run (smoke, tuning, perf) used `--no-cloudwatch` because the AWS API was unavailable, so no run has a `cloudwatch.csv` and its summary has no ALB fields. The ALB metrics in the report come from AWS console screenshots, not from `cloudwatch_fetch.py`.

## Summary fields

- **Client:** runtime, throughput, failures, retries, ingest latency (mean, p50, p95, p99), leaderboard updates delivered, and subscribers that received none.
- **Queue:** sampled every 5 s, RabbitMQ's statistics interval. A spike shorter than one sample can be missed.
  - `queue_depth_max` and `queue_depth_p95` use total depth (ready + unacked, as the RabbitMQ console shows it). `queue_ready_max` is real backlog; `queue_unacked_max` is messages prefetched to consumers.
  - `queue_depth_profile` (frozen rule; see `depth_profile` in `summarize.py`):
    - `flat` when the publishing peak is below 200;
    - otherwise `sawtooth` (at least two rise-and-fall cycles) or `plateau` (at least half of the publishing samples above 50% of the peak);
    - otherwise `flat`; `+spike` is added when exactly one publishing sample is above half a peak of at least 50.
  - Producer and consumer rates are averaged over samples where messages were flowing. `queue_depth.svg` plots depth over time.
- **Consumer lag (measured):** `lag_p50_ms`, `lag_p99_ms` and `lag_max_ms` come from the consumer's `/lag` endpoint. Lag is processing start minus the ingest server's `x-publish-ms` publish time, for messages published in the main phase only; the warmup is excluded by publish time. `est_drain_*_ms` (depth ÷ ack rate) is only an estimate of backlog drain time and is never used as lag.
- **Swipe loss:** `swipes_missing` = client successes − swipes the consumer processed during the run. `loss_check` is PASS only if nothing is missing, nothing went to the DLQ, and the queues drained. `consumer_retries` and `consumer_ack_failures` are the consumer's own counters.
- **Targets:**
  - `target_depth`: `queue_depth_max` < 1000;
  - `target_lag`: `lag_p99_ms` < 100, or NOT MEASURED without samples or if more than 1% of messages had no publish header;
  - `target_loss`: `loss_check` PASS;
  - `targets_overall` is PASS only if all three pass.
- **System:** for each host, average and max CPU, max steal, max memory, average network in/out.
- **Distribution:** POST requests per ingest server and each server's share.

## Part 4 tuning

Every tuning run uses the same conditions: 4 ingest servers behind the ALB, an EC2 client, `--swipes 232000` (200K plus the 32K warmup), Top-N 20, and single-active-consumer resort queues. Each round changes one setting; every other setting stays at its default (prefetch 100, batch ACK 1, channel pool 64) or at the winner of an earlier round. Results are in `results/tuning/`, and `results/tuning/comparison.csv` has one row per run.

Labels are `<round>-c<client threads>-k<consumer threads>`, plus a suffix for the setting under test in r3–r5 (`-p<prefetch>`, `-a<ACK batch>`, `-pool<size>`), for example `r1-c128-k10` or `r3-c256-k10-p100`. The part before the first `-` is the round that `--compare` groups by.

| Round | Changes | Fixed | Winner |
|---|---|---|---|
| r1 | client threads 64, 128, 256, 512 | consumer threads 10 | client 256 (`r1-c256-k10`, 5,446.9 swipes/s). 512 failed the lag target (p99 104 ms). |
| r2 | consumer threads 10, 20, 40, 80 | client 256 | 10 threads (`r2-c256-k10`, 5,419.8 swipes/s). All four were within 5%, so the fewest threads won. |
| r3 | prefetch (`CONSUMER_PREFETCH`) 10, 100, 250 | client 256, consumer 10 | prefetch 100 (`r3-c256-k10-p100`, 5,235.0 swipes/s). Prefetch 10 failed the depth target (max 10,724) and the lag target (p99 1,739 ms); 250 had a higher p99 latency (150.3 vs 106.4 ms). |
| r4 | batch ACK (`CONSUMER_ACK_BATCH`) 1, 10, 50 | prefetch 100 | ACK 1 (`r4-c256-k10-a001`, 5,143.7 swipes/s). All three were within 5%, and ACK 1 had the lowest p99 latency (97.1 ms). |
| r5 | channel pool (`RABBITMQ_CHANNEL_POOL_SIZE`, ingest servers) 32, 64, 128 | ACK 1 | Pool 64 was kept (`r5-c256-k10-pool064`, 5,390.9 swipes/s). Pool 32 was more than 5% below it. By the compare rule, pool 128 ranks first (p99 97.9 vs 100.4 ms), but its throughput was lower (5,312.8 swipes/s) and its peak depth higher (452 vs 288). |
| final | validation of the selected configuration | all winners above | `final-c256-k05to10-p100-a001-pool064`: 5,381.9 swipes/s, all three targets PASS. |

The runs in `results/tuning/_superseded/` are excluded. They were replaced by the r3 runs marked "rerun" in their notes.

**Before each run**, on the consumer instance, pin the thread count by setting all three to N in `/etc/skitrak/consumer.env`. The autoscaler stays on but cannot move:

```
CONSUMER_THREADS=N
CONSUMER_MIN_THREADS=N
CONSUMER_MAX_THREADS=N
```

Then:
1. Run `sudo systemctl restart skitrak-consumer`. This also gives every run a fresh aggregator and counters.
2. Check that `/health` reports `"threads": N`.
3. Confirm the resort queues are empty.

**Run:**

```bash
python3 run_test.py --config test_config.json --label r1-c128-k10 \
    --base-url http://<alb-dns>/ski-servlet --threads 128 --swipes 232000 \
    --consumer-threads 10 --notes "prefetch=100 ack=1 pool=64"
```

`--consumer-threads` is checked against the consumer's live `/health` count at the start and end of the run.

**Compare:** `python3 summarize.py --compare results` writes `results/comparison.csv` and prints the best run per round. A run qualifies only if:
- `loss_check` is PASS,
- it had 0 failed swipes,
- its consumer thread count was confirmed.

The highest throughput wins. Among runs within 5% of it, round r2 prefers the fewest consumer threads, then the lowest p99 latency, then the lowest peak queue depth.

**Final configuration:** the selected configuration, as used in the final validation run, was:

```
CONSUMER_AUTOSCALE=true
CONSUMER_THREADS=10
CONSUMER_MIN_THREADS=5
CONSUMER_MAX_THREADS=10
CONSUMER_PREFETCH=100
CONSUMER_ACK_BATCH=1
RABBITMQ_CHANNEL_POOL_SIZE=64   # on each ingest server
```

The deployed default is `CONSUMER_MIN_THREADS=1`, `CONSUMER_MAX_THREADS=10`.

The validation run (label `final-c256-k05to10-p100-a001-pool064`) was started with no `--consumer-threads`, because the count can change during the run. It scaled from 10 down to 5 (see its `consumer_threads.csv`).

Note that `consumer_threads_ok` also requires the count to be the same at the start and end of the run. If the autoscaler changes it, the final run shows as not qualified in `--compare`, and only for that reason. Judge that run by its queue-depth and loss results.
