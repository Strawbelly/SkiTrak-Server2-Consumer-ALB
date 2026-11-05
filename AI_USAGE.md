# AI Usage Reflection

## Where and for What I Used AI

- **Ingest server and channel pool:** Claude Code implemented `SwipeEventPublisher` and `ChannelPool` in `/server-v2`: one connection, borrowed and returned channels with publisher confirms, one retry on a fresh channel, and reconnection. Later it added an `x-publish-ms` header to each published swipe so consumer lag could be measured.
- **Consumer aggregation:** Claude Code generated the first version of the consumer (`ConsumerPool`, `ResortWorker`, `LeaderboardAggregator`, `TopN`). It split the work into many small classes; I had it merge them, then checked that de-duplication by `eventId`, broadcasting, reject-to-DLQ and the counters were still there.
- **Backplane / fanout wiring:** Claude Code wired the `skitrak.leaderboard` fanout exchange and `LeaderboardRelay`, which gives each server its own relay queue.
- **Tuning and monitoring scripts:** Claude Code wrote the scripts in `/monitoring` (`queue_monitor.py`, `host_monitor.py`, `cloudwatch_fetch.py`, `run_test.py`, `summarize.py`), added `LagStats` and a `/lag` endpoint to the consumer, and ran the tuning rounds (client threads, consumer threads, prefetch, batch ACK, channel pool) and the 1/2/4-server and 1M tests on EC2.
- **ALB and testing on AWS:** I registered and deregistered servers in the ALB target group by hand between runs, because Claude Code's AWS access was blocked, and took the RabbitMQ and ALB screenshots myself.

## Suggestions I Accepted

- **Broadcast backplane with one relay queue per server.** Claude Code proposed publishing full Top-N snapshots to a fanout exchange, with each server consuming its own exclusive, auto-delete relay queue. I accepted it because the ALB can then balance requests freely while every subscriber still gets every update. I verified it with a controlled test: two swipes sent directly to server1 produced boards of 397 and then 741 ft at a subscriber connected to server3.
- **Best-effort leaderboard broadcast.** The consumer acks a swipe once it is applied, even if publishing the board fails. I kept this after reasoning it through: a redelivered swipe would be skipped as a duplicate, so retrying would not re-send the board, and the next board is a full snapshot anyway.

## Suggestions I Rejected or Corrected

- **Pinning the consumer thread count for the final run.** Claude Code proposed setting MIN equal to MAX for the final validation run. I rejected this because the assignment requires dynamic scaling, and had it use MIN=5, MAX=10 so the autoscaler could actually change the thread count.
- **Reporting an estimate as consumer lag.** Claude Code's first queue monitor reported `est_lag_ms`, computed as queue depth ÷ ack rate, as the consumer lag. That only estimates how long a backlog takes to drain; it does not measure how long a message actually waited. I had it measure real lag instead, from a publish timestamp header on each swipe to the start of processing, and rename the estimate to a drain-time figure that is never reported as lag.

## What the AI Got Wrong, and Decisions I Reasoned Through Myself

- **The connection-locality comparison.** When I asked Claude to compare the backplane with co-location, it described co-location as each server aggregating only its own swipes, then argued against that. The assignment defines co-location as partitioning by resort. I compared the backplane with resort partitioning instead: it would need per-resort routing rules, cap scaling at 10 nodes, and make failover harder.
- **Per-resort ordering and consumer concurrency.** Single-active-consumer allows one active consumer per resort queue, so consumer threads beyond 10 are standbys and add no capacity. Ordering per resort therefore caps parallelism at the number of resorts, which is why I compared server counts with the same 10 active consumers.

## Transcripts

- Claude Code: `transcripts/`