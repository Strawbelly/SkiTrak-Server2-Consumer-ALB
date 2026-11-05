# Deployment Configuration

All components were deployed manually on EC2 in `us-west-2` (VPC `vpc-0b7bec73128926358`). There are no automated deployment scripts; the configuration and steps below are what was used.

## EC2 Instances

| Role | Instance ID | Type | Private IP |
|---|---|---|---|
| Ingest Server 1 | `i-0219a49d7e5e1230c` | t3.micro | `172.31.44.0` |
| Ingest Server 2 | `i-0e7424e3074c0a088` | t3.micro | `172.31.37.24` |
| Ingest Server 3 | `i-053e0aa8147fa39b9` | t3.micro | `172.31.46.123` |
| Ingest Server 4 | `i-013f48cd38e7e5afa` | t3.micro | `172.31.37.7` |
| Consumer | `i-060ffa330f705ad73` | t3.micro | `172.31.42.186` |
| RabbitMQ | `i-0e8a731d7392045a4` | t3.micro | `172.31.44.149` |

---

## ALB Configuration

- Name: `ski-ingest-alb`, internet-facing, IPv4
- Listener: HTTP:80 → target group `ski-ingest-tg` (HTTP:8080, instance targets)
- Subnets: `subnet-015f511e5594a8635` (us-west-2a), `subnet-00d66701eac658daf` (us-west-2b)
- Health check: HTTP `/ski-servlet/health` on the traffic port, every 30 s, timeout 5 s, healthy 2, unhealthy 3, success code 200 (`/ski-servlet` is the Tomcat context path)
- Idle timeout: 120 s
- Stickiness: enabled, load-balancer-generated cookie, 300 s
- WebSocket: supported over HTTP/1.1; an established WebSocket stays on the server that accepted the upgrade
- Ingest servers accept port 8080 only from the ALB security group (and from my IP for testing)

**Target group members:** servers 1–4. For the performance tests, servers were registered and deregistered by hand so the target group held 1, 2 or 4 servers.

---

## RabbitMQ

- Runs in a Docker container on the RabbitMQ instance
- AMQP port 5672, management UI port 15672
- Statistics interval (`collect_statistics_interval`): 5000 ms

### Swipe queues

- Exchange `skitrak.exchange` (topic, durable); routing key `resort.{resortID}`
- Queues `skitrak.resort.1` … `skitrak.resort.10` (classic, durable), each bound with `resort.N`
    - `x-single-active-consumer: true`
    - `x-message-ttl: 60000`
    - `x-max-length: 10000`
    - `x-dead-letter-exchange: skitrak.dlx`
- Dead letters: `skitrak.dlx` (fanout) → `skitrak.dlq` (durable)

### Backplane setup

- Exchange `skitrak.leaderboard` (fanout, durable). The consumer publishes each changed Top-N snapshot to it.
- Each ingest server's `LeaderboardRelay` declares its own server-named, exclusive, auto-delete relay queue bound to `skitrak.leaderboard` (`x-max-length: 1000`, `x-overflow: drop-head`), consumes it with auto-ack, and pushes each board to its local WebSocket subscribers.

All exchanges and queues are declared by the code at startup (`Topology` in the consumer, `LeaderboardRelay` in each server), so no manual setup in the management UI is needed.

---

## Ingest Servers (server-v2)

- Tomcat 9 at `/opt/tomcat9`, run as `ec2-user` (not a systemd service)
- WAR deployed as `/opt/tomcat9/webapps/ski-servlet.war`
- Environment in `/opt/tomcat9/bin/setenv.sh`:

  export RABBITMQ_HOST=172.31.44.149
  export RABBITMQ_USER=<user>
  export RABBITMQ_PASSWORD=<password>
  export RABBITMQ_CHANNEL_POOL_SIZE=64

## Consumer

- JAR at `/opt/skitrak/ski-consumer-1.0-SNAPSHOT.jar`, run by the systemd service `skitrak-consumer` (restarts automatically on failure)
- Environment in `/etc/skitrak/consumer.env`:

  RABBITMQ_HOST=172.31.44.149
  RABBITMQ_USER=<user>
  RABBITMQ_PASSWORD=<password>
  CONSUMER_RESORT_IDS=1-10
  CONSUMER_AUTOSCALE=true
  CONSUMER_THREADS=10
  CONSUMER_MIN_THREADS=5
  CONSUMER_MAX_THREADS=10
  CONSUMER_PREFETCH=100
  CONSUMER_ACK_BATCH=1

---

## Manual Deployment Steps

1. **RabbitMQ:** start the RabbitMQ container on the RabbitMQ instance and create the user.
2. **Build:** `mvn -pl server-v2 package` and `mvn -pl consumer package`.
3. **Ingest servers:** on each server, copy the WAR to `/opt/tomcat9/webapps/`, set `setenv.sh`, and start Tomcat with `/opt/tomcat9/bin/startup.sh`.
4. **Consumer:** copy the JAR to `/opt/skitrak/`, set `/etc/skitrak/consumer.env`, and run `sudo systemctl restart skitrak-consumer`. Check `curl localhost:8081/health`.
5. **ALB:** register the servers in `ski-ingest-tg` and wait until they are healthy.