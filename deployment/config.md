# Deployment Configuration

## EC2 Setup

**SkiTrak Ingest Server 1**
- Instance ID: `i-0219a49d7e5e1230c`
- Instance Type: `t3.micro`
- Private IP: `172.31.44.0`
- Availability Zone: `us-west-2b`

**SkiTrak Ingest Server 2**
- Instance ID: `i-0e7424e3074c0a088`
- Instance Type: `t3.micro`
- Private IP: `172.31.37.24`
- Availability Zone: `us-west-2b`

**SkiTrak Ingest Server 3**
- Instance ID: `i-053e0aa8147fa39b9`
- Instance Type: `t3.micro`
- Private IP: `172.31.46.123`
- Availability Zone: `us-west-2b`

**SkiTrak Ingest Server 3**
- Instance ID: `i-013f48cd38e7e5afa`
- Instance Type: `t3.micro`
- Private IP: `172.31.37.7`
- Availability Zone: `us-west-2b`

**Consumer**
- Instance ID: `i-060ffa330f705ad73`
- Instance Type: `t3.micro`
- Private IP: `172.31.42.186`

**RabbitMQ**
- Instance ID: `i-0e8a731d7392045a4`
- Instance Type: `t3.micro`
- Private IP: `172.31.44.149`

**VPC**
- VPC ID: `vpc-0b7bec73128926358`
- Region: `us-west-2`

### Security Group Inbound Rules

**ALB Security Group (`ski-alb-sg`)**
- Port 80 (TCP) — HTTP — Source: `0.0.0.0/0`

**Ingest Server Security Group (`cs6650-lab1-sg`)**
- Port 8080 (TCP) — HTTP — Source: ALB Security Group (`sg-00a2821dac52391d1`)
- Port 8080 (TCP) — HTTP — Source: My IP
- Port 22 (TCP) — SSH — Source: My IP

---

## ALB Configuration

- ALB Name: `ski-ingest-alb`
- Scheme: internet-facing
- IP Address Type: IPv4
- Region: `us-west-2`
- Listener Protocol: HTTP
- Listener Port: 80
- Target Group Name: `ski-ingest-tg`
- Target Group Protocol: HTTP
- Target Group Port: 8080
- Target Type: Instance

### Health Check Configuration

- Health Check Protocol: HTTP
- Health Check Path: `/ski-servlet/health`
- Health Check Port: Traffic Port
- Health Check Interval: 30 seconds
- Health Check Timeout: 5 seconds
- Healthy Threshold: 2
- Unhealthy Threshold: 3
- Success Codes: 200

### ALB Attributes

- Idle Timeout: 120 seconds
- Stickiness: Enabled
- Stickiness Type: Load Balancer Generated Cookie
- Stickiness Cookie Duration: 300 seconds

### ALB Subnets

- `subnet-015f511e5594a8635` — `us-west-2a`
- `subnet-00d66701eac658daf` — `us-west-2b`

### Target Group Members

- Server 1 — `i-0219a49d7e5e1230c` — Port 8080 — Status: Healthy
- Server 2 — `i-0e7424e3074c0a088` — Port 8080 — Status: Healthy

### WebSocket Support

- WebSocket connections are supported over HTTP/1.1.
- An established WebSocket connection remains on the ingest server that accepted the connection.
- ALB cookie stickiness is enabled for session affinity on subsequent HTTP requests.

---

## RabbitMQ Setup

- Instance ID: `i-0e8a731d7392045a4`
- Instance Type: `t3.micro`
- Private IP: `172.31.44.149`
- AMQP Port: 5672
- Management UI Port: 15672

### Exchange Configuration

- Exchange Name: `skitrak.exchange`
- Exchange Type: topic
- Routing Key Pattern: `resort.{resortID}`

### Queue Configuration

- Queue Names: `resort.1` through `resort.10`
- Queue Type: classic
- Durable: `true`
- Single Active Consumer: `true`
- Message TTL: 60000 ms (60 seconds)
- Maximum Queue Length: 10000 messages
- Dead-letter Exchange: `skitrak.dlx`

### Queue Bindings

Each queue `resort.N` is bound to `skitrak.exchange` with routing key `resort.N`.

For example:
- Queue `resort.1` — Routing Key: `resort.1`
- Queue `resort.2` — Routing Key: `resort.2`
- ...
- Queue `resort.10` — Routing Key: `resort.10`

### Consumer Status

The following values reflect the RabbitMQ Management UI at the time of inspection:

- Queue State: running
- Consumers: 1
- Consumer Capacity: 100%