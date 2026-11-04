# CS6043 — TCP vs. UDP Multiplayer Arena (Coin Rush)

## Project Description

Coin Rush is a multiplayer game developed in Python to investigate the performance of TCP, UDP, and Hybrid communication in a real-time multiplayer environment.

Players move around an arena collecting coins while the server manages player positions, scores, and game rounds.

The main objective is to compare network performance under different conditions, including latency, packet loss, bandwidth limitations, congestion, and multi-hop communication.

### Networking Features

- **TCP:** Reliable and ordered movement and game-state communication.
- **UDP:** Movement updates with application-level reliability for important game-state information.
- **Hybrid:** UDP for movement updates and TCP for reliable game-state communication.
- **Network simulation:** Configurable delay, UDP movement loss, bandwidth, and background traffic.
- **Performance measurement:** CSV logging for update age, throughput, sequence gaps, and synchronization corrections.
- **Multi-hop testing:** Mininet topology with two virtual routers and configurable network links.

## Requirements

**Windows / Local Testing**
- Python 3
- Tkinter
- Matplotlib (for analysis)

**Linux / Multi-Hop Testing**
- Ubuntu 22.04
- Python 3 and Tkinter
- Mininet
- iperf3
- xterm

Install analysis dependencies:

```bash
pip install matplotlib
```

## 1. Basic Multiplayer Testing

Open three terminals in the project directory.

**Server:**
```bash
python server.py --protocol tcp
```

**Player 1:**
```bash
python client.py --protocol tcp
```

**Player 2:**
```bash
python client.py --protocol tcp
```

Use WASD or arrow keys to move and collect coins.

Change `tcp` to `udp` or `hybrid` in all three commands to test different networking modes.

## 2. Delay and Packet Loss

**TCP with 100 ms delay:**
```bash
python server.py --protocol tcp --delay-ms 100
```

**UDP with 100 ms delay and 20% simulated movement loss:**
```bash
python server.py --protocol udp --delay-ms 100 --loss-pct 20
```

Start both clients using the corresponding protocol.

Observe local movement responsiveness, remote player synchronization, and score updates.

## 3. Traffic Generation

Test different update rates and message sizes:

```bash
python client.py --protocol tcp --update-hz 30 --payload-bytes 512 --log-file tcp_test.csv
```

Generate additional burst traffic:

```bash
python client.py --protocol udp --update-hz 30 --payload-bytes 512 --burst-count 10 --burst-interval 1 --log-file udp_burst.csv
```

Run a second client with a different log filename.

## 4. Application-Level Congestion

Start the server with bandwidth and background traffic restrictions:

```bash
python server.py --protocol tcp --bandwidth-kbps 150 --background-kbps 20 --queue-kbytes 16 --congestion-log-file tcp_queue.csv
```

Repeat with UDP or Hybrid under equivalent settings.

This is an application-level congestion model, not actual TCP congestion control.

## 5. Multi-Hop Network Testing (Ubuntu)

Clean previous Mininet configurations:

```bash
sudo mn -c
```

Start the two-router topology:

```bash
sudo -E python3 multihop.py --bw-mbps 0.15 --delay-ms 50 --loss-pct 2 --queue-packets 50
```

Verify routing at the `mininet>` prompt:

```text
c1 ping -c 3 10.10.3.2
c1 traceroute -n 10.10.3.2
```

Open virtual-host terminals:

```text
xterm srv c1 c2
```

**Server (srv):**
```bash
python3 server.py --protocol tcp --host 10.10.3.2 --delay-ms 0 --bandwidth-kbps 0
```

**Clients (c1 and c2):**
```bash
python3 client.py --protocol tcp --host 10.10.3.2 --update-hz 30
```

Repeat using UDP and Hybrid.

## 6. Real Network Congestion (iperf3)

Start the iperf3 receiver at the Mininet prompt:

```text
srv iperf3 -s -D -p 5201
```

While Coin Rush is running, generate background traffic:

```text
c1 iperf3 -c 10.10.3.2 -u -b 60K -t 20 -R
```

Change `60K` to `20K`, `100K`, or `120K` to test increasing traffic.

Compare movement, coin collection, and synchronization before, during, and after congestion.

## 7. Automated Experiment Testing (Ubuntu)

Run a standardized 60-second experiment:

```bash
sudo -E python3 run_experiment.py --protocol tcp --duration 60 --bw-mbps 0.15 --delay-ms 50 --loss-pct 2 --iperf-kbps 60 --traffic-start 15 --traffic-duration 30 --trial 1
```

Change `--protocol` to `udp` or `hybrid` for comparisons.

The runner saves logs and experiment settings for analysis.

## 8. Results Analysis

Analyze client logs:

```bash
python analyze_logs.py player1.csv player2.csv
```

Compare automated experimental runs:

```bash
python compare_runs.py
```

Recorded measurements include:
- Mean and P95 movement update age
- Sending and receiving rates
- Application throughput
- Sequence gaps
- Client-side prediction corrections
- UDP reliability and congestion events where enabled

For valid comparisons, keep the network settings, test duration, update rate, and number of players consistent. Repeat each experiment multiple times.

## Notes

- Windows is used for local gameplay and application-level testing.
- Ubuntu with Mininet is used for controlled multi-hop experiments.
- Simulated UDP loss is different from real network-level packet loss.
- TCP, UDP, and Hybrid should be compared under equivalent network conditions.
- The browser-based game is not part of the current implementation.
