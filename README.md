# CS6043 Coin Rush (Shared Application Relay Bottleneck)

## Purpose and limitations
The new `--bandwidth-kbps` option rate-limits **outgoing relayed movement messages** via a shared FIFO queue in the Python server. `--background-kbps` consumes *virtual* service capacity in the same queue. `--queue-kbytes` caps the queue for UDP relayed movement: messages over capacity are dropped. TCP relayed messages remain queued rather than being dropped by the application, even if the requested queue limit is exceeded. All other traffic (inputs from clients to server, authoritative world snapshots, ACKs, hybrid TCP control channel) bypasses this limiter.

This deliberately models one application-level egress bottleneck and **is not** kernel traffic shaping, actual IP loss, TCP congestion-control behavior, or multi-hop routing. Virtual background load is not transmitted over the network. In a later phase use Linux `tc netem` + `tbf`/network namespaces or Mininet to test actual network effects equally on TCP and UDP.

`--bandwidth-kbps 0` (default) disables bottleneck shaping 

## Quick runs (three terminals each)

### Baseline TCP
```powershell
python server.py --protocol tcp --round-seconds 120 --delay-ms 100 --congestion-log-file p11_tcp_baseline_queue.csv
python client.py --protocol tcp --update-hz 30 --log-file p11_tcp_p1.csv
python client.py --protocol tcp --update-hz 30 --log-file p11_tcp_p2.csv
```

### Limited TCP movement relay
```powershell
python server.py --protocol tcp --round-seconds 120 --delay-ms 100 --bandwidth-kbps 40 --background-kbps 20 --queue-kbytes 16 --congestion-log-file p11_tcp_congested_queue.csv
python client.py --protocol tcp --update-hz 30 --log-file p11_tcp_congested_p1.csv
python client.py --protocol tcp --update-hz 30 --log-file p11_tcp_congested_p2.csv
```

### Limited UDP movement relay
```powershell
python server.py --protocol udp --round-seconds 120 --delay-ms 100 --loss-pct 0 --bandwidth-kbps 40 --background-kbps 20 --queue-kbytes 16 --congestion-log-file p11_udp_congested_queue.csv
python client.py --protocol udp --update-hz 30 --log-file p11_udp_congested_p1.csv
python client.py --protocol udp --update-hz 30 --log-file p11_udp_congested_p2.csv
```

Use *one command per terminal*, not all three in the same terminal. Try `--bandwidth-kbps 15 --background-kbps 10 --queue-kbytes 4` for a stronger demonstration. Allow queue delay to build, then stop after around 30 seconds.

### Hybrid
```powershell
python server.py --protocol hybrid --round-seconds 120 --delay-ms 100 --loss-pct 0 --bandwidth-kbps 40 --background-kbps 20 --queue-kbytes 16 --congestion-log-file p11_hybrid_queue.csv
python client.py --protocol hybrid --update-hz 30 --log-file p11_hybrid_p1.csv
python client.py --protocol hybrid --update-hz 30 --log-file p11_hybrid_p2.csv
```

## Queue CSV
`timestamp,event,protocol,bytes,queue_bytes,wait_ms`

- `enqueue`: message entered simulated shared relay, wait_ms is predicted queue+delay time.
- `deliver`: relay callback became eligible; wait_ms is actual residence time in queue before callback.
- `queue_drop`: UDP movement update discarded because the queue is full.
- `configured_loss`: UDP movement update discarded by `--loss-pct`, independently of congestion.
- `background_used`: virtual background traffic consumed modeled service time.

`wait_ms` is modeled relay waiting time, **not physical network RTT**. The byte accounting includes only relayed movement messages and virtual load, not all application traffic.

## Verification
`python test_phase11.py` runs quick queue-behavior tests. The script is optional for regular gameplay. Run at least two clients and verify movement, coins, timer, and logging on your PC. Keep the Phase 10 server backup.
