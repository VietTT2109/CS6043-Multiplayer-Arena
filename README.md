# CS6043 Coin Rush (Server-authoritative movement)

## Local TCP

Terminal 1: `python server.py --protocol tcp --host 127.0.0.1 --delay-ms 100`

Terminal 2: `python client.py --protocol tcp --update-hz 30 --log-file p1_authoritative_tcp.csv`

Terminal 3: `python client.py --protocol tcp --update-hz 30 --log-file p2_authoritative_tcp.csv`

## Local UDP

Terminal 1: `python server.py --protocol udp --host 127.0.0.1 --delay-ms 100 --loss-pct 0`

Terminal 2: `python client.py --protocol udp --update-hz 30 --log-file p1_authoritative_udp.csv`

Terminal 3: `python client.py --protocol udp --update-hz 30 --log-file p2_authoritative_udp.csv`

## Two computers

On the server computer: `python server.py --protocol udp --host 0.0.0.0 --delay-ms 100 --loss-pct 0`

On remote computer: `python client.py --protocol udp --host YOUR_SERVER_LAN_IPV4 --update-hz 30`

Restrict firewall permissions to trusted private networks. For TCP, change both protocol flags.

## How the new protocol works

- Each client sends `{"type":"input","seq":N,"dx":-1|0|1,"dy":-1|0|1,"sent_at":...}`.
- The server validates sequence order and bounds motion using elapsed server time.
- The server broadcasts authoritative `move` messages to *all* players, including the sender (for position reconciliation).
- Coin collection and scores are determined only by the server.
- The client predicts its own movement locally and replays pending inputs after receiving a server position.
- `--delay-ms` delays outgoing authoritative movement; `--loss-pct` discards some UDP application messages (not actual network packets). Coin/score updates are not impaired by this setting.
- CSV `send` rows now represent input messages; `receive` rows represent authoritative movement updates from other players. Their sizes may differ from older versions. `update_age_ms` is based on the originator timestamp and assumes synchronized system clocks on different computers.

## Tests

Try TCP/UDP at 0 and 100 ms delay. Test that both players see each other, coins are collected once, scores agree, and local motion is responsive. Under 30% UDP simulated loss expect stale/choppy remote positions and potentially more visible prediction corrections. Make new CSV filenames to preserve earlier baseline results.

Run `python -m py_compile server.py client.py` to check syntax. The included `test_network.py` is an optional smoke test for machines allowing local sockets.

**Limitations:** This is an incremental client-prediction implementation, not lag compensation or interpolation. Prediction uses elapsed client time; server uses elapsed server time, so corrections may occasionally be noticeable. Server state is not persisted. Server-authoritative speed is bounded but does not yet handle obstacles, anti-cheat identities, or internet security. Multi-hop is a separate later network experiment.
