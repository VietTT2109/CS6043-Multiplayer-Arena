# CS6043 Coin Rush (Timed Rounds)

Replace your current **server.py** and **client.py** together. Keep your earlier logs and `analyze_logs.py` (the included analysis copy is unchanged).

## New features

- Waiting lobby until at least `--min-players` are connected (default 2)
- Timed rounds (default 60 seconds)
- Winner/tie displayed at the end of the round
- Automatic new round after an 8-second intermission
- Game returns to lobby when active player count drops below minimum
- UDP best-effort leave on window close, plus 15-second timeout for unexpected disconnects
- TCP disconnect handling retained
- Existing server-authoritative movement, client-side prediction, coins, and CSV logging retained

## Local testing (3 terminals)

TCP:
```
python server.py --protocol tcp --round-seconds 20 --intermission-seconds 5
python client.py --protocol tcp --update-hz 30 --log-file p1_tcp.csv
python client.py --protocol tcp --update-hz 30 --log-file p2_tcp.csv
```

UDP: stop all three processes and replace `tcp` with `udp`. Server example:
```
python server.py --protocol udp --round-seconds 20 --intermission-seconds 5 --delay-ms 100 --loss-pct 10
```

## LAN play

Server: `python server.py --protocol udp --host 0.0.0.0`

Other laptop: `python client.py --protocol udp --host SERVER_LAN_IP`

Use a trusted network; allow inbound UDP/TCP port 5000 on the server host as needed.

## Tests

```
python -m unittest -v test_rounds.py test_network.py
```

## Caveats

- Disconnects during rounds with fewer than 2 players return to waiting instead of awarding a winner.
- UDP `leave` is best-effort. If it is lost or the app crashes, stale players are removed after ~15 seconds.
- TCP reconnect gets a new player ID; there are no persistent accounts.
- This milestone does not implement a manual ready-up lobby or manual restart. Rematches are automatic.
- Timers use server monotonic time; client countdown is an approximation refreshed by world updates.
- `--loss-pct` drops only outgoing UDP movement updates at the application level, not actual IP packets; it is not realistic network congestion.
