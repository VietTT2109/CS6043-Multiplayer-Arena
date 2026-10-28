# CS6043 Multiplayer Arena — experimental starter

Python 3 with standard library only. In three terminals run:

```powershell
python server.py --protocol tcp --delay-ms 100
python client.py --protocol tcp --update-hz 10 --log-file player1_tcp.csv
python client.py --protocol tcp --update-hz 10 --log-file player2_tcp.csv
```

Stop all three processes to change protocol, then repeat with `udp` replacing `tcp` and distinct log names. To simulate relayed UDP application-message loss, run `python server.py --protocol udp --delay-ms 100 --loss-pct 30`.

`--update-hz` controls the target rate of movement updates, from >0 to 30/s. Actual rate may be lower because the GUI updates approximately every 33 ms. `--log-file` writes a separate CSV for each client; always use different paths for the two players and each run. Close windows normally to finish writing files.

CSV fields include send/receive event, protocol, player IDs, sequence numbers, timestamps, sent timestamp, update age in milliseconds, coordinates, payload byte count, and observed sequence gaps. Age is one-way **update age**, not RTT, and works most meaningfully with synchronized clocks (same computer is fine). Sequence gaps are not exact network packet loss; missing last updates are not counted, and out-of-order updates may be discarded. Server delay is application-level relay delay; UDP loss is application-message drop, NOT TCP network packet loss. Future experiments should use network-level emulation for TCP loss and congestion.
