# CS6043 Multiplayer Coin Rush — playable prototype

This update adds a shared collectible coin and server-owned scores to the existing TCP/UDP arena. It preserves 30 Hz movement, CSV event logging, and the optional movement relay delay / UDP application-message loss. No new Python dependencies are required for the game (Tkinter is bundled in most Windows Python installs).

## Replace only two files
Copy `server.py` and `client.py` into your **existing** project folder. Keep `analyze_logs.py` and your CSV files. The CSV schema for movement send/receive events is unchanged.

## Local test (three terminals)

Terminal 1:

    python server.py --protocol tcp --delay-ms 100

Terminal 2:

    python client.py --protocol tcp --update-hz 30 --log-file p1_coin_tcp.csv

Terminal 3:

    python client.py --protocol tcp --update-hz 30 --log-file p2_coin_tcp.csv

Try moving onto the yellow coin. The scoreboard should increment and the coin should respawn at a new location for both clients.

For UDP, stop everything and run `--protocol udp` everywhere. Example server:

    python server.py --protocol udp --delay-ms 100 --loss-pct 10

## Playing from different computers on a LAN

On the server PC:

    ipconfig
    python server.py --protocol tcp

Note the server PC's LAN IPv4 address, e.g. `192.168.1.50` (example only). On another Windows PC connected to the same reachable private LAN:

    python client.py --protocol tcp --host 192.168.1.50

Allow inbound TCP port 5000 on the server's *private* network firewall when necessary. For UDP use `--protocol udp` on both ends and allow UDP port 5000. University/guest Wi-Fi can isolate devices. Avoid exposing port 5000 to the public internet: this is a development server without authentication or abuse protections.

## What is authoritative, and what is not?

- **Server-owned**: coin spawn, collision against reported position, scores, and scoreboard broadcasts.
- **Client-owned for now**: movement and position updates. A malicious client could falsify position. The next stage is server-authoritative input simulation, movement validation, and better reconnection handling.
- **Transport experiments**: the `--delay-ms` and `--loss-pct` options affect relayed movement updates. Coin/score events are sent separately; UDP score snapshots are repeated periodically and should not be interpreted as reliable UDP acknowledgements.
- **Multi-hop**: playing from two devices does not by itself guarantee a routed multi-hop network. We will add controlled routing/emulation for those experiments later.

## Files

- `server.py`: shared world and TCP/UDP movement relay.
- `client.py`: Tkinter arena, coin UI, scoreboard, CSV movement logging.
- `analyze_logs.py`: prior CSV latency analysis (unchanged).

Commit after testing:

    git add server.py client.py README.md
    git commit -m "Add multiplayer coin collection and server scoreboard"
    git push
