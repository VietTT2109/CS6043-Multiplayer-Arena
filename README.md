# Phase 8 — Synchronization measurements

New CSV fields: `correction_px` and `corrected`. Each `correction` event is generated when an authoritative acknowledgment is received for the local player. `correction_px` is the distance between the displayed/predicted position immediately before reconciliation and the reconciled predicted position after replaying outstanding inputs. `corrected` is 1 when this adjustment is >=1 pixel. These are *reconciliation adjustment* measurements, not a perfect time-aligned ground-truth position error; receiving an update is not itself evidence of a visible movement jump.

Existing `receive` events still log remote movement update age (calculated from sender wall time; requires synchronized clocks across computers) and sequence gaps (not precise physical packet loss).

For a quick two-client run:

Terminal 1: `python server.py --protocol tcp --delay-ms 100 --round-seconds 120`

Terminal 2: `python client.py --protocol tcp --update-hz 30 --log-file p8_tcp_p1.csv`

Terminal 3: `python client.py --protocol tcp --update-hz 30 --log-file p8_tcp_p2.csv`

Change all protocols to `udp` for UDP and add `--loss-pct 10` to the server if wanted. Analyze with `python analyze_logs.py p8_tcp_p1.csv p8_tcp_p2.csv` (matplotlib optional for graph).

Keep baseline Phase 7 experiments separate: Phase 8 adds logging and metrics; for meaningful comparison, freeze the version for each testing group.
