#!/usr/bin/env python3
"""Run a timed Coin Rush test in a fresh Mininet topology.

Requires Ubuntu with Mininet, Tk, iperf3 and access to an X desktop:
    sudo -E python3 run_experiment.py --protocol hybrid --duration 60
"""
import argparse
import csv
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

from multihop import build_network

HERE = Path(__file__).resolve().parent


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--protocol', choices=['tcp', 'udp', 'hybrid'], required=True)
    p.add_argument('--duration', type=int, default=60)
    p.add_argument('--bw-mbps', type=float, default=0.15)
    p.add_argument('--delay-ms', type=float, default=50)
    p.add_argument('--loss-pct', type=float, default=2)
    p.add_argument('--queue-packets', type=int, default=50)
    p.add_argument('--iperf-kbps', type=int, default=60, help='0 disables background traffic')
    p.add_argument('--traffic-start', type=int, default=15)
    p.add_argument('--traffic-duration', type=int, default=30)
    p.add_argument('--update-hz', type=float, default=30)
    p.add_argument('--payload-bytes', type=int, default=0)
    p.add_argument('--burst-count', type=int, default=0)
    p.add_argument('--window-scale', type=float, default=0.75)
    p.add_argument('--trial', type=int, default=1)
    p.add_argument('--output-dir', default='results/phase13')
    a = p.parse_args()
    if a.duration < 10 or a.traffic_start < 0 or a.traffic_duration < 0 or a.iperf_kbps < 0:
        p.error('invalid duration or background traffic')
    if a.iperf_kbps and a.traffic_start + a.traffic_duration > a.duration:
        p.error('traffic window must end before the trial finishes')
    if a.bw_mbps <= 0 or a.delay_ms < 0 or not 0 <= a.loss_pct <= 100 or a.queue_packets < 1:
        p.error('invalid network impairment setting')
    if not 0 < a.update_hz <= 60 or not 0.4 <= a.window_scale <= 1.5:
        p.error('invalid update rate or window scale')
    if a.payload_bytes and not 128 <= a.payload_bytes <= 1200:
        p.error('payload must be 0 or 128..1200')
    if not 0 <= a.burst_count <= 100:
        p.error('invalid burst-count')
    if not os.environ.get('DISPLAY'):
        p.error('DISPLAY is missing; run from an Ubuntu graphical desktop with sudo -E')
    return a


def stop(proc):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=4)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def main():
    a = parse_args()
    safe = re.sub(r'[^a-zA-Z0-9_-]', '_',
                  f'{a.protocol}_bw{a.bw_mbps:g}_d{a.delay_ms:g}_loss{a.loss_pct:g}_bg{a.iperf_kbps}_trial{a.trial}')
    root = (HERE / a.output_dir).resolve()
    run_dir = root / (dt.datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + safe)
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / 'settings.json').write_text(json.dumps(vars(a), indent=2), encoding='utf-8')
    print(f'Logs: {run_dir}', flush=True)

    net = None
    procs = []
    handles = []
    def launch(host, cmd, name):
        f = (run_dir / f'{name}.log').open('w', encoding='utf-8')
        handles.append(f)
        pr = host.popen(cmd, cwd=str(HERE), stdout=f, stderr=subprocess.STDOUT,
                        env=dict(os.environ, PYTHONUNBUFFERED='1'))
        procs.append(pr)
        return pr

    try:
        net = build_network(a.bw_mbps, a.delay_ms, a.loss_pct, a.queue_packets)
        srv, c1, c2 = (net[x] for x in ('srv', 'c1', 'c2'))
        print('Testing IP connectivity...', flush=True)
        for host in (c1, c2):
            result = host.cmd('ping -c 1 -W 2 10.10.3.2')
            if '1 received' not in result and '1 packets received' not in result:
                print(f'Warning: {host.name} ping unsuccessful; continuing (loss may cause it)')

        launch(srv, [sys.executable, str(HERE / 'server.py'), '--protocol', a.protocol,
                     '--host', '10.10.3.2', '--delay-ms', '0', '--bandwidth-kbps', '0',
                     '--round-seconds', str(a.duration + 20), '--min-players', '2'], 'server')
        time.sleep(1.5)
        if a.iperf_kbps:
            launch(srv, ['iperf3', '-s', '-p', '5201'], 'iperf_server')
            time.sleep(0.5)

        for host in (c1, c2):
            launch(host, [sys.executable, str(HERE / 'client.py'), '--protocol', a.protocol,
                          '--host', '10.10.3.2', '--update-hz', str(a.update_hz),
                          '--payload-bytes', str(a.payload_bytes), '--burst-count', str(a.burst_count),
                          '--window-scale', str(a.window_scale),
                          '--duration-seconds', str(a.duration),
                          '--log-file', str(run_dir / f'{host.name}.csv')], host.name)
        print('Game windows started. Move the players manually during the timed trial.', flush=True)
        start = time.monotonic()
        traffic = None
        while time.monotonic() - start < a.duration:
            elapsed = time.monotonic() - start
            if a.iperf_kbps and traffic is None and elapsed >= a.traffic_start:
                print(f'Starting iperf3 reverse UDP background load: {a.iperf_kbps} kbps', flush=True)
                traffic = launch(c1, ['iperf3', '-c', '10.10.3.2', '-u', '-b', f'{a.iperf_kbps}K',
                                      '-t', str(a.traffic_duration), '-R', '-p', '5201'], 'iperf_client')
            time.sleep(0.2)
        time.sleep(1.2)  # allow clients' auto-close to flush CSV
        for proc in procs:
            stop(proc)
        try:
            from analyze_logs import summarize
            rows = []
            for client in ('c1', 'c2'):
                path = run_dir / f'{client}.csv'
                if path.exists():
                    data = summarize(path)
                    data['run'] = run_dir.name
                    rows.append(data)
            if rows:
                with (run_dir / 'summary.csv').open('w', newline='', encoding='utf-8') as fp:
                    out = csv.DictWriter(fp, fieldnames=list(rows[0]))
                    out.writeheader()
                    out.writerows(rows)
                print('Summary:', flush=True)
                for row in rows:
                    print(f"  {row['file']}: mean age {row['mean_age']:.1f}ms; "
                          f"P95 {row['p95_age']:.1f}ms; rx {row['receive_hz']:.1f}Hz", flush=True)
            else:
                print('No client logs were produced. Inspect c1.log/c2.log and DISPLAY permissions.')
        except Exception as exc:
            print(f'Could not summarize logs: {exc}; raw logs preserved.', file=sys.stderr)
        print('Finished:', run_dir, flush=True)
    finally:
        for proc in reversed(procs):
            stop(proc)
        for handle in handles:
            handle.close()
        if net is not None:
            net.stop()


if __name__ == '__main__':
    main()
