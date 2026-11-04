#!/usr/bin/env python3
"""Aggregate completed runs and optionally plot mean update age."""
import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', default='results/experiments')
    args = ap.parse_args()
    root = Path(args.results)
    groups = defaultdict(list)
    for path in root.glob('*/summary.csv'):
        with (path.parent / 'settings.json').open(encoding='utf-8') as fp:
            import json
            config = json.load(fp)
        with path.open(newline='', encoding='utf-8') as fp:
            for row in csv.DictReader(fp):
                key = (config['protocol'], config['bw_mbps'], config['delay_ms'],
                       config['loss_pct'], config['iperf_kbps'])
                age = float(row['mean_age'])
                if math.isfinite(age):
                    groups[key].append(age)
    if not groups:
        print('No summarized trials found in', root)
        return
    records = []
    for key, vals in sorted(groups.items()):
        records.append(dict(protocol=key[0], bw_mbps=key[1], delay_ms=key[2],
                            loss_pct=key[3], iperf_kbps=key[4], n_client_logs=len(vals),
                            mean_age_ms=round(statistics.mean(vals), 3),
                            sd_age_ms=round(statistics.stdev(vals), 3) if len(vals)>1 else 0))
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'experiment_summary.csv').open('w', newline='', encoding='utf-8') as fp:
        writer = csv.DictWriter(fp, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    for r in records:
        print(f"{r['protocol']:<7} bw={r['bw_mbps']:>5} bg={r['iperf_kbps']:>4} "
              f"mean={r['mean_age_ms']:>8.1f} ms n={r['n_client_logs']}")
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print('Plot skipped: install matplotlib if desired')
        return
    labels = [f"{r['protocol']}\nbg {r['iperf_kbps']}K" for r in records]
    plt.figure(figsize=(max(7,len(labels)*0.9), 4))
    plt.bar(range(len(records)), [r['mean_age_ms'] for r in records])
    plt.xticks(range(len(labels)), labels)
    plt.ylabel('Mean update age (ms)')
    plt.title('Coin Rush: Protocol Comparison')
    plt.tight_layout()
    plt.savefig(root / 'protocol_comparison.png', dpi=140)
    plt.close()


if __name__ == '__main__':
    main()
