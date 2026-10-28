"""Analyze client CSV logs. Usage: python analyze_logs.py file1.csv file2.csv ..."""
import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


def percentile(values, percentile_value):
    values = sorted(values)
    if not values:
        return float('nan')
    index = (len(values) - 1) * percentile_value / 100
    low = int(index)
    high = min(low + 1, len(values) - 1)
    return values[low] * (high - index) + values[high] * (index - low)


def summarize(path):
    with open(path, newline='', encoding='utf-8') as handle:
        rows = list(csv.DictReader(handle))
    sends = [r for r in rows if r['event'] == 'send']
    receives = [r for r in rows if r['event'] == 'receive']
    ages = [float(r['update_age_ms']) for r in receives if r.get('update_age_ms')]
    gaps = sum(int(r['seq_gap'] or 0) for r in receives)

    def rate(events):
        if len(events) < 2:
            return 0.0
        elapsed = float(events[-1]['timestamp']) - float(events[0]['timestamp'])
        return (len(events) - 1) / elapsed if elapsed > 0 else 0.0

    return {'file': Path(path).name, 'protocol': rows[0]['protocol'] if rows else '?',
            'sent': len(sends), 'received': len(receives),
            'send_hz': rate(sends), 'receive_hz': rate(receives),
            'mean_age': statistics.mean(ages) if ages else float('nan'),
            'median_age': statistics.median(ages) if ages else float('nan'),
            'p95_age': percentile(ages, 95), 'seq_gaps': gaps}


def main():
    parser = argparse.ArgumentParser(description='Summarize TCP/UDP movement CSV files')
    parser.add_argument('files', nargs='+', help='CSV files from --log-file')
    parser.add_argument('--save', default='comparison.png', help='Output graph filename')
    args = parser.parse_args()
    results = [summarize(path) for path in args.files]
    print(f'{"File":<24} {"Sent":>6} {"Recv":>6} {"Tx Hz":>7} {"Rx Hz":>7} {"Mean ms":>9} {"Med ms":>8} {"P95 ms":>8} {"Gaps":>6}')
    for r in results:
        print(f'{r["file"]:<24} {r["sent"]:>6} {r["received"]:>6} {r["send_hz"]:>7.1f} '
              f'{r["receive_hz"]:>7.1f} {r["mean_age"]:>9.1f} {r["median_age"]:>8.1f} '
              f'{r["p95_age"]:>8.1f} {r["seq_gaps"]:>6}')
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print('\nGraph skipped: install matplotlib with: python -m pip install matplotlib')
        return
    labels = [r['file'].removesuffix('.csv') for r in results]
    fig, ax = plt.subplots(figsize=(max(8, len(labels) * 1.6), 5))
    x = list(range(len(labels)))
    ax.bar([i - .2 for i in x], [r['mean_age'] for r in results], width=.4, label='Average age')
    ax.bar([i + .2 for i in x], [r['p95_age'] for r in results], width=.4, label='95th percentile age')
    ax.set_xticks(x, labels, rotation=20, ha='right')
    ax.set_ylabel('Received update age (ms)')
    ax.set_title('Multiplayer movement update age by client log')
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.save, dpi=160)
    print(f'\nSaved graph to {args.save}')


if __name__ == '__main__':
    main()
