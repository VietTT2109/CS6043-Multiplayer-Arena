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
    traffic = [r for r in rows if r['event'] == 'traffic_send']
    reliable_acks = [r for r in rows if r['event'] == 'event_ack']
    corrections = [r for r in rows if r['event'] == 'correction']
    correction_values = [float(r['correction_px']) for r in corrections if r.get('correction_px')]
    correction_count = sum(int(r.get('corrected') or 0) for r in corrections)

    def rate(events):
        if len(events) < 2:
            return 0.0
        elapsed = float(events[-1]['timestamp']) - float(events[0]['timestamp'])
        return (len(events) - 1) / elapsed if elapsed > 0 else 0.0

    def nbytes(row):
        if row.get('wire_bytes'):
            return int(row['wire_bytes'])
        if row.get('payload_bytes'):
            return int(row['payload_bytes']) + (1 if row.get('protocol') == 'tcp' and row.get('event') in ('send', 'traffic_send') else 0)
        return 0

    def elapsed(events):
        if len(events) < 2:
            return 0.0
        return max(0.0, float(events[-1]['timestamp']) - float(events[0]['timestamp']))

    total_sent = sum(nbytes(r) for r in sends + traffic)
    total_recv = sum(nbytes(r) for r in receives)
    send_period = elapsed(sends + traffic if not traffic else sorted(sends + traffic, key=lambda r: float(r['timestamp'])))
    recv_period = elapsed(receives)

    return {'file': Path(path).name, 'protocol': rows[0]['protocol'] if rows else '?',
            'sent': len(sends), 'received': len(receives), 'burst_sent': len(traffic), 'reliable_acks': len(reliable_acks),
            'tx_bytes': total_sent, 'rx_bytes': total_recv,
            'tx_Bps': total_sent / send_period if send_period else 0,
            'rx_Bps': total_recv / recv_period if recv_period else 0,
            'send_hz': rate(sends), 'receive_hz': rate(receives),
            'mean_age': statistics.mean(ages) if ages else float('nan'),
            'median_age': statistics.median(ages) if ages else float('nan'),
            'p95_age': percentile(ages, 95), 'seq_gaps': gaps,
            'mean_correction': statistics.mean(correction_values) if correction_values else float('nan'),
            'p95_correction': percentile(correction_values, 95),
            'correction_count': correction_count}


def main():
    parser = argparse.ArgumentParser(description='Summarize TCP/UDP movement CSV files')
    parser.add_argument('files', nargs='+', help='CSV files from --log-file')
    parser.add_argument('--save', default='comparison.png', help='Output graph filename')
    args = parser.parse_args()
    results = [summarize(path) for path in args.files]
    print(f'{"File":<24} {"Sent":>6} {"Recv":>6} {"Tx Hz":>7} {"Rx Hz":>7} {"Mean ms":>9} {"Med ms":>8} {"P95 ms":>8} {"Gaps":>6} {"Corr avg":>9} {"Corr P95":>9} {"Corrections":>11}')
    for r in results:
        print(f'{r["file"]:<24} {r["sent"]:>6} {r["received"]:>6} {r["send_hz"]:>7.1f} '
              f'{r["receive_hz"]:>7.1f} {r["mean_age"]:>9.1f} {r["median_age"]:>8.1f} '
              f'{r["p95_age"]:>8.1f} {r["seq_gaps"]:>6} {r["mean_correction"]:>9.2f} '
              f'{r["p95_correction"]:>9.2f} {r["correction_count"]:>11}')
    print('\nUDP reliable-world ACKs by client log:')
    for r in results:
        print(f'{r["file"]}: {r["reliable_acks"]} ACK transmissions (includes duplicate acknowledgments)')
    print('\nApplication traffic summary (JSON bytes; TCP includes newline delimiter, not TCP/IP headers):')
    print(f'{"File":<24} {"Burst msgs":>11} {"TX bytes":>10} {"RX bytes":>10} {"TX B/s":>10} {"RX B/s":>10}')
    for r in results:
        print(f'{r["file"]:<24} {r["burst_sent"]:>11} {r["tx_bytes"]:>10} '
              f'{r["rx_bytes"]:>10} {r["tx_Bps"]:>10.1f} {r["rx_Bps"]:>10.1f}')
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
    graph_path = Path(args.save)
    throughput_path = graph_path.with_name(graph_path.stem + '_throughput.png')
    fig2, ax2 = plt.subplots(figsize=(max(8, len(labels) * 1.6), 5))
    ax2.bar([i-.2 for i in x], [r['tx_Bps'] for r in results], width=.4, label='Sent')
    ax2.bar([i+.2 for i in x], [r['rx_Bps'] for r in results], width=.4, label='Received')
    ax2.set_xticks(x, labels, rotation=20, ha='right')
    ax2.set_ylabel('Logged application bytes per second')
    ax2.set_title('Measured application traffic by client log')
    ax2.legend()
    fig2.tight_layout()
    fig2.savefig(throughput_path, dpi=160)
    print(f'Saved graph to {throughput_path}')
    plt.close(fig)
    plt.close(fig2)


if __name__ == '__main__':
    main()
