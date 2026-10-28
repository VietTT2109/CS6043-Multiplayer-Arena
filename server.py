"""Multiplayer relay server with configurable application-level impairments."""
import argparse
import heapq
import json
import random
import socket
import threading
import time

HOST = '0.0.0.0'
BUFFER = 4096


def valid_update(msg):
    return (isinstance(msg, dict) and msg.get('type') == 'move'
            and all(isinstance(msg.get(k), (int, float)) for k in ('x', 'y', 'seq'))
            and 0 <= msg['x'] <= 800 and 0 <= msg['y'] <= 500)


class DelayedRelay:
    """Deliver relayed messages after a configured delay, optionally dropping UDP sends."""
    def __init__(self, delay_ms, loss_pct):
        self.delay = delay_ms / 1000
        self.loss = loss_pct / 100
        self.cv = threading.Condition()
        self.jobs = []
        self.order = 0
        threading.Thread(target=self.worker, daemon=True).start()

    def submit(self, deliver, udp=False):
        if udp and random.random() < self.loss:
            return
        with self.cv:
            self.order += 1
            heapq.heappush(self.jobs, (time.monotonic() + self.delay, self.order, deliver))
            self.cv.notify()

    def worker(self):
        while True:
            with self.cv:
                while not self.jobs:
                    self.cv.wait()
                when, _, _ = self.jobs[0]
                remaining = when - time.monotonic()
                if remaining > 0:
                    self.cv.wait(remaining)
                    continue
                _, _, deliver = heapq.heappop(self.jobs)
            try:
                deliver()
            except OSError:
                pass


def serve_udp(port, relay):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, port))
    players = {}
    next_id = 1
    print(f'UDP server listening on {HOST}:{port}', flush=True)
    while True:
        raw, addr = sock.recvfrom(BUFFER)
        try:
            msg = json.loads(raw)
        except (ValueError, UnicodeError):
            continue
        if not isinstance(msg, dict):
            continue
        if addr not in players:
            if msg.get('type') != 'join':
                continue
            players[addr] = (next_id, time.monotonic())
            sock.sendto(json.dumps({'type': 'welcome', 'id': next_id}).encode(), addr)
            print(f'Player {next_id} joined from {addr}', flush=True)
            next_id += 1
            continue
        player_id, _ = players[addr]
        players[addr] = (player_id, time.monotonic())
        if valid_update(msg):
            update = {'type': 'move', 'id': player_id, 'x': msg['x'],
                      'y': msg['y'], 'seq': int(msg['seq']),
                      'sent_at': msg.get('sent_at')}
            wire = json.dumps(update).encode()
            for peer, (_, seen) in list(players.items()):
                if peer != addr and time.monotonic() - seen < 15:
                    relay.submit(lambda peer=peer, wire=wire: sock.sendto(wire, peer), udp=True)


def serve_tcp(port, relay):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, port))
    listener.listen()
    clients = {}
    lock = threading.Lock()
    next_id = 1
    print(f'TCP server listening on {HOST}:{port}', flush=True)

    def send_msg(conn, msg):
        conn.sendall((json.dumps(msg) + '\n').encode())

    def handle(conn, player_id):
        try:
            with conn:
                for line in conn.makefile('r', encoding='utf-8'):
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    if not valid_update(msg):
                        continue
                    update = {'type': 'move', 'id': player_id, 'x': msg['x'],
                              'y': msg['y'], 'seq': int(msg['seq']),
                              'sent_at': msg.get('sent_at')}
                    with lock:
                        recipients = [c for c in clients if c is not conn]
                    for peer in recipients:
                        relay.submit(lambda peer=peer, update=update: send_msg(peer, update))
        except (OSError, UnicodeError):
            pass
        finally:
            with lock:
                clients.pop(conn, None)
            print(f'Player {player_id} disconnected', flush=True)

    while True:
        conn, addr = listener.accept()
        with lock:
            player_id = next_id
            next_id += 1
            clients[conn] = player_id
        try:
            send_msg(conn, {'type': 'welcome', 'id': player_id})
        except OSError:
            with lock:
                clients.pop(conn, None)
            conn.close()
            continue
        print(f'Player {player_id} joined from {addr}', flush=True)
        threading.Thread(target=handle, args=(conn, player_id), daemon=True).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--protocol', choices=['tcp', 'udp'], required=True)
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--delay-ms', type=float, default=0, help='Extra server-to-client delay in milliseconds')
    parser.add_argument('--loss-pct', type=float, default=0, help='Chance to drop a relayed UDP movement message (0-100)')
    args = parser.parse_args()
    if args.delay_ms < 0 or not 0 <= args.loss_pct <= 100:
        parser.error('delay-ms must be nonnegative; loss-pct must be 0-100')
    if args.protocol == 'tcp' and args.loss_pct:
        parser.error('TCP packet loss requires network-level emulation; --loss-pct only applies to UDP')
    print(f'Configured: {args.delay_ms:g} ms added relay delay, {args.loss_pct:g}% UDP movement loss')
    relay = DelayedRelay(args.delay_ms, args.loss_pct)
    (serve_tcp if args.protocol == 'tcp' else serve_udp)(args.port, relay)
