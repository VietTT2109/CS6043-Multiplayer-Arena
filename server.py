"""Coin Rush: server-authoritative TCP/UDP movement, timed rounds, optional relay impairment.

Movement updates are direction inputs; the server computes and validates positions.
--loss-pct drops outgoing UDP application updates, NOT IP packets.
"""
import argparse
import heapq
import json
import math
import random
import socket
import threading
import time

WIDTH, HEIGHT = 800, 500
SPEED = 150.0
PLAYER_R, COIN_R = 13, 11
BUFFER = 8192


def valid_input(m):
    return (isinstance(m, dict) and m.get('type') == 'input'
            and type(m.get('seq')) is int and m['seq'] >= 0
            and all(type(m.get(k)) in (int, float) and math.isfinite(m[k])
                    and -1 <= m[k] <= 1 for k in ('dx', 'dy')))


class Game:
    def __init__(self, round_seconds=60, intermission_seconds=8, min_players=2):
        self.round_seconds = round_seconds
        self.intermission_seconds = intermission_seconds
        self.min_players = min_players
        self.phase = 'waiting'
        self.deadline = None
        self.winners = []
        self.round_number = 0
        self.lock = threading.RLock()
        self.positions = {}    # id: [x,y]
        self.last_input = {}   # id: monotonic time of last accepted input
        self.seqs = {}         # id: last accepted sequence number
        self.scores = {}
        self.coin = self.new_coin()
        self.version = 0

    @staticmethod
    def new_coin():
        return [random.randint(45, WIDTH - 45), random.randint(45, HEIGHT - 45)]

    def join(self, pid):
        with self.lock:
            self.positions[pid] = [WIDTH / 2.0, HEIGHT / 2.0]
            self.last_input[pid] = time.monotonic()
            self.seqs[pid] = -1
            self.scores[pid] = 0
            self.version += 1
            self.advance()
            return self.world()

    def leave(self, pid):
        with self.lock:
            for d in (self.positions, self.last_input, self.seqs, self.scores):
                d.pop(pid, None)
            self.version += 1
            self.advance()
            return self.world()

    def advance(self):
        """Update round transitions using monotonic server time (lock required)."""
        with self.lock:
            now = time.monotonic()
            count = len(self.scores)
            if count < self.min_players:
                if self.phase != 'waiting':
                    self.phase, self.deadline, self.winners = 'waiting', None, []
                    self.version += 1
                return
            if self.phase == 'waiting' or (self.phase == 'finished' and now >= self.deadline):
                self.phase = 'playing'
                self.deadline = now + self.round_seconds
                self.round_number += 1
                self.winners = []
                self.coin = self.new_coin()
                for pid in self.scores:
                    self.scores[pid] = 0
                self.version += 1
            elif self.phase == 'playing' and now >= self.deadline:
                self.phase = 'finished'
                best = max(self.scores.values(), default=0)
                self.winners = sorted(pid for pid, score in self.scores.items() if score == best)
                self.deadline = now + self.intermission_seconds
                self.version += 1

    def world(self):
        with self.lock:
            return {'type': 'world', 'coin': self.coin[:],
                    'scores': {str(k): v for k, v in self.scores.items()},
                    'version': self.version,
                    'phase': self.phase,
                    'remaining': max(0, self.deadline - time.monotonic()) if self.deadline else 0,
                    'winners': self.winners[:], 'round': self.round_number,
                    'player_count': len(self.scores), 'min_players': self.min_players}

    def input(self, pid, msg):
        """Returns (authoritative movement update, world changed), or (None, False)."""
        if not valid_input(msg):
            return None, False
        with self.lock:
            if pid not in self.positions or msg['seq'] <= self.seqs[pid]:
                return None, False
            now = time.monotonic()
            # Server elapsed time bounds travel; forged client timestamps cannot accelerate movement.
            dt = max(0.0, min(now - self.last_input[pid], 0.1))
            self.last_input[pid] = now
            self.seqs[pid] = msg['seq']
            dx, dy = float(msg['dx']), float(msg['dy'])
            norm = max(1.0, math.hypot(dx, dy))
            x, y = self.positions[pid]
            x = min(WIDTH - 15, max(15, x + SPEED * dx / norm * dt))
            y = min(HEIGHT - 15, max(15, y + SPEED * dy / norm * dt))
            self.positions[pid] = [x, y]
            coin_changed = False
            if self.phase == 'playing' and (x-self.coin[0])**2 + (y-self.coin[1])**2 <= (PLAYER_R+COIN_R)**2:
                self.scores[pid] += 1
                old = self.coin
                for _ in range(30):
                    self.coin = self.new_coin()
                    if (self.coin[0]-old[0])**2 + (self.coin[1]-old[1])**2 > 120**2:
                        break
                self.version += 1
                coin_changed = True
            update = {'type': 'move', 'id': pid, 'x': round(x, 2), 'y': round(y, 2),
                      'seq': msg['seq'], 'sent_at': msg.get('sent_at')}
            return update, coin_changed


class DelayedRelay:
    def __init__(self, delay_ms, loss_pct):
        self.delay = delay_ms / 1000
        self.loss = loss_pct / 100
        self.cv = threading.Condition()
        self.jobs = []
        self.count = 0
        threading.Thread(target=self.run, daemon=True).start()

    def submit(self, deliver, udp=False):
        if udp and random.random() < self.loss:
            return
        with self.cv:
            self.count += 1
            heapq.heappush(self.jobs, (time.monotonic() + self.delay, self.count, deliver))
            self.cv.notify()

    def run(self):
        while True:
            with self.cv:
                while not self.jobs:
                    self.cv.wait()
                wait = self.jobs[0][0] - time.monotonic()
                if wait > 0:
                    self.cv.wait(wait)
                    continue
                _, _, deliver = heapq.heappop(self.jobs)
            try:
                deliver()
            except OSError:
                pass


def serve_tcp(host, port, relay, round_seconds, intermission_seconds, min_players):
    game = Game(round_seconds, intermission_seconds, min_players)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen()
    clients = {}  # conn -> (player_id, send_lock)
    clients_lock = threading.Lock()
    print(f'TCP server on {host}:{port}', flush=True)

    def send(conn, msg):
        with clients_lock:
            client = clients.get(conn)
        if client is None:
            return
        with client[1]:
            conn.sendall((json.dumps(msg) + '\n').encode())

    def broadcast_world():
        world = game.world()
        with clients_lock:
            peers = list(clients)
        for c in peers:
            try:
                send(c, world)
            except OSError:
                pass

    def heartbeat():
        while True:
            time.sleep(0.25)
            game.advance()
            broadcast_world()

    threading.Thread(target=heartbeat, daemon=True).start()

    def handle(conn, pid):
        try:
            with conn:
                for line in conn.makefile('r', encoding='utf-8'):
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    update, changed = game.input(pid, msg)
                    if update:
                        with clients_lock:
                            peers = list(clients)
                        for c in peers:  # INCLUDING originator: authoritative correction / ACK
                            relay.submit(lambda c=c, update=update: send(c, update))
                        if changed:
                            broadcast_world()
        except (OSError, UnicodeError):
            pass
        finally:
            with clients_lock:
                clients.pop(conn, None)
            game.leave(pid)
            broadcast_world()
            print(f'Player {pid} left', flush=True)

    pid_next = 1
    while True:
        conn, addr = listener.accept()
        with clients_lock:
            pid = pid_next
            pid_next += 1
            clients[conn] = (pid, threading.Lock())
        game.join(pid)
        print(f'Player {pid} joined from {addr}', flush=True)
        send(conn, {'type': 'welcome', 'id': pid, 'x': WIDTH/2, 'y': HEIGHT/2})
        broadcast_world()
        threading.Thread(target=handle, args=(conn, pid), daemon=True).start()


def serve_udp(host, port, relay, round_seconds, intermission_seconds, min_players):
    game = Game(round_seconds, intermission_seconds, min_players)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    clients = {}  # address -> [id, last_seen]
    clients_lock = threading.Lock()
    print(f'UDP server on {host}:{port}', flush=True)

    def send(addr, message):
        sock.sendto(json.dumps(message).encode(), addr)

    def broadcast_world():
        with clients_lock:
            peers = list(clients)
        world = game.world()
        for addr in peers:
            try:
                send(addr, world)
            except OSError:
                pass

    def heartbeat():
        while True:
            time.sleep(0.5)
            now = time.monotonic()
            with clients_lock:
                expired = [(addr, info[0]) for addr, info in clients.items()
                           if now - info[1] > 15]
                for addr, _ in expired:
                    clients.pop(addr, None)
            for _, pid in expired:
                game.leave(pid)
            game.advance()
            broadcast_world()

    threading.Thread(target=heartbeat, daemon=True).start()
    pid_next = 1
    while True:
        raw, addr = sock.recvfrom(BUFFER)
        try:
            msg = json.loads(raw)
        except (ValueError, UnicodeError):
            continue
        if not isinstance(msg, dict):
            continue
        with clients_lock:
            entry = clients.get(addr)
            if entry:
                entry[1] = time.monotonic()
        if entry is None:
            if msg.get('type') != 'join':
                continue
            with clients_lock:
                pid = pid_next
                pid_next += 1
                clients[addr] = [pid, time.monotonic()]
            game.join(pid)
            print(f'Player {pid} joined from {addr}', flush=True)
            send(addr, {'type': 'welcome', 'id': pid, 'x': WIDTH/2, 'y': HEIGHT/2})
            broadcast_world()
            continue
        pid = entry[0]
        if msg.get('type') == 'leave':
            with clients_lock:
                clients.pop(addr, None)
            game.leave(pid)
            broadcast_world()
            print(f'Player {pid} left', flush=True)
            continue
        if msg.get('type') == 'join':
            send(addr, {'type': 'welcome', 'id': pid, 'x': WIDTH/2, 'y': HEIGHT/2})
            broadcast_world()
            continue
        update, changed = game.input(pid, msg)
        if update:
            with clients_lock:
                peers = list(clients)
            for peer in peers:
                relay.submit(lambda peer=peer, update=update: send(peer, update), udp=True)
            if changed:
                broadcast_world()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--protocol', choices=('tcp', 'udp'), required=True)
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=5000)
    ap.add_argument('--delay-ms', type=float, default=0)
    ap.add_argument('--loss-pct', type=float, default=0)
    ap.add_argument('--round-seconds', type=float, default=60)
    ap.add_argument('--intermission-seconds', type=float, default=8)
    ap.add_argument('--min-players', type=int, default=2)
    args = ap.parse_args()
    if args.delay_ms < 0 or not 0 <= args.loss_pct <= 100:
        ap.error('delay-ms must be >= 0 and loss-pct between 0 and 100')
    if args.protocol == 'tcp' and args.loss_pct:
        ap.error('loss-pct applies only to UDP application movement updates')
    if args.round_seconds <= 0 or args.intermission_seconds <= 0 or not 1 <= args.min_players <= 20:
        ap.error('round/intermission seconds must be positive; min-players must be 1-20')
    relay = DelayedRelay(args.delay_ms, args.loss_pct)
    (serve_tcp if args.protocol == 'tcp' else serve_udp)(
        args.host, args.port, relay, args.round_seconds, args.intermission_seconds, args.min_players)
