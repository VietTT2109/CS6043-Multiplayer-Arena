"""Coin Rush: server-authoritative TCP/UDP movement, timed rounds, optional relay impairment.

Movement updates are direction inputs; the server computes and validates positions.
--loss-pct drops outgoing UDP application updates, NOT IP packets.
"""
import argparse
import csv
import secrets
from pathlib import Path
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
    """Educational application-layer bottleneck for movement relays only.

    All relayed movements share one FIFO rate limiter. Background load consumes
    *virtual* capacity, not actual network packets. This is not kernel-level
    congestion, TCP congestion control, or a multi-hop network emulator.
    """
    def __init__(self, delay_ms, loss_pct, bandwidth_kbps=0, background_kbps=0,
                 queue_kbytes=64, log_file=None):
        self.delay = delay_ms / 1000
        self.loss = loss_pct / 100
        self.bytes_per_second = bandwidth_kbps * 1000 / 8 if bandwidth_kbps else 0
        self.background_bytes_per_second = background_kbps * 1000 / 8
        self.queue_limit = int(queue_kbytes * 1024)
        self.cv = threading.Condition()
        self.jobs = []
        self.count = 0
        self.queued_bytes = 0
        self.next_free = time.monotonic()
        self.log_handle = None
        self.log_writer = None
        if log_file:
            path = Path(log_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.log_handle = path.open('w', newline='', encoding='utf-8')
            self.log_writer = csv.writer(self.log_handle)
            self.log_writer.writerow(['timestamp', 'event', 'protocol', 'bytes',
                                      'queue_bytes', 'wait_ms'])
        threading.Thread(target=self.run, daemon=True).start()
        if self.background_bytes_per_second:
            threading.Thread(target=self.background_worker, daemon=True).start()

    def log(self, event, protocol, size, wait_ms=0):
        if self.log_writer:
            self.log_writer.writerow([time.time(), event, protocol, size,
                                      self.queued_bytes, round(wait_ms, 2)])
            self.log_handle.flush()

    def submit(self, deliver, udp=False, size=128):
        size = max(1, int(size))
        protocol = 'udp' if udp else 'tcp'
        if udp and random.random() < self.loss:
            with self.cv:
                self.log('configured_loss', protocol, size)
            return
        with self.cv:
            now = time.monotonic()
            if self.bytes_per_second and self.queued_bytes + size > self.queue_limit and udp:
                self.log('queue_drop', protocol, size)
                return
            # A virtual background packet is simply a no-op that occupies
            # space in the same serialization queue.
            self.count += 1
            start = max(self.next_free, now + self.delay) if self.bytes_per_second else now + self.delay
            finish = start + size / self.bytes_per_second if self.bytes_per_second else start
            if self.bytes_per_second:
                self.next_free = finish
            self.queued_bytes += size
            heapq.heappush(self.jobs, (finish, self.count, deliver, size, protocol, now))
            self.log('enqueue', protocol, size, (finish - now) * 1000)
            self.cv.notify()

    def background_worker(self):
        # A packet every 50 ms approximates a steady competing application.
        interval = .05
        while True:
            size = max(1, round(self.background_bytes_per_second * interval))
            with self.cv:
                # Do not let fake traffic grow forever or starve shutdown.
                too_full = self.queued_bytes + size > self.queue_limit
            if not too_full:
                self.submit_background(size)
            time.sleep(interval)

    def submit_background(self, size):
        with self.cv:
            now = time.monotonic()
            self.count += 1
            start = max(self.next_free, now + self.delay)
            finish = start + size / self.bytes_per_second
            self.next_free = finish
            self.queued_bytes += size
            heapq.heappush(self.jobs, (finish, self.count, None, size, 'background', now))
            self.log('enqueue', 'background', size, (finish - now) * 1000)
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
                _, _, deliver, size, protocol, submitted = heapq.heappop(self.jobs)
                self.queued_bytes = max(0, self.queued_bytes - size)
                self.log('deliver' if deliver else 'background_used', protocol,
                         size, (time.monotonic() - submitted) * 1000)
            if deliver:
                try:
                    deliver()
                except OSError:
                    pass


class ReliableEvents:
    """Retransmit changed UDP world snapshots until ACK, with bounded retries."""
    def __init__(self, send, log_file=None, timeout=0.35, max_retries=8):
        self.send = send
        self.timeout = timeout
        self.max_retries = max_retries
        self.pending = {}  # (address, version) -> [message, last_sent, retries, started]
        self.lock = threading.RLock()
        self.log_handle = None
        self.writer = None
        if log_file:
            path = Path(log_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.log_handle = path.open('w', newline='', encoding='utf-8')
            self.writer = csv.writer(self.log_handle)
            self.writer.writerow(['timestamp', 'event', 'peer', 'version', 'retries', 'ack_ms'])
        threading.Thread(target=self.worker, daemon=True).start()

    def log(self, kind, peer, version, retries=0, ack_ms=''):
        if self.writer:
            self.writer.writerow([time.time(), kind, repr(peer), version, retries, ack_ms])
            self.log_handle.flush()

    def publish(self, peers, world):
        version = world['version']
        now = time.monotonic()
        msg = {'type': 'reliable_world', 'event_id': version, 'world': world}
        with self.lock:
            for peer in peers:
                # A newer snapshot supersedes older snapshots for this peer.
                for key in list(self.pending):
                    if key[0] == peer:
                        self.pending.pop(key, None)
                self.pending[(peer, version)] = [msg, now, 0, now]
                try:
                    self.send(peer, msg)
                    self.log('initial', peer, version)
                except OSError:
                    pass

    def ack(self, peer, version):
        if type(version) is not int:
            return
        with self.lock:
            item = self.pending.pop((peer, version), None)
            if item:
                self.log('ack', peer, version, item[2], round((time.monotonic()-item[3])*1000, 2))

    def forget(self, peer):
        with self.lock:
            for key in list(self.pending):
                if key[0] == peer:
                    self.pending.pop(key, None)

    def worker(self):
        while True:
            time.sleep(0.05)
            now = time.monotonic()
            with self.lock:
                for (peer, version), item in list(self.pending.items()):
                    msg, last, retries, start = item
                    if now - last < self.timeout:
                        continue
                    if retries >= self.max_retries:
                        self.pending.pop((peer, version), None)
                        self.log('give_up', peer, version, retries)
                        continue
                    try:
                        self.send(peer, msg)
                        item[1] = now
                        item[2] += 1
                        self.log('retry', peer, version, item[2])
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
                            relay.submit(lambda c=c, update=update: send(c, update), size=len(json.dumps(update).encode()) + 1)
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


def serve_udp(host, port, relay, round_seconds, intermission_seconds, min_players, reliable_log_file=None, event_loss_pct=0):
    game = Game(round_seconds, intermission_seconds, min_players)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    clients = {}  # address -> [id, last_seen]
    clients_lock = threading.Lock()
    print(f'UDP server on {host}:{port}', flush=True)
    def send_reliable(addr, msg):
        # Application-level simulation of unreliable critical-event delivery.
        if random.random() * 100 < event_loss_pct:
            return
        send(addr, msg)

    reliable = ReliableEvents(send=send_reliable, log_file=reliable_log_file)

    def send(addr, message):
        sock.sendto(json.dumps(message).encode(), addr)

    last_published_version = [-1]

    def broadcast_world():
        with clients_lock:
            peers = list(clients)
        world = game.world()
        if world['version'] != last_published_version[0]:
            last_published_version[0] = world['version']
            reliable.publish(peers, world)
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
                    reliable.forget(addr)
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
        if msg.get('type') == 'event_ack':
            reliable.ack(addr, msg.get('event_id'))
            continue
        if msg.get('type') == 'leave':
            with clients_lock:
                clients.pop(addr, None)
                reliable.forget(addr)
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
                relay.submit(lambda peer=peer, update=update: send(peer, update), udp=True, size=len(json.dumps(update).encode()))
            if changed:
                broadcast_world()


def serve_hybrid(host, port, udp_port, relay, round_seconds, intermission_seconds, min_players):
    """TCP control plane, UDP movement plane; linked with random session tokens."""
    game = Game(round_seconds, intermission_seconds, min_players)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen()
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind((host, udp_port))
    connections = {}  # conn -> (pid, token, send_lock)
    endpoints = {}   # pid -> UDP addr
    tokens = {}      # token -> pid
    lock = threading.RLock()
    print(f'HYBRID server: TCP {host}:{port}, UDP {host}:{udp_port}', flush=True)

    def send_tcp(conn, msg):
        with lock:
            info = connections.get(conn)
        if not info:
            return
        with info[2]:
            conn.sendall((json.dumps(msg) + '\n').encode())

    def broadcast_world():
        world = game.world()
        with lock:
            peers = list(connections)
        for conn in peers:
            try:
                send_tcp(conn, world)
            except OSError:
                pass

    def heartbeat():
        while True:
            time.sleep(.25)
            game.advance()
            broadcast_world()

    def udp_worker():
        while True:
            raw, addr = udp.recvfrom(BUFFER)
            try:
                msg = json.loads(raw)
            except (ValueError, UnicodeError):
                continue
            if not isinstance(msg, dict):
                continue
            token = msg.get('token')
            with lock:
                pid = tokens.get(token) if isinstance(token, str) else None
                old_addr = endpoints.get(pid) if pid is not None else None
            if pid is None:
                continue
            if msg.get('type') == 'udp_hello':
                with lock:
                    endpoints[pid] = addr
                udp.sendto(json.dumps({'type': 'udp_ready'}).encode(), addr)
                continue
            if addr != old_addr:
                continue
            update, changed = game.input(pid, msg)
            if update:
                wire = json.dumps(update).encode()
                with lock:
                    peers = list(endpoints.values())
                for peer in peers:
                    relay.submit(lambda peer=peer, wire=wire: udp.sendto(wire, peer), udp=True, size=len(wire))
                if changed:
                    broadcast_world()

    def handle(conn, pid, token):
        try:
            with conn:
                for line in conn.makefile('r', encoding='utf-8'):
                    # Control channel currently needs no client-originated commands.
                    try:
                        json.loads(line)
                    except ValueError:
                        continue
        except (OSError, UnicodeError):
            pass
        finally:
            with lock:
                connections.pop(conn, None)
                tokens.pop(token, None)
                endpoints.pop(pid, None)
            game.leave(pid)
            broadcast_world()
            print(f'Player {pid} left', flush=True)

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=udp_worker, daemon=True).start()
    next_pid = 1
    while True:
        conn, addr = listener.accept()
        token = secrets.token_hex(16)
        with lock:
            pid = next_pid
            next_pid += 1
            connections[conn] = (pid, token, threading.Lock())
            tokens[token] = pid
        game.join(pid)
        print(f'Player {pid} joined via TCP from {addr}', flush=True)
        try:
            send_tcp(conn, {'type': 'welcome', 'id': pid, 'x': WIDTH/2, 'y': HEIGHT/2,
                            'token': token, 'udp_port': udp_port})
            broadcast_world()
        except OSError:
            with lock:
                connections.pop(conn, None)
                tokens.pop(token, None)
            game.leave(pid)
            conn.close()
            continue
        threading.Thread(target=handle, args=(conn, pid, token), daemon=True).start()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--protocol', choices=('tcp', 'udp', 'hybrid'), required=True)
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=5000)
    ap.add_argument('--udp-port', type=int, default=None, help='Hybrid movement port (default: port + 1)')
    ap.add_argument('--reliable-log-file', default=None, help='UDP reliable-event ACK/retry CSV')
    ap.add_argument('--event-loss-pct', type=float, default=0, help='Simulated UDP reliable-event message loss (0-100)')
    ap.add_argument('--delay-ms', type=float, default=0)
    ap.add_argument('--loss-pct', type=float, default=0)
    ap.add_argument('--bandwidth-kbps', type=float, default=0, help='Simulated shared relay throughput (kilobits/s); 0=unlimited')
    ap.add_argument('--background-kbps', type=float, default=0, help='Virtual competing relay traffic in kilobits/s')
    ap.add_argument('--queue-kbytes', type=float, default=64, help='Simulated relay queue capacity in KB (UDP drops on overflow)')
    ap.add_argument('--congestion-log-file', default=None, help='CSV log of relay queue and wait times')
    ap.add_argument('--round-seconds', type=float, default=60)
    ap.add_argument('--intermission-seconds', type=float, default=8)
    ap.add_argument('--min-players', type=int, default=2)
    args = ap.parse_args()
    if args.delay_ms < 0 or not 0 <= args.loss_pct <= 100:
        ap.error('delay-ms must be >= 0 and loss-pct between 0 and 100')
    if not 0 <= args.event_loss_pct <= 100:
        ap.error('event-loss-pct must be 0-100')
    if args.protocol != 'udp' and args.event_loss_pct:
        ap.error('event-loss-pct is only supported in UDP-only mode')
    if args.protocol == 'tcp' and args.loss_pct:
        ap.error('loss-pct applies only to UDP application movement updates')
    if args.round_seconds <= 0 or args.intermission_seconds <= 0 or not 1 <= args.min_players <= 20:
        ap.error('round/intermission seconds must be positive; min-players must be 1-20')
    if args.bandwidth_kbps < 0 or args.background_kbps < 0 or args.queue_kbytes <= 0:
        ap.error('bandwidth/background rates must be nonnegative; queue-kbytes must be positive')
    if args.background_kbps and not args.bandwidth_kbps:
        ap.error('--background-kbps requires --bandwidth-kbps')
    udp_port = args.udp_port or (args.port + 1)
    if not 1 <= args.port <= 65535 or not 1 <= udp_port <= 65535 or (args.protocol == 'hybrid' and udp_port == args.port):
        ap.error('invalid port configuration')
    relay = DelayedRelay(args.delay_ms, args.loss_pct, args.bandwidth_kbps,
                         args.background_kbps, args.queue_kbytes, args.congestion_log_file)
    if args.bandwidth_kbps:
        print(f'Application relay bottleneck: {args.bandwidth_kbps:g} kb/s, '
              f'background={args.background_kbps:g} kb/s, queue={args.queue_kbytes:g} KB', flush=True)
    if args.protocol == 'hybrid':
        serve_hybrid(args.host, args.port, udp_port, relay, args.round_seconds,
                     args.intermission_seconds, args.min_players)
    elif args.protocol == 'tcp':
        serve_tcp(args.host, args.port, relay, args.round_seconds, args.intermission_seconds, args.min_players)
    else:
        serve_udp(args.host, args.port, relay, args.round_seconds, args.intermission_seconds,
                  args.min_players, args.reliable_log_file, args.event_loss_pct)
