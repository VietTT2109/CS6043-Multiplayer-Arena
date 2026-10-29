
"""CS6043 server: TCP/UDP arena with shared coin game and relay impairments.

The server owns coin positions/scores. Movement remains client-predicted and
client-reported in this milestone; authoritative movement is a later upgrade.
"""
import argparse
import heapq
import json
import random
import socket
import threading
import time

BUFFER = 4096
WIDTH, HEIGHT = 800, 500
RADIUS = 13
COIN_RADIUS = 11


def valid_update(msg):
    return (isinstance(msg, dict) and msg.get('type') == 'move'
            and all(isinstance(msg.get(k), (int, float)) for k in ('x', 'y', 'seq'))
            and isinstance(msg.get('seq'), int) and not isinstance(msg['seq'], bool)
            and 0 <= msg['x'] <= WIDTH and 0 <= msg['y'] <= HEIGHT)


class GameState:
    def __init__(self):
        self.lock = threading.Lock()
        self.scores = {}
        self.positions = {}
        self.coin = self.new_coin()
        self.version = 0

    @staticmethod
    def new_coin():
        return [random.randint(45, WIDTH - 45), random.randint(45, HEIGHT - 45)]

    def join(self, player_id):
        with self.lock:
            self.scores[player_id] = 0
            return self.snapshot_locked()

    def leave(self, player_id):
        with self.lock:
            self.scores.pop(player_id, None)
            self.positions.pop(player_id, None)
            self.version += 1
            return self.snapshot_locked()

    def move(self, player_id, x, y):
        with self.lock:
            self.positions[player_id] = (x, y)
            if ((x - self.coin[0]) ** 2 + (y - self.coin[1]) ** 2
                    > (RADIUS + COIN_RADIUS) ** 2):
                return None
            self.scores[player_id] += 1
            # Choose a different coin location so it cannot be collected twice
            old = self.coin
            for _ in range(20):
                self.coin = self.new_coin()
                if (self.coin[0] - old[0]) ** 2 + (self.coin[1] - old[1]) ** 2 > 120 ** 2:
                    break
            self.version += 1
            return self.snapshot_locked()

    def snapshot_locked(self):
        return {'type': 'world', 'coin': list(self.coin),
                'scores': {str(k): v for k, v in self.scores.items()},
                'version': self.version}

    def snapshot(self):
        with self.lock:
            return self.snapshot_locked()


class DelayedRelay:
    """Applies added delay and optional UDP application-message drop to movement only."""
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
                remaining = self.jobs[0][0] - time.monotonic()
                if remaining > 0:
                    self.cv.wait(remaining)
                    continue
                _, _, deliver = heapq.heappop(self.jobs)
            try:
                deliver()
            except OSError:
                pass


def serve_udp(host, port, relay):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    players = {}   # addr -> [id, last_seen]
    game = GameState()
    next_id = 1
    print(f'UDP game server listening on {host}:{port}', flush=True)

    def send_world():
        wire = json.dumps(game.snapshot()).encode()
        for peer in list(players):
            try:
                sock.sendto(wire, peer)
            except OSError:
                pass

    def heartbeat():
        while True:
            time.sleep(0.5)
            # Refresh snapshots and prune abandoned UDP peers.
            expired = [peer for peer, (_, seen) in list(players.items())
                       if time.monotonic() - seen > 15]
            for peer in expired:
                player_id, _ = players.pop(peer, (None, 0))
                if player_id is not None:
                    game.leave(player_id)
            send_world()

    threading.Thread(target=heartbeat, daemon=True).start()
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
            players[addr] = [next_id, time.monotonic()]
            game.join(next_id)
            print(f'Player {next_id} joined from {addr}', flush=True)
            next_id += 1
            # Client retries join if this welcome is lost on the network.
            sock.sendto(json.dumps({'type': 'welcome', 'id': players[addr][0]}).encode(), addr)
            send_world()
            continue
        player_id = players[addr][0]
        players[addr][1] = time.monotonic()
        if msg.get('type') == 'join':
            sock.sendto(json.dumps({'type': 'welcome', 'id': player_id}).encode(), addr)
            send_world()
        elif valid_update(msg):
            update = {'type': 'move', 'id': player_id, 'x': msg['x'],
                      'y': msg['y'], 'seq': msg['seq'], 'sent_at': msg.get('sent_at')}
            wire = json.dumps(update).encode()
            for peer in list(players):
                if peer != addr:
                    relay.submit(lambda peer=peer, wire=wire: sock.sendto(wire, peer), udp=True)
            if game.move(player_id, msg['x'], msg['y']) is not None:
                send_world()


def serve_tcp(host, port, relay):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen()
    clients = {}  # conn -> [id, per-connection send lock]
    clients_lock = threading.Lock()
    game = GameState()
    next_id = 1
    print(f'TCP game server listening on {host}:{port}', flush=True)

    def send_msg(conn, msg):
        with clients_lock:
            info = clients.get(conn)
        if info is None:
            return
        with info[1]:
            conn.sendall((json.dumps(msg) + '\n').encode())

    def broadcast_world():
        world = game.snapshot()
        with clients_lock:
            peers = list(clients)
        for peer in peers:
            try:
                send_msg(peer, world)
            except OSError:
                pass

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
                              'y': msg['y'], 'seq': msg['seq'],
                              'sent_at': msg.get('sent_at')}
                    with clients_lock:
                        recipients = [c for c in clients if c is not conn]
                    for peer in recipients:
                        relay.submit(lambda peer=peer, update=update: send_msg(peer, update))
                    if game.move(player_id, msg['x'], msg['y']) is not None:
                        broadcast_world()
        except (OSError, UnicodeError):
            pass
        finally:
            with clients_lock:
                clients.pop(conn, None)
            game.leave(player_id)
            broadcast_world()
            print(f'Player {player_id} disconnected', flush=True)

    while True:
        conn, addr = listener.accept()
        with clients_lock:
            player_id = next_id
            next_id += 1
            clients[conn] = (player_id, threading.Lock())
        game.join(player_id)
        print(f'Player {player_id} joined from {addr}', flush=True)
        try:
            send_msg(conn, {'type': 'welcome', 'id': player_id})
            broadcast_world()
        except OSError:
            with clients_lock:
                clients.pop(conn, None)
            game.leave(player_id)
            conn.close()
            continue
        threading.Thread(target=handle, args=(conn, player_id), daemon=True).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--protocol', choices=['tcp', 'udp'], required=True)
    parser.add_argument('--host', default='0.0.0.0',
                        help='IPv4 interface to bind (default: all interfaces)')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--delay-ms', type=float, default=0,
                        help='Added server-to-client movement relay delay (ms)')
    parser.add_argument('--loss-pct', type=float, default=0,
                        help='Chance to drop relayed UDP movement message (0-100)')
    args = parser.parse_args()
    if args.delay_ms < 0 or not 0 <= args.loss_pct <= 100:
        parser.error('delay-ms must be nonnegative; loss-pct must be 0-100')
    if args.protocol == 'tcp' and args.loss_pct:
        parser.error('--loss-pct only applies to UDP application movement messages')
    print(f'Configured: {args.delay_ms:g} ms relay delay, {args.loss_pct:g}% UDP movement loss')
    relay = DelayedRelay(args.delay_ms, args.loss_pct)
    (serve_tcp if args.protocol == 'tcp' else serve_udp)(args.host, args.port, relay)
