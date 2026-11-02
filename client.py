"""Coin Rush Tkinter client: locally predicted, server-authoritative multiplayer.
"""
import argparse
import csv
import json
import math
import queue
import socket
import statistics
import threading
import time
import tkinter as tk
from collections import deque
from pathlib import Path

WIDTH, HEIGHT = 800, 500
SPEED = 150.0
RENDER_MS = 16
RATE_WINDOW = 5.0


def move(x, y, dx, dy, dt):
    magnitude = max(1.0, math.hypot(dx, dy))
    return (min(WIDTH - 15, max(15, x + SPEED * dx / magnitude * dt)),
            min(HEIGHT - 15, max(15, y + SPEED * dy / magnitude * dt)))


class Arena:
    def __init__(self, root, protocol, host, port, update_hz=30, log_file=None):
        self.root, self.protocol = root, protocol
        self.send_interval = 1.0 / update_hz
        self.next_send = time.monotonic() + self.send_interval
        self.last_frame = time.monotonic()
        self.last_network_step = self.last_frame
        self.send_times, self.recv_times = deque(), deque()
        self.age_samples = deque(maxlen=500)
        self.missing_updates = 0
        self.incoming = queue.Queue()
        self.keys = set()
        self.other = {}  # id -> x, y, seq
        self.last_own_ack = -1
        self.pending = deque()  # (seq, dx, dy, client_dt)
        self.x, self.y = WIDTH / 2, HEIGHT / 2
        self.seq, self.player_id = 0, None
        self.coin = None
        self.scores = {}
        self.world_version = -1
        self.phase = 'waiting'
        self.remaining = 0.0
        self.world_received_at = time.monotonic()
        self.winners = []
        self.round_number = 0
        self.player_count = 0
        self.min_players = 2
        self.running = True
        self.status = 'Connecting...'
        self.last_join = time.monotonic()
        self.log_handle, self.log_writer = None, None
        if log_file:
            p = Path(log_file)
            p.parent.mkdir(parents=True, exist_ok=True)
            self.log_handle = p.open('w', newline='', encoding='utf-8')
            self.log_writer = csv.writer(self.log_handle)
            self.log_writer.writerow(['event', 'protocol', 'player_id', 'peer_id', 'seq',
                                      'timestamp', 'sent_at', 'update_age_ms',
                                      'x', 'y', 'payload_bytes', 'seq_gap'])
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM)
        self.sock.settimeout(3)
        try:
            self.sock.connect((host, port))
        except OSError:
            self.sock.close()
            if self.log_handle:
                self.log_handle.close()
            raise
        if protocol == 'tcp':
            self.sock.settimeout(None)
        root.title('CS6043 Coin Rush - ' + protocol.upper())
        root.resizable(False, False)
        self.canvas = tk.Canvas(root, width=WIDTH, height=HEIGHT, bg='#142437', highlightthickness=0)
        self.canvas.pack()
        self.info = tk.Label(root, text='Connecting...', anchor='w')
        self.info.pack(fill='x')
        root.bind_all('<KeyPress>', self.key_down)
        root.bind_all('<KeyRelease>', self.key_up)
        root.bind('<FocusOut>', lambda _: self.keys.clear())
        self.canvas.config(takefocus=1)
        self.canvas.focus_set()
        root.after(200, self.canvas.focus_set)
        root.protocol('WM_DELETE_WINDOW', self.close)
        threading.Thread(target=self.receiver, daemon=True).start()
        if protocol == 'udp':
            self.send({'type': 'join'})
        self.tick()

    def key_down(self, event):
        self.keys.add(event.keysym.lower())

    def key_up(self, event):
        self.keys.discard(event.keysym.lower())

    def directions(self):
        dx = int('d' in self.keys or 'right' in self.keys) - int('a' in self.keys or 'left' in self.keys)
        dy = int('s' in self.keys or 'down' in self.keys) - int('w' in self.keys or 'up' in self.keys)
        return dx, dy

    def log(self, event, player, peer, seq, sent_at, age, x, y, nbytes, gap=0):
        if self.log_writer:
            self.log_writer.writerow([event, self.protocol, player, peer, seq,
                                      time.time(), sent_at, age, x, y, nbytes, gap])

    def send(self, msg):
        try:
            data = json.dumps(msg).encode()
            if self.protocol == 'tcp':
                self.sock.sendall(data + b'\n')
            else:
                self.sock.send(data)
            if msg.get('type') == 'input':
                self.send_times.append(time.monotonic())
                self.log('send', self.player_id, '', msg['seq'], msg['sent_at'], '',
                         self.x, self.y, len(data))
        except OSError:
            self.status = 'Connection lost'
            self.running = False

    def receiver(self):
        try:
            if self.protocol == 'tcp':
                for line in self.sock.makefile('r', encoding='utf-8'):
                    if not self.running:
                        break
                    try:
                        self.incoming.put(json.loads(line))
                    except ValueError:
                        pass
            else:
                while self.running:
                    try:
                        self.incoming.put(json.loads(self.sock.recv(8192)))
                    except (socket.timeout, ValueError):
                        continue
        except (OSError, UnicodeError):
            pass
        if self.running:
            self.running = False
            self.status = 'Server disconnected'

    def process_messages(self):
        while True:
            try:
                msg = self.incoming.get_nowait()
            except queue.Empty:
                return
            if not isinstance(msg, dict):
                continue
            kind = msg.get('type')
            if kind == 'welcome':
                self.player_id = msg['id']
                if self.last_own_ack == -1:
                    self.x, self.y = msg.get('x', WIDTH/2), msg.get('y', HEIGHT/2)
                self.status = f'Player {self.player_id} via {self.protocol.upper()}'
            elif kind == 'world':
                if type(msg.get('version')) is int and msg['version'] >= self.world_version:
                    self.world_version = msg['version']
                    self.coin = msg.get('coin')
                    self.scores = msg.get('scores', {})
                    self.phase = msg.get('phase', 'waiting')
                    self.remaining = max(0.0, float(msg.get('remaining', 0)))
                    self.world_received_at = time.monotonic()
                    self.winners = msg.get('winners', [])
                    self.round_number = msg.get('round', 0)
                    self.player_count = msg.get('player_count', len(self.scores))
                    self.min_players = msg.get('min_players', 2)
                    self.other = {p: v for p, v in self.other.items() if str(p) in self.scores}
            elif kind == 'move':
                pid, seq = msg.get('id'), msg.get('seq')
                x, y = msg.get('x'), msg.get('y')
                if type(pid) is not int or type(seq) is not int or not all(type(n) in (float, int) for n in (x, y)):
                    continue
                if pid == self.player_id:
                    if seq <= self.last_own_ack:
                        continue
                    self.last_own_ack = seq
                    while self.pending and self.pending[0][0] <= seq:
                        self.pending.popleft()
                    # Rebuild predicted location from server's acknowledged position.
                    px, py = float(x), float(y)
                    for _, dx, dy, dt in self.pending:
                        px, py = move(px, py, dx, dy, dt)
                    self.x, self.y = px, py
                else:
                    old = self.other.get(pid)
                    if old and seq <= old[2]:
                        continue
                    gap = max(0, seq-old[2]-1) if old else 0
                    self.missing_updates += gap
                    self.other[pid] = (x, y, seq)
                    sent_at = msg.get('sent_at')
                    age = None
                    if type(sent_at) in (int, float):
                        test_age = (time.time() - sent_at) * 1000
                        if 0 <= test_age < 60000:
                            age = test_age
                            self.age_samples.append(age)
                    self.recv_times.append(time.monotonic())
                    self.log('receive', self.player_id, pid, seq, sent_at, age,
                             x, y, len(json.dumps(msg).encode()), gap)

    def tick(self):
        self.process_messages()
        now = time.monotonic()
        dt = max(0, min(now - self.last_frame, 0.1))
        self.last_frame = now
        if self.running and self.player_id is not None:
            dx, dy = self.directions()
            self.x, self.y = move(self.x, self.y, dx, dy, dt)
            if now >= self.next_send:
                elapsed = max(0, min(now - self.last_network_step, 0.1))
                self.last_network_step = now
                self.seq += 1
                self.pending.append((self.seq, dx, dy, elapsed))
                # Prevent unlimited memory growth if acknowledgements are lost.
                while len(self.pending) > 120:
                    self.pending.popleft()
                self.send({'type': 'input', 'seq': self.seq, 'dx': dx, 'dy': dy,
                           'sent_at': time.time()})
                self.next_send += (int(max(0, now-self.next_send)/self.send_interval)+1)*self.send_interval
        elif self.running and self.protocol == 'udp' and now - self.last_join > 1:
            self.send({'type': 'join'})  # retry if initial join/welcome lost
            self.last_join = now
        self.draw(now)
        if self.running:
            self.root.after(RENDER_MS, self.tick)
        elif self.log_handle:
            self.log_handle.flush()

    @staticmethod
    def recent_rate(times, now):
        while times and times[0] < now - RATE_WINDOW:
            times.popleft()
        return len(times) / RATE_WINDOW

    def draw(self, now):
        self.canvas.delete('all')
        for x in range(0, WIDTH, 50):
            self.canvas.create_line(x, 0, x, HEIGHT, fill='#26394d')
        for y in range(0, HEIGHT, 50):
            self.canvas.create_line(0, y, WIDTH, y, fill='#26394d')
        if isinstance(self.coin, list) and len(self.coin) == 2:
            cx, cy = self.coin
            self.canvas.create_oval(cx-11, cy-11, cx+11, cy+11, fill='#ffdf55', outline='white', width=2)
        self.dot(self.x, self.y, '#44adff', f'You (P{self.player_id or "?"})')
        for pid, (x, y, _) in self.other.items():
            self.dot(x, y, '#ffbd57', f'Player {pid}')
        scoreboard = '   '.join(f'P{k}: {v}' for k, v in sorted(self.scores.items(), key=lambda item: int(item[0])))
        self.canvas.create_text(10, 12, anchor='nw', text=f'Scores: {scoreboard}', fill='white', font=('Arial', 13, 'bold'))
        remaining = max(0, self.remaining - (now - self.world_received_at))
        if self.phase == 'playing':
            phase_text = f'Round {self.round_number}: {remaining:.0f}s remaining'
        elif self.phase == 'finished':
            winner_names = ', '.join(f'P{pid}' for pid in self.winners)
            phase_text = f'Round over! Winner: {winner_names or "None"} | Next round in {remaining:.0f}s'
        else:
            phase_text = f'Waiting for players ({self.player_count}/{self.min_players})'
        self.canvas.create_text(WIDTH/2, 22, text=phase_text, fill='#ffdf55',
                                font=('Arial', 15, 'bold'))
        ages = list(self.age_samples)
        avg = statistics.mean(ages) if ages else 0
        p95 = sorted(ages)[int(.95*(len(ages)-1))] if ages else 0
        self.info.config(text=f'{self.status} | Age avg/p95: {avg:.0f}/{p95:.0f} ms | '
                         f'Send/recv: {self.recent_rate(self.send_times,now):.1f}/'
                         f'{self.recent_rate(self.recv_times,now):.1f} Hz | Gaps: {self.missing_updates}')

    def dot(self, x, y, color, name):
        self.canvas.create_oval(x-13, y-13, x+13, y+13, fill=color, outline='white', width=2)
        self.canvas.create_text(x, y-24, text=name, fill='white', font=('Arial', 11))

    def close(self):
        self.running = False
        if self.protocol == 'udp':
            # Best effort: UDP leave can be lost; server also expires stale clients.
            try:
                self.sock.send(json.dumps({'type': 'leave'}).encode())
            except OSError:
                pass
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        if self.log_handle:
            self.log_handle.flush()
            self.log_handle.close()
        self.root.destroy()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--protocol', choices=['tcp', 'udp'], required=True)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--update-hz', type=float, default=30)
    parser.add_argument('--log-file', default=None)
    args = parser.parse_args()
    if not 0 < args.update_hz <= 60:
        parser.error('update-hz must be in (0,60]')
    root = tk.Tk()
    Arena(root, args.protocol, args.host, args.port, args.update_hz, args.log_file)
    root.mainloop()
