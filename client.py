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


def encode_sized(msg, target_bytes):
    """Make a JSON payload of exactly target_bytes (or natural size if zero)."""
    raw = json.dumps(msg, separators=(',', ':'), ensure_ascii=True).encode('ascii')
    if not target_bytes:
        return raw
    if len(raw) > target_bytes:
        raise ValueError(f'Input message ({len(raw)} bytes) exceeds --payload-bytes {target_bytes}')
    if len(raw) == target_bytes:
        return raw
    overhead = len(b',"p":""')
    if target_bytes - len(raw) < overhead:
        raise ValueError(f'Cannot pad message from {len(raw)} to {target_bytes} bytes')
    return raw[:-1] + b',"p":"' + b'x' * (target_bytes - len(raw) - overhead) + b'"}'



def move(x, y, dx, dy, dt):
    magnitude = max(1.0, math.hypot(dx, dy))
    return (min(WIDTH - 15, max(15, x + SPEED * dx / magnitude * dt)),
            min(HEIGHT - 15, max(15, y + SPEED * dy / magnitude * dt)))


class Arena:
    def __init__(self, root, protocol, host, port, update_hz=30, log_file=None,
                 payload_bytes=0, burst_count=0, burst_interval=1.0):
        self.root, self.protocol = root, protocol
        self.payload_bytes = payload_bytes
        self.burst_count = burst_count
        self.burst_interval = burst_interval
        self.next_burst = time.monotonic() + burst_interval
        self.burst_seq = 0
        self.bytes_sent_window = deque()  # (monotonic time, socket bytes)
        self.bytes_recv_window = deque()
        self.send_interval = 1.0 / update_hz
        self.next_send = time.monotonic() + self.send_interval
        self.last_frame = time.monotonic()
        self.last_network_step = self.last_frame
        self.send_times, self.recv_times = deque(), deque()
        self.age_samples = deque(maxlen=500)
        self.missing_updates = 0
        self.correction_samples = deque(maxlen=500)
        self.correction_count = 0
        self.correction_threshold_px = 1.0
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
                                      'x', 'y', 'payload_bytes', 'seq_gap',
                                      'correction_px', 'corrected', 'wire_bytes'])
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

    def log(self, event, player, peer, seq, sent_at, age, x, y, nbytes, gap=0,
            correction_px="", corrected="", wire_bytes=""):
        if self.log_writer:
            self.log_writer.writerow([event, self.protocol, player, peer, seq,
                                      time.time(), sent_at, age, x, y, nbytes, gap,
                                      correction_px, corrected, wire_bytes])

    def send(self, msg):
        try:
            # Padding is only applied to movement inputs and synthetic background traffic.
            sized = msg.get('type') in ('input', 'traffic')
            data = encode_sized(msg, self.payload_bytes if sized else 0)
            wire = data + (b'\n' if self.protocol == 'tcp' else b'')
            if self.protocol == 'tcp':
                self.sock.sendall(wire)
            else:
                self.sock.send(data)
            if sized:
                now = time.monotonic()
                self.bytes_sent_window.append((now, len(wire)))
                if msg['type'] == 'input':
                    self.send_times.append(now)
                    self.log('send', self.player_id, '', msg['seq'], msg['sent_at'], '',
                             self.x, self.y, len(data), wire_bytes=len(wire))
                else:
                    self.log('traffic_send', self.player_id, '', msg['seq'], '', '',
                             '', '', len(data), wire_bytes=len(wire))
        except (OSError, ValueError) as exc:
            self.status = f'Sending failed: {exc}'
            self.running = False

    def send_burst(self):
        # These messages add upstream load, but have NO effect on the gameplay state.
        # The Phase 7 server ignores messages of type traffic.
        for _ in range(self.burst_count):
            self.burst_seq += 1
            self.send({'type': 'traffic', 'seq': self.burst_seq})
            if not self.running:
                break

    def receiver(self):
        try:
            if self.protocol == 'tcp':
                for line in self.sock.makefile('r', encoding='utf-8'):
                    if not self.running:
                        break
                    try:
                        self.incoming.put((json.loads(line), len(line.encode('utf-8'))))
                    except ValueError:
                        pass
            else:
                while self.running:
                    try:
                        raw = self.sock.recv(8192)
                        self.incoming.put((json.loads(raw), len(raw)))
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
            if isinstance(msg, tuple):
                msg, received_bytes = msg
                self.bytes_recv_window.append((time.monotonic(), received_bytes))
            else:
                received_bytes = 0
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
                    # Correction distance compares the current predicted location
                    # with the reconciled prediction after replaying unacknowledged inputs.
                    # This is not the error against the server's location at the same time.
                    correction_px = math.hypot(px - self.x, py - self.y)
                    corrected = int(correction_px >= self.correction_threshold_px)
                    self.correction_samples.append(correction_px)
                    self.correction_count += corrected
                    self.log('correction', self.player_id, pid, seq,
                             msg.get('sent_at'), '', px, py,
                             len(json.dumps(msg).encode()),
                             correction_px=round(correction_px, 4), corrected=corrected)
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
                             x, y, len(json.dumps(msg).encode()), gap,
                             wire_bytes=received_bytes)

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
            if self.burst_count and now >= self.next_burst:
                self.send_burst()
                self.next_burst += (int(max(0, now-self.next_burst)/self.burst_interval)+1)*self.burst_interval
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

    @staticmethod
    def bytes_per_second(samples, now):
        while samples and samples[0][0] < now - RATE_WINDOW:
            samples.popleft()
        return sum(item[1] for item in samples) / RATE_WINDOW

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
        mean_correction = statistics.mean(self.correction_samples) if self.correction_samples else 0.0
        self.info.config(text=f'{self.status} | Age avg/p95: {avg:.0f}/{p95:.0f} ms | '
                         f'Correction avg: {mean_correction:.1f}px ({self.correction_count} ≥1px) | '
                         f'Send/recv: {self.recent_rate(self.send_times,now):.1f}/'
                         f'{self.recent_rate(self.recv_times,now):.1f} Hz | '
                         f'Tx/Rx: {self.bytes_per_second(self.bytes_sent_window,now):.0f}/'
                         f'{self.bytes_per_second(self.bytes_recv_window,now):.0f} B/s | '
                         f'Gaps: {self.missing_updates}')

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
    parser.add_argument('--payload-bytes', type=int, default=0,
                        help='Exact JSON size for input and traffic messages; 0=natural size')
    parser.add_argument('--burst-count', type=int, default=0,
                        help='Extra upstream traffic messages per burst, not gameplay inputs')
    parser.add_argument('--burst-interval', type=float, default=1.0,
                        help='Seconds between synthetic traffic bursts')
    args = parser.parse_args()
    if not 0 < args.update_hz <= 60:
        parser.error('update-hz must be in (0,60]')
    if args.payload_bytes != 0 and not 128 <= args.payload_bytes <= 1200:
        parser.error('--payload-bytes must be 0 or between 128 and 1200 (UDP-safe test range)')
    if not 0 <= args.burst_count <= 100 or args.burst_interval <= 0:
        parser.error('--burst-count must be 0-100 and --burst-interval must be positive')
    root = tk.Tk()
    Arena(root, args.protocol, args.host, args.port, args.update_hz, args.log_file,
          args.payload_bytes, args.burst_count, args.burst_interval)
    root.mainloop()
