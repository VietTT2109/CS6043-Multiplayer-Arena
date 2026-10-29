"""Tkinter multiplayer arena: independent rendering/sending and CSV network logging."""
import argparse
import csv
import json
import queue
import socket
import statistics
import threading
import time
import tkinter as tk
from collections import deque
from pathlib import Path

WIDTH, HEIGHT = 800, 500
RENDER_MS = 16                       # ~60 FPS, independent of network sends
MOVEMENT_PIXELS_PER_SECOND = 150    # consistent speed across update rates
RATE_WINDOW = 5.0                   # recent-rate display window, seconds


class Arena:
    def __init__(self, root, protocol, host, port, update_hz=30, log_file=None):
        self.root = root
        self.protocol = protocol
        self.update_hz = update_hz
        self.send_interval = 1.0 / update_hz
        self.next_send = time.monotonic() + self.send_interval
        self.last_frame = time.monotonic()
        self.send_times = deque()
        self.recv_times = deque()
        self.age_samples = deque(maxlen=500)
        self.sent_count = 0
        self.recv_count = 0
        self.missing_updates = 0
        self.coin = None
        self.scores = {}
        self.world_version = -1
        self.last_join = time.monotonic()
        self.log_handle = None
        self.log_writer = None
        if log_file:
            path = Path(log_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.log_handle = path.open('w', newline='', encoding='utf-8')
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
        self.incoming = queue.Queue()
        self.keys = set()
        self.other = {}  # id -> (x, y, seq)
        self.x, self.y = WIDTH / 2, HEIGHT / 2
        self.seq = 0
        self.player_id = None
        self.running = True
        self.status = 'Connecting...'

        root.title(f'CS6043 Arena - {protocol.upper()}')
        root.resizable(False, False)
        self.canvas = tk.Canvas(root, width=WIDTH, height=HEIGHT, bg='#142437', highlightthickness=0)
        self.canvas.pack()
        self.info = tk.Label(root, text='Connecting...', anchor='w')
        self.info.pack(fill='x')
        root.bind_all('<KeyPress>', self.key_down)
        root.bind_all('<KeyRelease>', self.key_up)
        root.bind('<FocusOut>', lambda _event: self.keys.clear())
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

    def log(self, event, player_id, peer_id, seq, sent_at, age_ms, x, y, nbytes, gap):
        if self.log_writer:
            self.log_writer.writerow([event, self.protocol, player_id, peer_id, seq,
                                      time.time(), sent_at, age_ms, x, y, nbytes, gap])

    def send(self, msg):
        try:
            raw = json.dumps(msg).encode()
            if self.protocol == 'tcp':
                self.sock.sendall(raw + b'\n')
            else:
                self.sock.send(raw)
            if msg.get('type') == 'move':
                now = time.monotonic()
                self.send_times.append(now)
                self.sent_count += 1
                self.log('send', self.player_id, None, msg['seq'], msg['sent_at'],
                         None, msg['x'], msg['y'], len(raw), 0)
        except OSError:
            self.status = 'Network connection lost'
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
                        continue
            else:
                while self.running:
                    try:
                        raw = self.sock.recv(4096)
                        self.incoming.put(json.loads(raw))
                    except (socket.timeout, ValueError):
                        continue
        except (OSError, UnicodeError):
            pass
        if self.running:
            self.status = 'Network connection closed'
            self.running = False

    def process_messages(self):
        while True:
            try:
                msg = self.incoming.get_nowait()
            except queue.Empty:
                break
            if not isinstance(msg, dict):
                continue
            if msg.get('type') == 'welcome':
                self.player_id = msg['id']
                self.status = f'Player {self.player_id} connected via {self.protocol.upper()}'
            elif msg.get('type') == 'world':
                version = msg.get('version', -1)
                if isinstance(version, int) and version >= self.world_version:
                    coin = msg.get('coin')
                    scores = msg.get('scores')
                    if isinstance(coin, list) and len(coin) == 2 and isinstance(scores, dict):
                        self.coin = coin
                        self.scores = scores
                        self.world_version = version
            elif msg.get('type') == 'move':
                pid = msg.get('id')
                if pid == self.player_id or not isinstance(pid, int):
                    continue
                seq = msg.get('seq')
                previous = self.other.get(pid)
                if not isinstance(seq, int) or (previous is not None and seq <= previous[2]):
                    continue
                gap = max(0, seq - previous[2] - 1) if previous is not None else 0
                self.missing_updates += gap
                sent_at = msg.get('sent_at')
                age_ms = None
                if isinstance(sent_at, (float, int)):
                    candidate = (time.time() - sent_at) * 1000
                    if 0 <= candidate < 60000:
                        age_ms = candidate
                        self.age_samples.append(age_ms)
                x, y = msg.get('x'), msg.get('y')
                if not isinstance(x, (float, int)) or not isinstance(y, (float, int)):
                    continue
                self.other[pid] = (x, y, seq)
                self.recv_count += 1
                self.recv_times.append(time.monotonic())
                self.log('receive', self.player_id, pid, seq, sent_at, age_ms,
                         x, y, len(json.dumps(msg).encode()), gap)

    def tick(self):
        self.process_messages()
        now = time.monotonic()
        dt = min(now - self.last_frame, 0.1)
        self.last_frame = now
        if self.running and self.protocol == 'udp' and self.player_id is None and now - self.last_join > 1:
            self.send({'type': 'join'})
            self.last_join = now
        if self.running and self.player_id is not None:
            dx = int('d' in self.keys or 'right' in self.keys) - int('a' in self.keys or 'left' in self.keys)
            dy = int('s' in self.keys or 'down' in self.keys) - int('w' in self.keys or 'up' in self.keys)
            self.x = max(15, min(WIDTH - 15, self.x + dx * MOVEMENT_PIXELS_PER_SECOND * dt))
            self.y = max(15, min(HEIGHT - 15, self.y + dy * MOVEMENT_PIXELS_PER_SECOND * dt))
            if now >= self.next_send:
                self.seq += 1
                self.send({'type': 'move', 'x': round(self.x, 2), 'y': round(self.y, 2),
                           'seq': self.seq, 'sent_at': time.time()})
                # Maintain the requested cadence without a backlog of overdue sends.
                self.next_send += (int(max(0, now - self.next_send) / self.send_interval) + 1) * self.send_interval
        self.draw(now)
        if self.running:
            self.root.after(RENDER_MS, self.tick)
        else:
            self.info.config(text=self.status)
            if self.log_handle:
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
        if self.coin is not None:
            cx, cy = self.coin
            self.canvas.create_oval(cx-11, cy-11, cx+11, cy+11, fill='#f3d34a', outline='#fff3aa', width=2)
            self.canvas.create_text(cx, cy-20, text='COIN', fill='#fff3aa', font=('Arial', 10, 'bold'))
        self.dot(self.x, self.y, '#44adff', f'You (P{self.player_id or "?"})')
        for pid, (x, y, _) in self.other.items():
            self.dot(x, y, '#ffbd57', f'Player {pid}')
        score_line = '   '.join(f'P{pid}: {score}' for pid, score in sorted(self.scores.items(), key=lambda item: int(item[0])))
        self.canvas.create_text(WIDTH // 2, 18, text='COIN RUSH   |   ' + (score_line or 'Waiting for players'),
                                fill='white', font=('Arial', 14, 'bold'))
        age = list(self.age_samples)
        avg = statistics.mean(age) if age else 0
        p95 = sorted(age)[int(0.95 * (len(age) - 1))] if age else 0
        send_rate = self.recent_rate(self.send_times, now)
        recv_rate = self.recent_rate(self.recv_times, now)
        self.info.config(text=f'{self.status} | Age avg/p95: {avg:.0f}/{p95:.0f} ms | '
                              f'Send/recv: {send_rate:.1f}/{recv_rate:.1f} Hz | Gaps: {self.missing_updates}')

    def dot(self, x, y, color, name):
        self.canvas.create_oval(x-13, y-13, x+13, y+13, fill=color, outline='white', width=2)
        self.canvas.create_text(x, y-24, text=name, fill='white', font=('Arial', 11))

    def close(self):
        self.running = False
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
    parser.add_argument('--update-hz', type=float, default=30,
                        help='Movement updates per second (suggested: 10, 30, 60)')
    parser.add_argument('--log-file', default=None, help='CSV file to record sent and received updates')
    args = parser.parse_args()
    if not 0 < args.update_hz <= 60:
        parser.error('--update-hz must be greater than 0 and at most 60')
    window = tk.Tk()
    Arena(window, args.protocol, args.host, args.port, args.update_hz, args.log_file)
    window.mainloop()
