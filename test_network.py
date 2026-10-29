"""Headless two-client TCP/UDP sanity test"""
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

SERVER = Path(__file__).with_name('server.py')

def test(protocol, port):
    proc = subprocess.Popen([sys.executable, str(SERVER), '--protocol', protocol,
                             '--host','127.0.0.1','--port',str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    a = b = None
    try:
        time.sleep(.35)
        typ = socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM
        a = socket.socket(socket.AF_INET, typ)
        b = socket.socket(socket.AF_INET, typ)
        for s in (a,b):
            s.settimeout(2)
            s.connect(('127.0.0.1',port))
        def send(s, m):
            raw = json.dumps(m).encode()
            s.sendall(raw+b'\n' if protocol=='tcp' else raw)
        buffers = {a:b'',b:b''}
        def recv(s):
            if protocol == 'udp':
                return json.loads(s.recv(8192))
            while b'\n' not in buffers[s]:
                buffers[s] += s.recv(8192)
            line, buffers[s] = buffers[s].split(b'\n',1)
            return json.loads(line)
        if protocol=='udp':
            send(a,{'type':'join'});send(b,{'type':'join'})
        def welcome(s):
            for _ in range(10):
                m = recv(s)
                if m.get('type')=='welcome':return m
            raise AssertionError('welcome not received')
        ida = welcome(a)['id']; idb = welcome(b)['id']
        assert ida != idb
        # forged coordinates cannot be used: server expects 'input'.
        send(a,{'type':'move','x':790,'y':490,'seq':1})
        time.sleep(.07)
        send(a,{'type':'input','dx':1,'dy':0,'seq':2,'sent_at':time.time()})
        messages=[]
        for _ in range(10):
            m=recv(b);messages.append(m)
            if m.get('type')=='move' and m.get('id')==ida: break
        positions=[m for m in messages if m.get('type')=='move' and m.get('id')==ida]
        assert positions, messages
        assert 400 < positions[-1]['x'] < 430, positions[-1]
        print(protocol, 'PASS', 'players',ida,idb,'server x',positions[-1]['x'])
    finally:
        for s in (a,b):
            if s: s.close()
        proc.terminate()
        try: proc.wait(timeout=2)
        except subprocess.TimeoutExpired: proc.kill()

if __name__ == '__main__':
    test('tcp', 51990)
    test('udp', 51991)
