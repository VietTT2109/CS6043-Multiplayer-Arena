import csv
import io
import queue
import unittest
from collections import deque
from client import Arena

class MetricsTest(unittest.TestCase):
    def test_correction_from_authoritative_ack(self):
        arena = Arena.__new__(Arena)
        arena.incoming = queue.Queue()
        arena.incoming.put({'type':'move','id':1,'seq':2,'x':100,'y':100,'sent_at':0})
        arena.player_id = 1
        arena.last_own_ack = 1
        arena.pending = deque([(2,1,0,0.1),(3,1,0,0.1)])
        arena.x, arena.y = 150, 100
        arena.other = {}
        arena.correction_samples = deque(maxlen=500)
        arena.correction_count = 0
        arena.correction_threshold_px = 1.0
        arena.log_writer = csv.writer(buf := io.StringIO())
        arena.protocol = 'tcp'
        arena.process_messages()
        self.assertEqual(arena.last_own_ack, 2)
        self.assertEqual(arena.correction_count, 1)
        self.assertAlmostEqual(arena.x, 115.0)
        self.assertAlmostEqual(arena.correction_samples[-1], 35.0)
        self.assertEqual(next(csv.reader(io.StringIO(buf.getvalue())))[0], 'correction')

if __name__ == '__main__':
    unittest.main()
