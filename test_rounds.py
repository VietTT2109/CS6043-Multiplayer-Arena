"""Run with: python test_rounds.py"""
import time
import unittest
from unittest.mock import patch
from server import Game

class RoundsTest(unittest.TestCase):
    def test_round_lifecycle(self):
        now = [100.0]
        with patch('server.time.monotonic', side_effect=lambda: now[0]):
            game = Game(round_seconds=3, intermission_seconds=2, min_players=2)
            game.join(1)
            self.assertEqual(game.phase, 'waiting')
            game.join(2)
            self.assertEqual(game.phase, 'playing')
            self.assertEqual(game.round_number, 1)
            game.scores[1] = 3
            game.scores[2] = 1
            now[0] += 3.1
            game.advance()
            self.assertEqual(game.phase, 'finished')
            self.assertEqual(game.winners, [1])
            now[0] += 2.1
            game.advance()
            self.assertEqual(game.phase, 'playing')
            self.assertEqual(game.round_number, 2)
            self.assertEqual(game.scores, {1: 0, 2: 0})
            game.leave(2)
            self.assertEqual(game.phase, 'waiting')

    def test_no_scoring_while_waiting_or_finished(self):
        with patch('server.time.monotonic', return_value=50):
            game = Game(round_seconds=2, intermission_seconds=3, min_players=2)
            game.join(1)
            game.coin = [400, 250]
            game.input(1, {'type':'input','seq':1,'dx':0,'dy':0})
            self.assertEqual(game.scores[1],0)

if __name__ == '__main__':
    unittest.main()
