"""Tests for streaming analytics pipeline"""
import unittest

class TestStreamingPipeline(unittest.TestCase):
    
    def test_event_validation(self):
        """Test event has required fields"""
        event = {'room_id': 'R1', 'song_id': 'S1', 'event_type': 'complete'}
        required = ['room_id', 'song_id', 'event_type']
        for field in required:
            self.assertIn(field, event)
    
    def test_metric_aggregation(self):
        """Test metric calculation logic"""
        events = [
            {'event_type': 'complete'},
            {'event_type': 'skip'},
            {'event_type': 'complete'}
        ]
        complete = sum(1 for e in events if e['event_type'] == 'complete')
        skip = sum(1 for e in events if e['event_type'] == 'skip')
        
        self.assertEqual(complete, 2)
        self.assertEqual(skip, 1)

if __name__ == '__main__':
    unittest.main()
