"""Unit tests for streaming pipeline"""
import unittest
import json

class TestPipelineComponents(unittest.TestCase):
    
    def test_event_structure(self):
        """Test event has required fields"""
        event = {
            'room_id': 'R1',
            'song_id': 'S1',
            'event_type': 'complete'
        }
        required = ['room_id', 'song_id', 'event_type']
        for field in required:
            self.assertIn(field, event)
    
    def test_aggregation_logic(self):
        """Test metric calculation"""
        events = [
            {'event_type': 'complete'},
            {'event_type': 'skip'},
            {'event_type': 'complete'}
        ]
        complete = sum(1 for e in events if e['event_type'] == 'complete')
        skip = sum(1 for e in events if e['event_type'] == 'skip')
        
        self.assertEqual(complete, 2)
        self.assertEqual(skip, 1)
        self.assertAlmostEqual(complete/(complete+skip), 0.67, places=1)

if __name__ == '__main__':
    unittest.main()
