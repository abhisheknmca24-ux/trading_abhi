import unittest
from datetime import datetime
from signal_sources import SignalManager, SignalSource

class TestSignalSources(unittest.TestCase):
    def setUp(self):
        self.manager = SignalManager()
        # Mock _now to keep dates consistent
        self.manager._now = lambda: datetime(2026, 5, 8, 12, 0, 0)
        
    def test_basic_signal(self):
        self.manager.add_signals(["15:20 EURUSD CALL"], SignalSource.EXTERNAL)
        merged = self.manager.get_merged_signals()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["direction"], "CALL")
        self.assertEqual(merged[0]["boost"], 0)
        self.assertFalse(merged[0]["skip"])

    def test_multiple_sources_boost(self):
        self.manager.add_signals(["15:20 EURUSD CALL"], SignalSource.EXTERNAL)
        self.manager.add_signals(["15:20 EURUSD CALL"], SignalSource.INTERNAL)
        merged = self.manager.get_merged_signals()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["boost"], 10)  # Standard multiple source boost
        
    def test_external_boost_param(self):
        self.manager.add_signals(["15:20 EURUSD CALL BOOST=20"], SignalSource.EXTERNAL)
        merged = self.manager.get_merged_signals()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["boost"], 20)
        
    def test_external_skip_param(self):
        self.manager.add_signals(["15:20 EURUSD CALL SKIP=TRUE"], SignalSource.EXTERNAL)
        merged = self.manager.get_merged_signals()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["direction"], "SKIP")
        self.assertTrue(merged[0]["skip"])

    def test_conflict_resolution(self):
        self.manager.add_signals(["15:20 EURUSD CALL"], SignalSource.EXTERNAL)
        self.manager.add_signals(["15:20 EURUSD PUT"], SignalSource.INTERNAL)
        merged = self.manager.get_merged_signals()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["direction"], "SKIP")
        self.assertTrue(merged[0]["skip"])

    def test_explicit_skip_direction(self):
        self.manager.add_signals(["15:20 EURUSD SKIP"], SignalSource.EXTERNAL)
        merged = self.manager.get_merged_signals()
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["direction"], "SKIP")
        self.assertTrue(merged[0]["skip"])

if __name__ == "__main__":
    unittest.main()
