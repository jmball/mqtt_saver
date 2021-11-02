import unittest

from saver.saver import Saver


class SaverTestCase(unittest.TestCase):
    """tests for the mqtt saver library"""

    mqtt_host = "127.0.0.1"

    def test_init(self):
        """class init"""
        s = Saver(mqtt_host=self.mqtt_host)
        self.assertIsInstance(s, Saver)

    def test_full_run(self):
        """test a full saver run (runs forever)"""
        s = Saver(mqtt_host=self.mqtt_host)
        s.run()