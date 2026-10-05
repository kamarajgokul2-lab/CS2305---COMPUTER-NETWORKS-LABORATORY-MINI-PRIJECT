import io
import json
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from protocol import encode, receive
from providers import Provider
from server import BroadcastServer

class ProjectTests(unittest.TestCase):
    def test_framing(self):
        stream = io.BytesIO(encode({'a': 1}) + encode({'b': 2}))
        self.assertEqual(receive(stream), {'a': 1})
        self.assertEqual(receive(stream), {'b': 2})
        self.assertIsNone(receive(stream))
        for frame in (b'[]\n', b'{bad}\n', b'{}', b'x' * 17000 + b'\n'):
            with self.assertRaises(ValueError):
                receive(io.BytesIO(frame))

    def test_real_cache_and_failure(self):
        with patch.dict('os.environ', {'OPENWEATHER_API_KEY': 'test',
                                      'ALPHAVANTAGE_API_KEY': 'test'}):
            provider = Provider('real')
        with patch.dict('os.environ', {'ALPHAVANTAGE_API_KEY': 'test'}), patch('providers.fetch', return_value={'Global Quote': {
                '01. symbol': 'AAPL', '05. price': '230.5',
                '07. latest trading day': '2026-10-02'}}) as fetch:
            self.assertEqual(provider.get('stock')['price'], 230.5)
            provider.get('stock')
            self.assertEqual(fetch.call_count, 1)
        provider.next_fetch['stock'] = 0
        with patch.object(provider, 'real', side_effect=ValueError('secret')):
            data = provider.get('stock')
            self.assertEqual(data['status'], 'stale')
            self.assertNotIn('secret', json.dumps(data))
            self.assertEqual(provider.get('weather')['status'], 'unavailable')

    def test_network(self):
        server = BroadcastServer('127.0.0.1', 0, 0.05)
        thread = threading.Thread(target=server.serve)
        thread.start()
        self.assertTrue(server.ready.wait(3))
        sockets, readers = [], []
        try:
            for subscription in ('WEATHER', 'STOCK', 'BOTH'):
                conn = socket.create_connection(('127.0.0.1', server.port), timeout=3)
                sockets.append(conn)
                reader = conn.makefile('rb')
                readers.append(reader)
                frame = encode({'type': 'subscribe', 'subscription': subscription})
                # Deliberately split a JSON frame across writes.
                conn.sendall(frame[:7])
                conn.sendall(frame[7:])
                self.assertEqual(receive(reader)['type'], 'subscribed')
            for subscription, reader in zip(('WEATHER', 'STOCK', 'BOTH'), readers):
                first, second = receive(reader), receive(reader)
                self.assertEqual(first['type'], 'update')
                self.assertGreater(second['sequence'], first['sequence'])
                self.assertEqual('weather' in first, subscription != 'STOCK')
                self.assertEqual('stock' in first, subscription != 'WEATHER')
            sockets[0].shutdown(socket.SHUT_RDWR)
            readers[0].close()
            sockets[0].close()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                with server.lock:
                    if len(server.clients) == 2:
                        break
                time.sleep(0.02)
            with server.lock:
                self.assertEqual(len(server.clients), 2)
            self.assertEqual(receive(readers[1])['type'], 'update')
            with socket.create_connection(('127.0.0.1', server.port), timeout=3) as bad:
                bad.sendall(encode({'type': 'subscribe', 'subscription': 'INVALID'}))
                with bad.makefile('rb') as reader:
                    self.assertEqual(receive(reader)['type'], 'error')
                    self.assertIsNone(receive(reader))
            process = subprocess.run([sys.executable, str(Path(__file__).with_name('client.py')), '--port', str(server.port),
                                      '--subscription', 'STOCK', '--count', '2'],
                                     capture_output=True, text=True, timeout=5)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(process.stdout.count('"type": "update"'), 2)
        finally:
            for conn in sockets:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            for reader in readers:
                reader.close()
            for conn in sockets:
                conn.close()
            server.close()
            thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(server.clients), 0)

if __name__ == '__main__':
    unittest.main()

