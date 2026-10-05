"""Multithreaded TCP publisher. Each client has an independent bounded queue."""
import argparse
import logging
import math
import queue
import signal
import socket
import threading
from protocol import SUBSCRIPTIONS, encode, receive
from providers import Provider, load_env, timestamp

class BroadcastServer:
    def __init__(self, host='0.0.0.0', port=5000, interval=3, mode='simulated'):
        self.host, self.port, self.interval = host, port, interval
        self.provider = Provider(mode)
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.clients = {}
        self.connections = set()
        self.workers = []
        self.ready = threading.Event()

    def close(self):
        self.stop.set()
        with self.lock:
            connections = list(self.connections)
        for conn in connections:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def handle(self, conn, address):
        reader = None
        try:
            conn.settimeout(5)
            reader = conn.makefile('rb')
            request = receive(reader)
            subscription = request.get('subscription') if request else None
            if request is None or request.get('type') != 'subscribe' or subscription not in SUBSCRIPTIONS:
                conn.sendall(encode({'type': 'error', 'message': 'Send subscribe with WEATHER, STOCK or BOTH'}))
                return
            conn.sendall(encode({'type': 'subscribed', 'subscription': subscription}))
            outgoing = queue.Queue(maxsize=4)
            with self.lock:
                self.clients[conn] = (subscription, outgoing)
            logging.info('Client %s subscribed to %s', address, subscription)
            # Reader detects graceful disconnect independently of outbound data.
            done = threading.Event()
            def monitor():
                try:
                    while not done.is_set() and not self.stop.is_set():
                        try:
                            if not conn.recv(1024):
                                break
                        except socket.timeout:
                            continue
                except OSError:
                    pass
                finally:
                    done.set()
            watcher = threading.Thread(target=monitor, daemon=True)
            watcher.start()
            try:
                while not self.stop.is_set() and not done.is_set():
                    try:
                        message = outgoing.get(timeout=0.2)
                    except queue.Empty:
                        continue
                    conn.sendall(encode(message))
            finally:
                done.set()
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                watcher.join(timeout=6)
        except (OSError, ValueError, UnicodeError):
            logging.info('Client %s closed or sent an invalid frame', address)
        finally:
            with self.lock:
                self.clients.pop(conn, None)
                self.connections.discard(conn)
            if reader:
                reader.close()
            conn.close()

    def broadcast(self):
        sequence = 0
        while not self.stop.is_set():
            sequence += 1
            data = {topic: self.provider.get(topic) for topic in ('weather', 'stock')}
            with self.lock:
                clients = list(self.clients.items())
            for conn, (subscription, outgoing) in clients:
                message = {'type': 'update', 'sequence': sequence, 'time': timestamp()}
                for topic in ('weather', 'stock'):
                    if subscription in (topic.upper(), 'BOTH'):
                        message[topic] = data[topic]
                try:
                    outgoing.put_nowait(message)
                except queue.Full:
                    # A slow client cannot hold up other clients.
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            self.stop.wait(self.interval)

    def serve(self):
        publisher = None
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((self.host, self.port))
                self.port = listener.getsockname()[1]
                listener.listen(64)
                listener.settimeout(0.5)
                publisher = threading.Thread(target=self.broadcast, daemon=True)
                publisher.start()
                self.ready.set()
                logging.info('Listening on %s:%s (%s)', self.host, self.port, self.provider.mode)
                while not self.stop.is_set():
                    try:
                        conn, address = listener.accept()
                    except socket.timeout:
                        continue
                    with self.lock:
                        self.connections.add(conn)
                    worker = threading.Thread(target=self.handle, args=(conn, address), daemon=True)
                    self.workers = [w for w in self.workers if w.is_alive()]
                    self.workers.append(worker)
                    worker.start()
        finally:
            self.close()
            for worker in self.workers:
                worker.join(timeout=7)
            if publisher:
                publisher.join(timeout=18)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--interval', type=float, default=3)
    parser.add_argument('--mode', choices=('simulated', 'real'), default='simulated')
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or not math.isfinite(args.interval) or args.interval <= 0:
        parser.error('Use port 1..65535 and a finite positive interval')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    try:
        load_env()
        server = BroadcastServer(**vars(args))
        signal.signal(signal.SIGINT, lambda *_: server.close())
        signal.signal(signal.SIGTERM, lambda *_: server.close())
        server.serve()
    except (OSError, ValueError) as error:
        parser.exit(1, 'Server failed: ' + str(error) + '\n')

if __name__ == '__main__':
    main()
