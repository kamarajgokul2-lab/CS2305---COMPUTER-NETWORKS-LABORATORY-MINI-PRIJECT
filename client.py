"""Subscribe to a TCP broadcaster and display JSON updates."""
import argparse
import json
import socket
from protocol import SUBSCRIPTIONS, encode, receive

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--subscription', type=str.upper, choices=SUBSCRIPTIONS, default='BOTH')
    parser.add_argument('--count', type=int, default=0, help='Stop after N updates; 0 runs continuously')
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.count < 0:
        parser.error('Invalid port or count')
    try:
        with socket.create_connection((args.host, args.port), timeout=10) as conn:
            conn.settimeout(None)
            conn.sendall(encode({'type': 'subscribe', 'subscription': args.subscription}))
            with conn.makefile('rb') as reader:
                count = 0
                while True:
                    message = receive(reader)
                    if message is None:
                        print('Server disconnected.')
                        break
                    print(json.dumps(message, indent=2), flush=True)
                    if message.get('type') == 'error':
                        return 1
                    if message.get('type') == 'update':
                        count += 1
                        if args.count and count >= args.count:
                            break
    except KeyboardInterrupt:
        print('\nDisconnected.')
    except (OSError, ValueError, UnicodeError) as error:
        print('Connection failed:', error)
        return 1
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
