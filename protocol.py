"""UTF-8 JSON-lines framing; TCP reads may split or combine messages."""
import json

MAX_FRAME = 16384
SUBSCRIPTIONS = ('WEATHER', 'STOCK', 'BOTH')

def encode(message):
    return (json.dumps(message, allow_nan=False) + '\n').encode('utf-8')

def receive(reader):
    line = reader.readline(MAX_FRAME + 1)
    if not line:
        return None
    if len(line) > MAX_FRAME or not line.endswith(b'\n'):
        raise ValueError('Invalid or oversized JSON frame')
    message = json.loads(line)
    if not isinstance(message, dict):
        raise ValueError('Expected a JSON object')
    return message
