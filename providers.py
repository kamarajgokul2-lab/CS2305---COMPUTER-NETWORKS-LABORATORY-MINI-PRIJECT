"""Simulated data and optional HTTPS providers (no third-party dependencies)."""
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

def timestamp():
    return datetime.now(timezone.utc).isoformat()

def load_env():
    path = Path(__file__).with_name('.env')
    if path.exists():
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                key, separator, value = line.partition('=')
                if not separator or not key.strip().isidentifier():
                    raise ValueError('Invalid .env entry; use NAME=value')
                os.environ.setdefault(key.strip(), value.strip().strip('\"\''))

def fetch(url, parameters):
    with urlopen(url + '?' + urlencode(parameters), timeout=8) as response:
        return json.load(response)

class Provider:
    def __init__(self, mode):
        self.mode = mode
        self.cache = {}
        self.next_fetch = {}
        self.city = os.getenv('WEATHER_CITY', 'Chennai')
        self.symbol = os.getenv('STOCK_SYMBOL', 'AAPL')
        self.refresh = float(os.getenv('API_REFRESH_SECONDS', '3600'))
        if self.refresh <= 0 or not __import__('math').isfinite(self.refresh):
            raise ValueError('API_REFRESH_SECONDS must be finite and positive')
        if mode == 'real' and not all(os.getenv(k) for k in
                ('OPENWEATHER_API_KEY', 'ALPHAVANTAGE_API_KEY')):
            raise ValueError('Real mode needs OPENWEATHER_API_KEY and ALPHAVANTAGE_API_KEY')

    def real(self, topic):
        if topic == 'weather':
            data = fetch('https://api.openweathermap.org/data/2.5/weather', {
                'lat': os.getenv('WEATHER_LAT', '13.0827'),
                'lon': os.getenv('WEATHER_LON', '80.2707'),
                'appid': os.environ['OPENWEATHER_API_KEY'], 'units': 'metric'})
            return {'city': data['name'], 'temperature_c': float(data['main']['temp']),
                    'humidity_percent': int(data['main']['humidity']),
                    'condition': data['weather'][0]['description']}
        data = fetch('https://www.alphavantage.co/query', {
            'function': 'GLOBAL_QUOTE', 'symbol': self.symbol,
            'apikey': os.environ['ALPHAVANTAGE_API_KEY']})
        quote = data.get('Global Quote', {})
        if not quote or '05. price' not in quote:
            raise ValueError('Quote unavailable (key, symbol, entitlement or rate limit)')
        return {'symbol': quote['01. symbol'], 'price': float(quote['05. price']),
                'latest_trading_day': quote['07. latest trading day']}

    def get(self, topic):
        if self.mode == 'simulated':
            values = ({'city': self.city, 'temperature_c': round(random.uniform(28, 35), 2),
                       'humidity_percent': random.randint(60, 90), 'condition': 'Simulated'}
                      if topic == 'weather' else
                      {'symbol': self.symbol, 'price': round(random.uniform(220, 240), 2)})
            return dict(values, source='simulated', status='ok', fetched_at=timestamp())
        if time.monotonic() >= self.next_fetch.get(topic, 0):
            self.next_fetch[topic] = time.monotonic() + self.refresh
            try:
                self.cache[topic] = dict(self.real(topic), source='api', status='ok',
                                         fetched_at=timestamp())
            except Exception:
                # Never log URLs/exceptions containing API credentials.
                old = self.cache.get(topic, {'source': 'api'})
                self.cache[topic] = dict(old, status='stale' if 'fetched_at' in old else 'unavailable',
                                         error='API request failed; check key, network and quota')
        return self.cache[topic].copy()
