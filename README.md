# Real-time Weather / Stock TCP Broadcast

Computer Networks mini-project using Python 3.10+ and only the standard library.
The default simulated mode needs no internet, API keys, or installed packages.

## Windows quick start

Open PowerShell in this folder:

```powershell
cd "C:\Users\janan\Downloads\cn mini"
python server.py
```

In three additional PowerShell windows, run one command per window:

```powershell
cd "C:\Users\janan\Downloads\cn mini"
python client.py --subscription WEATHER
python client.py --subscription STOCK
python client.py --subscription BOTH
```

Each client receives JSON updates every 3 seconds. Press Ctrl+C to stop a client
or the server. If `python` is unavailable, try `py` in its place. Install Python
3.10 or newer if neither command works. No dependency installation is needed;
`python -m pip install -r requirements.txt` is optional and installs nothing.

Useful options:

```powershell
python server.py --host 127.0.0.1 --port 5001 --interval 2
python client.py --host 127.0.0.1 --port 5001 --subscription BOTH --count 5
python -m unittest -v
```

Use matching ports. Stop the existing server if the port is already in use.

## LAN demonstration

1. Connect server and client computers to the same trusted LAN or Wi-Fi.
2. Start `python server.py --host 0.0.0.0 --port 5000` on the server computer.
3. Run `ipconfig` there and find the active adapter's IPv4 address (for example
   192.168.1.10). Do not use 0.0.0.0 as a client destination.
4. On another computer, copy the project and run:
   `python client.py --host 192.168.1.10 --port 5000 --subscription WEATHER`.
5. If Windows Firewall prompts, allow Python on the Private network. If needed,
   create an inbound TCP rule for port 5000 limited to your private network.
6. Check reachability with PowerShell:
   `Test-NetConnection 192.168.1.10 -Port 5000`.

Guest Wi-Fi/client isolation, VPNs and firewalls can block LAN connections.
This classroom protocol has no authentication or encryption; use a trusted LAN.

## Optional real APIs

Copy `.env.example` to `.env`, then edit `.env` with your own keys:

```powershell
Copy-Item .env.example .env
notepad .env
python server.py --mode real
```

Alternatively set environment variables in the server PowerShell window:

```powershell
$env:OPENWEATHER_API_KEY = "your-key"
$env:ALPHAVANTAGE_API_KEY = "your-key"
python server.py --mode real
```

Use OpenWeather latitude/longitude for the desired location. WEATHER_CITY is
only the simulated display name; real weather uses the provider's location name.
STOCK_SYMBOL selects the quote symbol. Both API keys are required in real mode.
Existing environment variables take precedence over `.env`.

Provider documentation:
- https://openweathermap.org/api/current
- https://www.alphavantage.co/documentation/#latestprice

Weather temperature is Celsius and humidity is percent. Stock price is in the
instrument's quote currency; the project does not assume all symbols are USD.
Real data freshness depends on provider access and the market. GLOBAL_QUOTE can
be end-of-day rather than live; TCP broadcasts every few seconds do not imply
new market quotes every few seconds. API_REFRESH_SECONDS defaults to 3600 to
reduce calls; choose a refresh period appropriate to your provider quota.

API requests use HTTPS and an 8-second timeout. Results are cached separately
from the broadcast interval. A failed refresh retains prior values with
`status: stale`, or sends `status: unavailable` if no data was fetched. Errors
never switch silently to simulated data. `fetched_at` identifies the last
successful retrieval, and stock `latest_trading_day` identifies the quote day.
Network/API refreshes can delay a broadcast in real mode. Keys and quotas must
be verified with your own account; simulated mode is the tested offline demo.
Keep `.env` private; `.gitignore` excludes it. The .env parser supports simple
NAME=value lines, quoted values, and full-line comments (no interpolation).

## Architecture and protocol

Weather/stock provider -> broadcast thread -> bounded per-client queues ->
client handler threads -> TCP sockets -> subscriber clients.

The server accepts many simultaneous clients, protects its shared registry with
a lock, and broadcasts one shared snapshot with a monotonically increasing
sequence number. A handler writes each client's messages; a reader detects
connection closure. Slow clients are disconnected when their queue fills.
Ctrl+C closes connected sockets and stops server workers.

Every message is one UTF-8 JSON object followed by a newline. TCP is a byte
stream, so socket reads can split or combine messages; the receiver reads to the
newline rather than assuming one recv equals one message. Maximum frame size
is 16 KiB. Clients have 5 seconds to subscribe.

First client message:

```json
{"type":"subscribe","subscription":"WEATHER"}
```

The server replies with `type: subscribed`, followed by `type: update` objects
containing `sequence`, UTC ISO timestamp `time`, and only the selected `weather`
and/or `stock` fields. Invalid subscriptions get `type: error` then disconnect.
Subscription is fixed for the connection; reconnect to change it. An update
arrives on the next broadcast tick. Clients display complete JSON for easy
inspection. There is no automatic reconnection; rerun the client after a restart.

## Files and testing

- server.py: listener, subscription validation, threads, fan-out and shutdown.
- client.py: subscription command line and streaming receiver.
- protocol.py: JSON-lines encoder and bounded decoder.
- providers.py: simulation, .env loader, cached real providers.
- test_project.py: framing, concurrent subscriptions, malformed requests,
  disconnect cleanup, client process and mocked provider tests.
- requirements.txt: no external dependencies.

Run `python -m unittest -v`. Tests use loopback and an automatically selected
port, and do not call real APIs. For your presentation, show three subscribers,
stop one and observe the others continuing, then demonstrate a LAN client.
Discuss TCP reliability, application framing, unicast fan-out (this is not UDP
broadcast), multithreading, critical sections, queue backpressure and caching.
