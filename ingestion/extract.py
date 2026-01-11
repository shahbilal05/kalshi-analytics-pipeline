import requests
from datetime import datetime

def fetch_events_since(since_timestamp, limit=200, cursor=None):
    events_url = "https://api.elections.kalshi.com/trade-api/v2/events"
    events = []

    min_close_ts = int(since_timestamp.timestamp() * 1000) # fetch events after this timestamp (converted to Unix timestamp)

    print(f"Fetching events closed after {since_timestamp.isoformat()}.")

    while True:
        params = {
            'status': 'closed',
            'limit': limit,
            'min_close_ts': min_close_ts  
        }

        if cursor:
            params['cursor'] = cursor

        try:
            response = requests.get(events_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            events.extend(data.get("events", []))

            cursor = data.get("cursor")
            if not cursor:
                break

        except Exception as e:
            print(f"Error fetching events: {e}")
            raise
    
    print(f"Fetched {len(events)} new events.")
    return events


def fetch_markets_since(since_timestamp, limit=200, cursor=None):
    markets_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    markets = []

    min_close_ts = int(since_timestamp.timestamp() * 1000) # fetch markets after this timestamp

    while True:
        params = {
            'status': 'closed',
            'limit': limit,
            'min_close_ts': min_close_ts  
        }

        if cursor: 
            params['cursor'] = cursor
        
        try:
            response = requests.get(markets_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            markets.extend(data.get("markets", []))

            cursor = data.get("cursor")
            if not cursor:
                break

        except Exception as e:
            print(f"Error fetching markets: {e}")
            raise

    print(f"Fetched {len(markets)} new markets.")
    return markets

