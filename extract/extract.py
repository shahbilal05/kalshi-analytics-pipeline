import requests
from datetime import datetime

def fetch_closed_events(limit=200, cursor=None):
    events_url = "https://api.elections.kalshi.com/trade-api/v2/events"
    events = []

    while True:
        params = {
            'status': 'closed',
            'limit': limit
        }

        if cursor:
            params['cursor'] = cursor
        
        response = requests.get(events_url, params=params)
        response.raise_for_status()

        data = response.json()
        events.extend(data.get("events", []))

        cursor = data.get("cursor")
        if not cursor:
            break

    return events


def fetch_closed_markets(limit=200, cursor=None):
    markets_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    markets = []

    while True:
        params = {
            'status': 'closed',
            'limit': limit
        }

        if cursor: 
            params['cursor'] = cursor

        response = requests.get(markets_url, params=params)
        response.raise_for_status()

        data = response.json()
        markets.extend(data.get("markets", []))

        cursor = data.get("cursor")
        if not cursor:
            break

    return markets