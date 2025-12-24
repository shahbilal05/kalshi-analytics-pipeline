import requests

response = requests.get(
    "https://api.elections.kalshi.com/trade-api/v2/events",
    params={"status": "closed", "limit": 5}
)

print(response.json())