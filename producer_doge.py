import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

URL = "https://api.coingecko.com/api/v3/simple/price?ids=dogecoin&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true"
TOPIC = "crypto-dogecoin"

print("--- Starting Dogecoin Kafka Producer ---")

while True:
    try:
        res = requests.get(URL, timeout=10)
        if res.status_code == 200:
            data = res.json().get("dogecoin", {})
            if "usd" in data:
                payload = {
                    "crypto_id": "dogecoin",
                    "symbol": "DOGE",
                    "price_usd": float(data["usd"]),
                    "volume_24h": float(data.get("usd_24h_vol", 0.0)),
                    "change_24h": round(float(data.get("usd_24h_change", 0.0)), 2),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                producer.send(TOPIC, payload)
                print(f"[DOGE Sent] {payload}")
            else:
                print(f"[DOGE Warning] Missing payload data: {res.text}")
        elif res.status_code == 429:
            print("[DOGE Rate Limit] CoinGecko rate limit reached. Waiting...")
        else:
            print(f"[DOGE Error] HTTP {res.status_code}")
    except Exception as e:
        print(f"[DOGE Connection Error] {e}")

    time.sleep(10)