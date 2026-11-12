import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

URL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true"
TOPIC = "crypto-solana"

print("--- Starting Solana Kafka Producer ---")

while True:
    try:
        res = requests.get(URL, timeout=10)
        if res.status_code == 200:
            data = res.json().get("solana", {})
            if "usd" in data:
                payload = {
                    "crypto_id": "solana",
                    "symbol": "SOL",
                    "price_usd": float(data["usd"]),
                    "volume_24h": float(data.get("usd_24h_vol", 0.0)),
                    "change_24h": round(float(data.get("usd_24h_change", 0.0)), 2),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                producer.send(TOPIC, payload)
                print(f"[SOL Sent] {payload}")
            else:
                print(f"[SOL Warning] Missing payload data: {res.text}")
        elif res.status_code == 429:
            print("[SOL Rate Limit] CoinGecko rate limit reached. Waiting...")
        else:
            print(f"[SOL Error] HTTP {res.status_code}")
    except Exception as e:
        print(f"[SOL Connection Error] {e}")

    time.sleep(10)