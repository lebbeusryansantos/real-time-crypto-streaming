import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'crypto-dogecoin',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='doge-client-1',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[Consumer 1 - DOGE (Group 1)] Listening for events...")
for msg in consumer:
    val = msg.value
    print(f"[C1-DOGE] TS: {val['timestamp']} | Price: ${val['price_usd']:.4f} | Vol: ${val['volume_24h']:,.0f}")