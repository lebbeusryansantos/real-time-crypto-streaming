import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'crypto-solana',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='sol-client-1',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[Consumer 3 - SOL (Group 1)] Listening for events...")
for msg in consumer:
    val = msg.value
    print(f"[C3-SOL] TS: {val['timestamp']} | Price: ${val['price_usd']:.2f} | Vol: ${val['volume_24h']:,.0f}")