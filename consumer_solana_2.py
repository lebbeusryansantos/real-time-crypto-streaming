import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'crypto-solana',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='sol-client-2',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[Consumer 4 - SOL (Group 2)] Listening for events...")
for msg in consumer:
    val = msg.value
    print(f"[C4-SOL Raw Message Offset={msg.offset}] -> {val}")