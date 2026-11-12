import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from cassandra.cluster import Cluster

print("Initializing Spark & Cassandra...")
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('crypto_stream')

prepared_stmt = session.prepare("""
    INSERT INTO market_data (crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
""")

spark = SparkSession.builder \
    .appName("CryptoStreamingPipeline") \
    .master("local[*]") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("crypto_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("volume_24h", DoubleType(), True),
    StructField("change_24h", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

consumer = KafkaConsumer(
    'crypto-dogecoin', 'crypto-solana',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='spark-cassandra-pipeline',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting Spark Micro-Batch Processing Loop... (Press Ctrl+C to stop)")
print("Spark Web UI available at: http://localhost:4040")
batch_id = 0

try:
    while True:
        raw_msgs = consumer.poll(timeout_ms=5000)
        records = []
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                records.append(msg.value)

        if records:
            # Create Spark DataFrame from micro-batch
            df = spark.createDataFrame(records, schema=schema)
            count = df.count()

            print(f"\n==========================================")
            print(f"--- Processing Spark Micro-Batch {batch_id} ({count} rows) ---")
            df.show(truncate=False)

            # Insert parsed batch into Cassandra
            for row in df.collect():
                # Parse timestamp string to datetime object for Cassandra
                try:
                    ts_val = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_val = datetime.now()

                session.execute(prepared_stmt, (
                    row['crypto_id'],
                    row['symbol'],
                    float(row['price_usd']),
                    float(row['volume_24h']),
                    float(row['change_24h']),
                    ts_val
                ))

            print(f"[Spark Batch {batch_id}] Successfully persisted to Cassandra table -> market_data")
            print(f"==========================================\n")
            batch_id += 1
        else:
            time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping streaming pipeline...")
finally:
    consumer.close()
    spark.stop()
    cluster.shutdown()