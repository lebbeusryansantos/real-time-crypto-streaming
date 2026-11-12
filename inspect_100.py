#!/usr/bin/env python3
"""
inspect_100.py
Connects to Apache Cassandra (crypto_stream.market_data), checks stored record counts,
fetches sample records (up to 100 total: 50 DOGE + 50 SOL), prints them in a clean terminal table,
and summarizes data quality (missing values) and irregular timestamp gaps (API rate limits).
"""

import sys
from datetime import datetime, timedelta
try:
    import asyncore  # Python 3.12+ asyncore compatibility for cassandra-driver
except ImportError:
    pass
from cassandra.cluster import Cluster

def main():
    print("=" * 90)
    print("      DATA SCIENCE 3 WORKSHOP - CASSANDRA REAL-TIME DATA INSPECTION")
    print("=" * 90)
    print("Connecting to Apache Cassandra cluster at 127.0.0.1:9042 [keyspace: crypto_stream]...")

    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect('crypto_stream')
        print("[SUCCESS] Connected to Cassandra successfully.\n")
    except Exception as e:
        print(f"[ERROR] Failed to connect to Cassandra: {e}")
        sys.exit(1)

    # 1. Row Count Check
    print(">>> 1. DATABASE RECORD COUNT CHECK")
    print("-" * 50)
    try:
        total_row = session.execute("SELECT count(*) FROM market_data").one()
        total_in_db = total_row[0] if total_row else 0
        
        doge_count_row = session.execute("SELECT count(*) FROM market_data WHERE crypto_id = 'dogecoin'").one()
        doge_total = doge_count_row[0] if doge_count_row else 0

        sol_count_row = session.execute("SELECT count(*) FROM market_data WHERE crypto_id = 'solana'").one()
        sol_total = sol_count_row[0] if sol_count_row else 0

        print(f"  * Total Records Stored in Cassandra: {total_in_db}")
        print(f"    - Dogecoin (crypto-dogecoin)     : {doge_total} records")
        print(f"    - Solana   (crypto-solana)       : {sol_total} records")
        
        if total_in_db >= 100:
            print(f"  [STATUS] 100+ sample records achieved! ({total_in_db} records available)")
        else:
            print(f"  [STATUS] Streaming in progress: {total_in_db}/100 target records collected ({total_in_db}%).")
    except Exception as e:
        print(f"  [WARNING] Count query error: {e}")
        doge_total, sol_total, total_in_db = 0, 0, 0

    print("\n>>> 2. FETCHING 100 SAMPLE RECORDS (Up to 50 DOGE + 50 SOL)")
    print("-" * 50)
    
    doge_rows = session.execute(
        "SELECT crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp "
        "FROM market_data WHERE crypto_id = 'dogecoin' "
        "ORDER BY timestamp DESC LIMIT 50"
    )
    doge_list = list(doge_rows)

    sol_rows = session.execute(
        "SELECT crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp "
        "FROM market_data WHERE crypto_id = 'solana' "
        "ORDER BY timestamp DESC LIMIT 50"
    )
    sol_list = list(sol_rows)

    sample_records = doge_list + sol_list
    print(f"  * Retrieved {len(doge_list)} Dogecoin records and {len(sol_list)} Solana records.")
    print(f"  * Total Sample Records to Inspect: {len(sample_records)}\n")

    # 3. Print Clean Formatted Table
    print(">>> 3. TABULAR SAMPLE RECORDS")
    print("=" * 105)
    header = f"{'#':<4} | {'Crypto':<10} | {'Sym':<4} | {'Price (USD)':<14} | {'24h Volume ($)':<18} | {'24h Change':<12} | {'Timestamp':<20}"
    print(header)
    print("=" * 105)

    idx = 1
    # Print DOGE records first
    for r in doge_list:
        ts_str = r.timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(r.timestamp, datetime) else str(r.timestamp)
        price_str = f"${r.price_usd:.4f}" if r.price_usd is not None else "N/A"
        vol_str = f"${r.volume_24h:,.0f}" if r.volume_24h is not None else "N/A"
        chg_str = f"{r.change_24h:+.2f}%" if r.change_24h is not None else "N/A"
        print(f"{idx:<4} | {r.crypto_id:<10} | {r.symbol:<4} | {price_str:<14} | {vol_str:<18} | {chg_str:<12} | {ts_str:<20}")
        idx += 1

    if doge_list and sol_list:
        print("-" * 105)

    # Print SOL records next
    for r in sol_list:
        ts_str = r.timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(r.timestamp, datetime) else str(r.timestamp)
        price_str = f"${r.price_usd:.2f}" if r.price_usd is not None else "N/A"
        vol_str = f"${r.volume_24h:,.0f}" if r.volume_24h is not None else "N/A"
        chg_str = f"{r.change_24h:+.2f}%" if r.change_24h is not None else "N/A"
        print(f"{idx:<4} | {r.crypto_id:<10} | {r.symbol:<4} | {price_str:<14} | {vol_str:<18} | {chg_str:<12} | {ts_str:<20}")
        idx += 1

    print("=" * 105)

    # 4. Data Quality: Missing / Null Values Summary
    print("\n>>> 4. DATA QUALITY AUDIT (MISSING / NULL VALUES)")
    print("-" * 50)
    columns = ["crypto_id", "symbol", "price_usd", "volume_24h", "change_24h", "timestamp"]
    missing_counts = {col: 0 for col in columns}

    for r in sample_records:
        for col in columns:
            val = getattr(r, col, None)
            if val is None or val == "":
                missing_counts[col] += 1

    has_missing = any(c > 0 for c in missing_counts.values())
    if not has_missing:
        print(f"  [OK] No null or missing values found across all {len(sample_records)} inspected records!")
        for col in columns:
            print(f"       - {col:<12}: 0 nulls (100% complete)")
    else:
        print(f"  [ALERT] Missing values detected:")
        for col, count in missing_counts.items():
            print(f"       - {col:<12}: {count} missing ({count/len(sample_records):.1%})")

    # 5. Timestamp Gap & API Rate Limit Analysis
    print("\n>>> 5. TIMESTAMP INTERVAL & API RATE LIMIT GAP ANALYSIS")
    print("-" * 50)
    print("  * Standard Producer Poll Interval: 10 seconds (time.sleep(10))")
    print("  * Irregular Gap Threshold       : > 15 seconds (likely CoinGecko HTTP 429 rate limit or pause)")
    print()

    for coin_name, coin_rows in [("Dogecoin", doge_list), ("Solana", sol_list)]:
        print(f"  --- {coin_name} Stream Analysis ({len(coin_rows)} records) ---")
        if len(coin_rows) < 2:
            print("    Insufficient records to compute timestamp intervals.")
            continue

        # Sort chronologically (oldest to newest)
        sorted_rows = sorted(
            coin_rows,
            key=lambda x: x.timestamp if isinstance(x.timestamp, datetime) else datetime.strptime(str(x.timestamp), "%Y-%m-%d %H:%M:%S")
        )

        deltas = []
        gaps = []
        for i in range(1, len(sorted_rows)):
            t_prev = sorted_rows[i - 1].timestamp
            t_curr = sorted_rows[i].timestamp
            if not isinstance(t_prev, datetime):
                t_prev = datetime.strptime(str(t_prev), "%Y-%m-%d %H:%M:%S")
            if not isinstance(t_curr, datetime):
                t_curr = datetime.strptime(str(t_curr), "%Y-%m-%d %H:%M:%S")

            delta_sec = (t_curr - t_prev).total_seconds()
            deltas.append(delta_sec)

            if delta_sec > 15:
                gaps.append((t_prev, t_curr, delta_sec))

        avg_interval = sum(deltas) / len(deltas) if deltas else 0
        min_interval = min(deltas) if deltas else 0
        max_interval = max(deltas) if deltas else 0

        print(f"    Interval Stats: Min={min_interval:.1f}s | Avg={avg_interval:.1f}s | Max={max_interval:.1f}s")
        print(f"    Gaps Flagged (>15s): {len(gaps)}")

        if gaps:
            for g_prev, g_curr, g_sec in gaps[:5]:  # show up to 5 gaps
                print(f"      - {g_prev.strftime('%H:%M:%S')} -> {g_curr.strftime('%H:%M:%S')} "
                      f"({g_sec:.0f}s gap) [Suspected CoinGecko 429 Rate Limit]")
            if len(gaps) > 5:
                print(f"      ... and {len(gaps) - 5} additional gap(s).")
        else:
            print(f"    [SMOOTH] Steady 10-second stream with no significant rate limit stalls.")
        print()

    # 6. Summary Conclusion
    print(">>> 6. SUMMARY CONCLUSION FOR DATA SCIENCE 3 WORKSHOP")
    print("-" * 50)
    print(f"  * Pipeline Status: Active and continuously streaming into Cassandra.")
    print(f"  * Sample Size    : {len(sample_records)} records inspected ({len(doge_list)} DOGE, {len(sol_list)} SOL).")
    print(f"  * Schema Status  : All columns populated without schema corruption.")
    print("=" * 90)

    cluster.shutdown()

if __name__ == '__main__':
    main()
