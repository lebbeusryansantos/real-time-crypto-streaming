from flask import Flask, render_template
from cassandra.cluster import Cluster

app = Flask(__name__)

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('crypto_stream')

@app.route('/')
def index():
    # Fetch dogecoin data (LIMIT 50 for 100 total sample inspection)
    doge_rows = session.execute(
        "SELECT crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp "
        "FROM market_data WHERE crypto_id = 'dogecoin' "
        "ORDER BY timestamp DESC LIMIT 50"
    )
    doge_list = list(doge_rows)

    # Fetch solana data (LIMIT 50 for 100 total sample inspection)
    sol_rows = session.execute(
        "SELECT crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp "
        "FROM market_data WHERE crypto_id = 'solana' "
        "ORDER BY timestamp DESC LIMIT 50"
    )
    sol_list = list(sol_rows)

    doge_count = len(doge_list)
    sol_count = len(sol_list)
    total_count = doge_count + sol_count

    return render_template(
        'index.html',
        doge=doge_list,
        sol=sol_list,
        doge_count=doge_count,
        sol_count=sol_count,
        total_count=total_count
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
