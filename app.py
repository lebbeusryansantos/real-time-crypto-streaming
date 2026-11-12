from flask import Flask, render_template
from cassandra.cluster import Cluster

app = Flask(__name__)

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('crypto_stream')

@app.route('/')
def index():
    # we fetch dogecoin data :)
    doge_rows = session.execute(
        "SELECT crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp "
        "FROM market_data WHERE crypto_id = 'dogecoin' "
        "ORDER BY timestamp DESC LIMIT 25"
    )
    
    # we fetch solana data :)
    sol_rows = session.execute(
        "SELECT crypto_id, symbol, price_usd, volume_24h, change_24h, timestamp "
        "FROM market_data WHERE crypto_id = 'solana' "
        "ORDER BY timestamp DESC LIMIT 25"
    )

    return render_template('index.html', doge=list(doge_rows), sol=list(sol_rows))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)