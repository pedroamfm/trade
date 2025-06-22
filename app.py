from flask import Flask, request, jsonify
import yfinance as yf
from datetime import datetime
import pandas as pd

app = Flask(__name__)

@app.route("/get_stock_data", methods=["POST"])
def get_stock_data():
    try:
        # Leer el JSON enviado en el body
        content = request.get_json()

        if not content or "tickers" not in content:
            return jsonify({"error": "Missing 'tickers' in request body"}), 400

        tickers = content["tickers"]

        if not isinstance(tickers, list) or not all(isinstance(t, str) for t in tickers):
            return jsonify({"error": "'tickers' must be a list of strings"}), 400

        # Descargar datos
        data = yf.download(tickers, period="10d", interval="1d", group_by="ticker", auto_adjust=True, prepost=True)

        rows = []
        for ticker in tickers:
            if ticker in data.columns.levels[0]:
                ticker_data = data[ticker]
                if ticker_data.empty:
                    latest_day = ticker_data.iloc[0]
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    volume_10d = ticker_data["Volume"].mean()
                    previous_close = ticker_data["Close"].shift(1).iloc[0] if not pd.isna(ticker_data["Close"].shift(1).iloc[0]) else None
                    row = {
                        "Symbol": ticker,
                        "Timestamp": timestamp,
                        "Open": latest_day["Open"],
                        "dayLow": latest_day["Low"],
                        "dayHigh": latest_day["High"],
                        "Close": latest_day["Close"],
                        "PreviousClose": previous_close,
                        "volume": latest_day["Volume"],
                        "averageVolume": volume_10d
                    }
                    rows.append(row)

        return jsonify({"status": "success", "data": rows})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)

