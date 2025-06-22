
from flask import Flask, request, jsonify
import yfinance as yf
from datetime import datetime
import pandas as pd
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route("/get_stock_data", methods=["POST"])
def get_stock_data():
    try:
        content = request.get_json()

        if not content or "tickers" not in content:
            return jsonify({"error": "Missing 'tickers' in request body"}), 400

        tickers = content["tickers"]

        if not isinstance(tickers, list) or not all(isinstance(t, str) for t in tickers):
            return jsonify({"error": "'tickers' must be a list of strings"}), 400

        # Intentar descargar datos con manejo de errores
        try:
            data = yf.download(tickers, period="10d", interval="1d", group_by="ticker", auto_adjust=True, prepost=True)
        except Exception as e:
            logger.error(f"Failed to download data: {str(e)}")
            return jsonify({"status": "error", "message": "Failed to fetch data from yfinance"}), 500

        if data.empty:
            logger.warning(f"No data retrieved for tickers: {tickers}")
            return jsonify({"status": "error", "message": "No data available for the provided tickers"}), 404

        rows = []
        for ticker in tickers:
            if ticker in data.columns.levels[0]:
                ticker_data = data[ticker].dropna()
                if not ticker_data.empty:
                    latest_day = ticker_data.iloc[-1]
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    volume_10d = ticker_data["Volume"].mean() if "Volume" in ticker_data else 0
                    previous_close = ticker_data["Close"].shift(1).iloc[-1] if "Close" in ticker_data and not pd.isna(ticker_data["Close"].shift(1).iloc[-1]) else None
                    row = {
                        "Symbol": ticker,
                        "Timestamp": timestamp,
                        "Open": latest_day["Open"] if "Open" in latest_day else None,
                        "dayLow": latest_day["Low"] if "Low" in latest_day else None,
                        "dayHigh": latest_day["High"] if "High" in latest_day else None,
                        "Close": latest_day["Close"] if "Close" in latest_day else None,
                        "PreviousClose": previous_close,
                        "volume": latest_day["Volume"] if "Volume" in latest_day else 0,
                        "averageVolume": volume_10d
                    }
                    rows.append(row)
                else:
                    logger.warning(f"No data for ticker: {ticker}")
            else:
                logger.warning(f"Ticker {ticker} not found in downloaded data")

        if not rows:
            return jsonify({"status": "error", "message": "No valid data found for any ticker"}), 404

        return jsonify({"status": "success", "data": rows})

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
