from flask import Flask, jsonify, request
import yfinance as yf
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Lista predeterminada de tickers
default_tickers = [
    "DUK", "TJX", "AVGO", "EME", "DASH", "AEM", "LOAR", "AAON", "META", "ANET",
    "BAC", "UNH", "GEV", "CEG", "VST", "CCJ", "NOV", "KKR", "AES",
    "MCO", "COR", "SNOW", "RGTI", "QBTS", "AAPL", "CELH", "DECK", "ARM", "GM",
    "CRWD", "INTU", "ANGO", "INTC", "FUTU", "SHOP", "AMC", "FMC", "TEAM", "OSCR",
    "ADBE", "PYPL", "NBIX", "ASTS", "ATS", "CRWD", "HIMS", "EW", "STT", "DEO", "MDB", "KO", "WU"
]

@app.route('/get_stock_data', methods=['POST'])
def get_stock_data():
    try:
        # Obtener tickers desde la solicitud JSON, o usar la lista predeterminada
        data = request.get_json()
        tickers = data.get('tickers', default_tickers)

        # Descargar datos para 10 días
        data = yf.download(tickers, period="10d", interval="1d", group_by="ticker", auto_adjust=True, prepost=False)

        # Procesar los datos
        rows = []
        for ticker in data.columns.levels[0]:
            if ticker in tickers:
                ticker_data = data[ticker]
                if not ticker_data.empty:
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
                        "averageVolume": volume_10d,
                        "averageVolume10days": volume_10d
                    }
                    rows.append(row)

        # Devolver respuesta JSON
        return jsonify({
            "status": "success",
            "data": rows
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
