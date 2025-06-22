from flask import Flask, request, jsonify
import yfinance as yf
from datetime import datetime
import pandas as pd
import logging
import time

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def validate_ticker(ticker):
    """Valida si un ticker es válido consultando info básica"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        # Verificar si tiene datos básicos
        return 'symbol' in info or 'shortName' in info
    except:
        return False

def get_ticker_data_safely(ticker, retries=3, delay=1):
    """Obtiene datos de un ticker con reintentos"""
    for attempt in range(retries):
        try:
            logger.info(f"Attempting to fetch data for {ticker} (attempt {attempt + 1})")
            
            # Usar Ticker individual para mejor control de errores
            stock = yf.Ticker(ticker)
            
            # Intentar obtener datos históricos
            hist = stock.history(period="10d", interval="1d", auto_adjust=True, prepost=True)
            
            if hist.empty:
                logger.warning(f"No historical data for {ticker}")
                return None
                
            # Obtener información adicional si está disponible
            try:
                info = stock.info
                logger.info(f"Successfully retrieved info for {ticker}")
            except Exception as e:
                logger.warning(f"Could not get info for {ticker}: {str(e)}")
                info = {}
            
            return {
                'history': hist,
                'info': info
            }
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed for {ticker}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"All attempts failed for {ticker}")
                return None
    
    return None

@app.route("/get_stock_data", methods=["POST"])
def get_stock_data():
    try:
        content = request.get_json()

        if not content or "tickers" not in content:
            return jsonify({"error": "Missing 'tickers' in request body"}), 400

        tickers = content["tickers"]

        if not isinstance(tickers, list) or not all(isinstance(t, str) for t in tickers):
            return jsonify({"error": "'tickers' must be a list of strings"}), 400

        # Limpiar y validar tickers
        clean_tickers = [ticker.strip().upper() for ticker in tickers if ticker.strip()]
        
        if not clean_tickers:
            return jsonify({"error": "No valid tickers provided"}), 400

        logger.info(f"Processing tickers: {clean_tickers}")

        rows = []
        failed_tickers = []

        for ticker in clean_tickers:
            logger.info(f"Processing ticker: {ticker}")
            
            # Obtener datos del ticker
            ticker_data = get_ticker_data_safely(ticker)
            
            if ticker_data is None:
                failed_tickers.append(ticker)
                continue
                
            hist = ticker_data['history']
            info = ticker_data['info']
            
            if hist.empty:
                failed_tickers.append(ticker)
                continue

            try:
                # Obtener el último día de datos
                latest_day = hist.iloc[-1]
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Calcular promedio de volumen de 10 días
                volume_10d = hist["Volume"].mean() if len(hist) > 0 and "Volume" in hist.columns else 0
                
                # Obtener el cierre anterior
                previous_close = hist["Close"].shift(1).iloc[-1] if len(hist) > 1 else hist["Close"].iloc[-1]
                
                # Usar datos de info si están disponibles, sino usar datos históricos
                current_price = info.get('currentPrice') or info.get('regularMarketPrice') or latest_day.get("Close")
                
                row = {
                    "Symbol": ticker,
                    "Timestamp": timestamp,
                    "Open": float(latest_day["Open"]) if pd.notna(latest_day["Open"]) else None,
                    "dayLow": float(latest_day["Low"]) if pd.notna(latest_day["Low"]) else None,
                    "dayHigh": float(latest_day["High"]) if pd.notna(latest_day["High"]) else None,
                    "Close": float(current_price) if pd.notna(current_price) else None,
                    "PreviousClose": float(previous_close) if pd.notna(previous_close) else None,
                    "volume": int(latest_day["Volume"]) if pd.notna(latest_day["Volume"]) else 0,
                    "averageVolume": int(volume_10d) if pd.notna(volume_10d) else 0,
                    "companyName": info.get('shortName') or info.get('longName') or ticker
                }
                
                rows.append(row)
                logger.info(f"Successfully processed {ticker}")
                
            except Exception as e:
                logger.error(f"Error processing data for {ticker}: {str(e)}")
                failed_tickers.append(ticker)

        # Preparar respuesta
        response_data = {
            "status": "success" if rows else "partial_success" if failed_tickers else "error",
            "data": rows,
            "processed_count": len(rows),
            "requested_count": len(clean_tickers)
        }
        
        if failed_tickers:
            response_data["failed_tickers"] = failed_tickers
            response_data["message"] = f"Failed to retrieve data for: {', '.join(failed_tickers)}"

        if not rows:
            return jsonify({
                "status": "error", 
                "message": "No valid data found for any ticker",
                "failed_tickers": failed_tickers
            }), 404

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Endpoint para verificar que el servicio está funcionando"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

@app.route("/validate_ticker/<ticker>", methods=["GET"])
def validate_ticker_endpoint(ticker):
    """Endpoint para validar un ticker específico"""
    try:
        is_valid = validate_ticker(ticker.upper())
        return jsonify({
            "ticker": ticker.upper(),
            "is_valid": is_valid,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
