
from flask import Flask, request, jsonify
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import logging
import time
from functools import lru_cache
import threading

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Control de rate limiting
class RateLimiter:
    def __init__(self, max_requests=10, time_window=60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = threading.Lock()

    def can_make_request(self):
        with self.lock:
            now = datetime.now()
            # Limpiar requests antiguos
            self.requests = [req_time for req_time in self.requests
                           if (now - req_time).seconds < self.time_window]

            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    def wait_time(self):
        with self.lock:
            if not self.requests:
                return 0
            oldest_request = min(self.requests)
            return max(0, self.time_window - (datetime.now() - oldest_request).seconds)

# Instancia global del rate limiter
rate_limiter = RateLimiter(max_requests=5, time_window=60)

# Cache simple para datos de tickers
ticker_cache = {}
CACHE_DURATION = 300  # 5 minutos

def get_cached_data(ticker):
    """Obtiene datos del cache si están frescos"""
    if ticker in ticker_cache:
        cached_time, data = ticker_cache[ticker]
        if (datetime.now() - cached_time).seconds < CACHE_DURATION:
            logger.info(f"Using cached data for {ticker}")
            return data
    return None

def cache_data(ticker, data):
    """Guarda datos en cache"""
    ticker_cache[ticker] = (datetime.now(), data)

def validate_ticker_with_delay(ticker):
    """Valida ticker con control de rate limiting"""
    # Verificar cache primero
    cached = get_cached_data(f"validate_{ticker}")
    if cached is not None:
        return cached

    if not rate_limiter.can_make_request():
        wait_time = rate_limiter.wait_time()
        logger.warning(f"Rate limit reached, waiting {wait_time} seconds")
        time.sleep(wait_time + 1)

    try:
        stock = yf.Ticker(ticker)
        # Usar un método más ligero para validación
        info = stock.fast_info  # Más rápido que .info
        is_valid = hasattr(info, 'last_price') or hasattr(info, 'previous_close')

        # Cachear resultado
        cache_data(f"validate_{ticker}", is_valid)

        # Pequeña pausa para evitar rate limiting
        time.sleep(0.5)

        return is_valid
    except Exception as e:
        logger.error(f"Error validating ticker {ticker}: {str(e)}")
        return False

def get_ticker_data_with_backoff(ticker, max_retries=3):
    """Obtiene datos con backoff exponencial"""
    # Verificar cache primero
    cached = get_cached_data(ticker)
    if cached is not None:
        return cached

    for attempt in range(max_retries):
        if not rate_limiter.can_make_request():
            wait_time = rate_limiter.wait_time()
            logger.warning(f"Rate limit reached, waiting {wait_time} seconds")
            time.sleep(wait_time + 1)

        try:
            logger.info(f"Fetching data for {ticker} (attempt {attempt + 1})")

            stock = yf.Ticker(ticker)

            # Obtener datos históricos con período más corto para reducir carga
            hist = stock.history(period="5d", interval="1d")

            if hist.empty:
                logger.warning(f"No data for {ticker}")
                backoff_time = (2 ** attempt)
                time.sleep(backoff_time)
                continue

            # Intentar obtener fast_info en lugar de info completo
            try:
                fast_info = stock.fast_info
                additional_info = {
                    'current_price': getattr(fast_info, 'last_price', None),
                    'previous_close': getattr(fast_info, 'previous_close', None),
                    'market_cap': getattr(fast_info, 'market_cap', None),
                    'shares': getattr(fast_info, 'shares', None)
                }
            except:
                additional_info = {}

            result = {
                'history': hist,
                'info': additional_info
            }

            # Cachear resultado exitoso
            cache_data(ticker, result)

            # Pausa para evitar rate limiting
            time.sleep(1)

            return result

        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed for {ticker}: {str(e)}")
            if "429" in str(e) or "Too Many Requests" in str(e):
                backoff_time = (3 ** attempt) + 5  # Backoff más agresivo para 429
                logger.warning(f"Rate limited, backing off for {backoff_time} seconds")
                time.sleep(backoff_time)
            else:
                backoff_time = (2 ** attempt)
                time.sleep(backoff_time)

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

        # Limpiar tickers
        clean_tickers = [ticker.strip().upper() for ticker in tickers if ticker.strip()]

        if not clean_tickers:
            return jsonify({"error": "No valid tickers provided"}), 400

        # Limitar número de tickers por request para evitar rate limiting
        if len(clean_tickers) > 5:
            return jsonify({"error": "Maximum 5 tickers per request to avoid rate limiting"}), 400

        logger.info(f"Processing tickers: {clean_tickers}")

        rows = []
        failed_tickers = []

        for i, ticker in enumerate(clean_tickers):
            logger.info(f"Processing ticker {i+1}/{len(clean_tickers)}: {ticker}")

            # Pausa entre tickers para evitar rate limiting
            if i > 0:
                time.sleep(2)

            ticker_data = get_ticker_data_with_backoff(ticker)

            if ticker_data is None:
                failed_tickers.append(ticker)
                continue

            hist = ticker_data['history']
            info = ticker_data['info']

            if hist.empty:
                failed_tickers.append(ticker)
                continue

            try:
                latest_day = hist.iloc[-1]
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                volume_avg = hist["Volume"].mean() if len(hist) > 0 else 0
                previous_close = hist["Close"].shift(1).iloc[-1] if len(hist) > 1 else hist["Close"].iloc[-1]

                current_price = info.get('current_price') or latest_day.get("Close")

                row = {
                    "Symbol": ticker,
                    "Timestamp": timestamp,
                    "Open": float(latest_day["Open"]) if pd.notna(latest_day["Open"]) else None,
                    "dayLow": float(latest_day["Low"]) if pd.notna(latest_day["Low"]) else None,
                    "dayHigh": float(latest_day["High"]) if pd.notna(latest_day["High"]) else None,
                    "Close": float(current_price) if pd.notna(current_price) else None,
                    "PreviousClose": float(previous_close) if pd.notna(previous_close) else None,
                    "volume": int(latest_day["Volume"]) if pd.notna(latest_day["Volume"]) else 0,
                    "averageVolume": int(volume_avg) if pd.notna(volume_avg) else 0,
                    "cached": ticker in [k for k in ticker_cache.keys() if not k.startswith('validate_')]
                }

                rows.append(row)
                logger.info(f"Successfully processed {ticker}")

            except Exception as e:
                logger.error(f"Error processing data for {ticker}: {str(e)}")
                failed_tickers.append(ticker)

        response_data = {
            "status": "success" if rows else "partial_success" if failed_tickers else "error",
            "data": rows,
            "processed_count": len(rows),
            "requested_count": len(clean_tickers),
            "cache_info": f"{len(ticker_cache)} items cached",
            "rate_limit_info": f"{len(rate_limiter.requests)} requests in last minute"
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
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "cache_size": len(ticker_cache),
        "rate_limit_requests": len(rate_limiter.requests)
    })

@app.route("/validate_ticker/<ticker>", methods=["GET"])
def validate_ticker_endpoint(ticker):
    try:
        is_valid = validate_ticker_with_delay(ticker.upper())
        return jsonify({
            "ticker": ticker.upper(),
            "is_valid": is_valid,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "cached": f"validate_{ticker.upper()}" in ticker_cache
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/clear_cache", methods=["POST"])
def clear_cache():
    global ticker_cache
    cache_size = len(ticker_cache)
    ticker_cache.clear()
    return jsonify({
        "message": f"Cache cleared, removed {cache_size} items",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
