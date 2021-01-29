BINANCE = "binance"

API_URL = "https://api.binance.com/api/v3"
MAX_RESULTS = 1000
# Endpoint historicalTrades weight is 5
# Response 429, when x-mbx-used-weight-1m about 1200
# Per minute, 1200 / 5 = 240 req/m
# Per second, 240 / 60.0 = 4 req/s
MIN_ELAPSED_PER_REQUEST = 1 / 4.0
