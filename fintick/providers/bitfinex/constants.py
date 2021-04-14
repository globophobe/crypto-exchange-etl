BITFINEX = "bitfinex"

API_URL = "https://api-pub.bitfinex.com/v2/trades"
MAX_RESULTS = 10000
# 90 requests per minute, so 1.5 requests per second
MIN_ELAPSED_PER_REQUEST = 1 / 1.5
