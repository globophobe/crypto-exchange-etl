KRAKEN = "kraken"
KRAKEN_PAGINATION_ID = "KRAKEN_PAGINATION_ID"

API_URL = "https://api.kraken.com/0/public"
MAX_RESULTS = float("inf")
# Trade history increases counter by 2
# Counter reduced by 1 every 3 seconds
# Continuously, about 0.16 req/s
MIN_ELAPSED_PER_REQUEST = 6.0  # Slow AF
