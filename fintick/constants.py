import httpx
import pandas as pd

FINTICK = "fintick"
FINTICK_API = "fintick-api"
FINTICK_AGGREGATE = "fintick-aggregate"
FINTICK_AGGREGATE_BARS = "fintick-aggregate-bars"

GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"
FIREBASE_ADMIN_CREDENTIALS = "FIREBASE_ADMIN_CREDENTIALS"
PROJECT_ID = "PROJECT_ID"
FIRESTORE_COLLECTIONS = "FIRESTORE_COLLECTIONS"
BIGQUERY_LOCATION = "BIGQUERY_LOCATION"
BIGQUERY_DATASET = "BIGQUERY_DATASET"
BIGQUERY_TABLES = "BIGQUERY_TABLES"


GCP_APPLICATION_CREDENTIALS = (
    GOOGLE_APPLICATION_CREDENTIALS,
    FIREBASE_ADMIN_CREDENTIALS,
)

BINANCE_API_KEY = "BINANCE_API_KEY"

PRODUCTION_ENV_VARS = (
    PROJECT_ID,
    BIGQUERY_LOCATION,
    BIGQUERY_DATASET,
    BINANCE_API_KEY,
)

BIGQUERY_HOT = pd.Timedelta("2d")
BIGQUERY_MAX_HOT = pd.Timedelta("6d")

HTTPX_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
)
