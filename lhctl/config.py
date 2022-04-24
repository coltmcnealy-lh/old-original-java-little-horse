import os


API_URL_KEY = "LHORSE_API_URL"
DEFAULT_API_URL = os.getenv(
    API_URL_KEY,
    "http://localhost:5000"
)
