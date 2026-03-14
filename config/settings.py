"""Settings and configuration for Tiki crawler."""

TIKI_SITEMAP_INDEX = "https://tiki.vn/clover/sitemap_categories_index.xml"
TIKI_CATEGORY_API = "https://tiki.vn/api/personalish/v1/blocks/listings"
TIKI_PRODUCT_API = "https://tiki.vn/api/v2/products"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Referer": "https://tiki.vn/",
    "Connection": "keep-alive",
}

NO_PROXY = ""

PROXIES = [
    "wihveu:kruydrva@74.81.32.203:3119",
    NO_PROXY,
    "wihveu:kruydrva@74.81.32.203:3119",
    NO_PROXY,
]

DEFAULT_WORKERS = 8
MAX_WORKERS = 16

CATEGORY_API_DELAY = 0.1
PRODUCT_API_DELAY = 0.2
REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.5

RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_WAIT = 2
RETRY_RATE_LIMIT_WAIT = 3
RETRY_JITTER = 0.5

PROGRESS_LOG_INTERVAL = 50
CHECKPOINT_INTERVAL = 500

BATCH_SIZE = 500
BATCH_PAUSE = 60
BATCH_PROGRESS_LOG = 5

OUTPUT_CSV = "tiki_books.csv"
OUTPUT_CSV_PARTIAL = "tiki_books_partial.csv"
ERROR_LOG = "failed_requests.txt"
CHECKPOINT_FILE = "crawler_checkpoint.json"
