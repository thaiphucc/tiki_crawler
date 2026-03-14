# Tiki Book Crawler

A multi-threaded web crawler for collecting book product data from Tiki.vn.


## Quick Start

```bash
# Step 1: Collect product IDs (run once)
python collect_ids.py

# Step 2: Fetch product details (may run many times if needed although it already automatically retries failed products)
python main.py
```

## Usage

### collect_ids.py - Step 1 + 2

Collects product IDs from all book categories. Run this once to discover categories and gather product IDs.

```bash
# Run all products
python collect_ids.py

# Limit to 100 products
python collect_ids.py --max-products 100

# Resume from checkpoint
python collect_ids.py --resume

# Start fresh 
python collect_ids.py --no-resume
```

### main.py - Step 3, 4, 5

Fetches full product details, auto-retries failures, and saves to CSV. Run multiple times until all products succeed.

```bash
# Run with defaults (8 workers)
python main.py

# Use 2 workers 
python main.py --workers 2

# Resume from checkpoint
python main.py --resume

# Skip auto-retry
python main.py --no-retry

# Save checkpoint every 50 products
python main.py --checkpoint-every 50
```

## Project Structure

```
tiki_crawler/
├── main.py                 # Main crawler (Step 3-5: fetch details, auto-retry, save)
├── collect_ids.py          # ID collector (Step 1-2: categories + product IDs)
├── config/
│   └── settings.py         # Configuration (URLs, delays, proxies)
├── core/
│   ├── session.py          # HTTP session management with proxy support
│   ├── fetcher.py          # HTTP requests with retry logic
│   └── parser.py           # XML/JSON parsing utilities
├── services/
│   ├── category_service.py # Category discovery from sitemap
│   ├── product_service.py  # Product data fetching
│   ├── exporter.py        # CSV export functionality
│   └── checkpoint.py      # Checkpoint save/load
├── workers/
│   ├── pool.py             # Worker pool manager
│   └── base.py             # Base worker implementation
├── models/
│   └── product.py          # Product data model and extraction
└── ui/
    └── status_display.py   # Rich console UI
```

## Output Files

| File | Description |
|------|-------------|
| `tiki_books.csv` | Final output with all product details |
| `tiki_books_partial.csv` | Backup (updated during crawl) |
| `crawler_checkpoint.json` | Resume state (deleted on success) |
| `tiki_crawler.log` | Error log for debugging |
