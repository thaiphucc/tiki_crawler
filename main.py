#!/usr/bin/env python3
"""
Tiki Book Crawler - Main Entry Point

Multi-module crawler with:
- Proxy support (user:pass@ip:port format)
- Multi-worker crawling with different IPs
- Live Rich console UI
- Checkpoint/resume functionality
- Auto-retry for failed products

Usage:
    Run collect_ids.py once to collect product IDs
    python collect_ids.py                   # Run with defaults
    python collect_ids.py --max-products 100 # Limit to 100 products

    # Then run main.py to fetch details (may run many times if needed although it already automatically retries failed products)
    python main.py                           # Run with defaults (8 workers)
    python main.py --workers 2               # Run with 2 workers
    python main.py --resume                  # Resume from checkpoint
    python main.py --no-retry               # Skip auto-retry
    python main.py --checkpoint-every 50    # Save checkpoint every 50 products

Steps:
    STEP 3: Fetch Product Details (8 workers)
    STEP 4: Auto-Retry Failed Products (loops until all succeed)
    STEP 5: Save Results to CSV
"""

import argparse
import logging
import re
import signal
import sys
import time
from typing import List, Dict, Set, Optional

from config.settings import (
    DEFAULT_WORKERS,
    MAX_WORKERS,
    CHECKPOINT_INTERVAL,
    OUTPUT_CSV,
    OUTPUT_CSV_PARTIAL,
    ERROR_LOG,
)
from core.session import create_session
from core.fetcher import Fetcher
from core.parser import Parser
from services.category_service import CategoryService
from services.product_service import ProductService
from services.exporter import Exporter
from services.checkpoint import CheckpointManager, CrawlerCheckpoint
from workers.pool import WorkerPool
from workers.base import WorkerStatus
from ui.status_display import StatusDisplay


class TikiCrawler:
    """
    Main crawler orchestrator.

    Manages the entire crawling process: category discovery,
    product collection, multi-worker fetching, and CSV export.
    """

    def __init__(
        self,
        num_workers: int = DEFAULT_WORKERS,
        max_products: int = None,
        checkpoint_interval: int = CHECKPOINT_INTERVAL,
        checkpoint_manager: CheckpointManager = None,
        resume: bool = None,
        skip_auto_retry: bool = False
    ):
        """
        Initialize crawler.

        Args:
            num_workers: Number of concurrent workers
            max_products: Maximum products to collect
            checkpoint_interval: Save checkpoint every N products
            checkpoint_manager: Checkpoint manager instance
            resume: Whether to resume from checkpoint (None = ask)
            skip_auto_retry: Skip auto-retry loop
        """
        self.num_workers = min(num_workers, MAX_WORKERS)
        self.max_products = max_products
        self.checkpoint_interval = checkpoint_interval
        self.skip_auto_retry = skip_auto_retry

        # Initialize components
        self.session = create_session()
        self.fetcher = Fetcher(self.session)
        self.parser = Parser()
        self.exporter = Exporter()
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.display = StatusDisplay()

        # Data storage
        self.categories: List[Dict] = []
        self.seen_product_ids: Set[str] = set()
        self.all_products_data: List[Dict] = []
        self.products_data: List[Dict] = []
        self.product_id_to_index: Dict[str, int] = {}
        self.failed_ids: List[str] = []

        # Worker pool
        self.worker_pool: Optional[WorkerPool] = None

        # Progress tracking
        self._products_collected: List[Dict] = []
        self._collected_lock = None  # Will be created in run()

        # Resume state
        self.resume_from_checkpoint = False
        self.checkpoint: Optional[CrawlerCheckpoint] = None

        # Logger
        self.logger = None

        # Check resume option
        if resume is None:
            resume = self.checkpoint_manager.ask_resume()
        self.resume_from_checkpoint = resume

        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Setup logging
        self._setup_logging()

    def _signal_handler(self, signum, frame):
        print("\n\nReceived interrupt signal. Saving checkpoint...")
        self._save_checkpoint()
        self.display.print_warning("Interrupted. Checkpoint saved.")
        sys.exit(0)

    def _setup_logging(self):
        self.logger = logging.getLogger('tiki_crawler')
        self.logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler('tiki_crawler.log', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

    def log_error(self, error_type: str, product_id: str = None, details: str = None) -> None:
        msg = f"{error_type}"
        if product_id:
            msg += f" - Product ID: {product_id}"
        if details:
            msg += f" - {details}"
        self.logger.error(msg)

    def extract_failed_from_log(self) -> Set[str]:
        """Extract unique failed product IDs from log file."""
        failed_ids: Set[str] = set()
        log_file = 'tiki_crawler.log'

        try:
            with open(log_file, encoding='utf-8') as f:
                for line in f:
                    match = re.search(r"FETCH_FAILED.*Product ID: (\d+)", line)
                    if match:
                        failed_ids.add(match.group(1))
        except FileNotFoundError:
            pass

        return failed_ids

    def _save_checkpoint(self):
        """Save current state to checkpoint."""
        checkpoint = CrawlerCheckpoint(
            categories=self.categories,
            product_ids=list(self.seen_product_ids),
            processed_ids=self.seen_product_ids,
            products_data=self._products_collected,
            failed_ids=self.failed_ids,
            progress={
                'total_products': len(self.seen_product_ids),
                'collected': len(self._products_collected),
                'failed': len(self.failed_ids),
            }
        )
        self.checkpoint_manager.save(checkpoint)
        self.exporter.save_partial(self._products_collected)

    def _load_checkpoint(self):
        """Load state from checkpoint."""
        self.checkpoint = self.checkpoint_manager.load()
        if self.checkpoint:
            self.categories = self.checkpoint.categories

            # Handle both cases:
            # 1. products_data exists (from previous main.py runs)
            # 2. only product_ids exists (from collect_ids.py)
            if self.checkpoint.products_data:
                self._products_collected = self.checkpoint.products_data
                # Build product_id_to_index from loaded data
                for idx, product in enumerate(self._products_collected):
                    prod_id = str(product.get('id', ''))
                    if prod_id:
                        self.product_id_to_index[prod_id] = idx
                self.seen_product_ids = set(self.product_id_to_index.keys())
                self.display.print_info(
                    f"Resumed with {len(self._products_collected)} products already collected"
                )
            elif self.checkpoint.product_ids:
                # From collect_ids.py - we have IDs but no products_data yet
                self.seen_product_ids = set(self.checkpoint.product_ids)
                self.display.print_info(
                    f"Resumed with {len(self.seen_product_ids)} product IDs from collect_ids.py"
                )

            self.failed_ids = self.checkpoint.failed_ids

    def discover_categories(self):
        """Discover book categories from Tiki."""
        self.display.print_header("STEP 1: Discovering Book Categories")

        category_service = CategoryService(session=self.session)
        self.categories = category_service.discover_categories()

        if not self.categories:
            self.display.print_error("No book categories found!")
            return False

        self.display.print_success(f"Found {len(self.categories)} book categories")
        self.display.print_categories(self.categories)
        return True

    def _has_checkpoint_data(self) -> Optional[CrawlerCheckpoint]:
        """Check if checkpoint has valid data to resume from and return it."""
        if not self.checkpoint_manager.exists():
            return None

        checkpoint = self.checkpoint_manager.load()
        # Accept checkpoint if it has either products_data OR product_ids
        if checkpoint and (len(checkpoint.products_data) > 0 or len(checkpoint.product_ids) > 0):
            return checkpoint
        return None

    def _get_products_needing_details(self, product_ids: List[str]) -> List[str]:
        """Get list of products that don't have full details yet."""
        needs_details = []
        for prod_id in product_ids:
            idx = self.product_id_to_index.get(prod_id)
            if idx is not None and idx < len(self._products_collected):
                product = self._products_collected[idx]
                # If doesn't have full details, needs fetching
                if not product.get('has_details', False):
                    needs_details.append(prod_id)
            else:
                # Not in collected list, needs fetching
                needs_details.append(prod_id)
        return needs_details

    def collect_products(self):
        """Collect all product IDs from categories."""
        self.display.print_header("STEP 2: Collecting Product Data from Categories")

        # Track processed category IDs to avoid duplicates
        processed_category_ids = set()

        # Create ProductService once for all categories
        ps = ProductService(session=self.session)

        for cat in self.categories:
            cat_id = cat['id']

            # Skip duplicate categories
            if cat_id in processed_category_ids:
                continue
            processed_category_ids.add(cat_id)

            cat_name = cat.get('name', cat_id)
            self.display.print_info(f"  Processing: {cat_name} (ID: {cat_id})")

            # Get first page to find total
            first_page = self.fetcher.fetch_category_products(cat_id, page=1)
            if not first_page:
                if hasattr(self, 'logger') and self.logger:
                    self.log_error("CATEGORY_FETCH_FAILED", cat_id, f"Failed to fetch category: {cat_name}")
                continue

            pagination = self.parser.get_category_pagination(first_page)
            total_pages = pagination['last_page']
            total_products = pagination['total']

            self.display.print_info(
                f"    Pages: {total_pages}, Products: {total_products}"
            )

            # Collect product IDs from all pages
            for page in range(1, total_pages + 1):
                if page > 1:
                    page_data = self.fetcher.fetch_category_products(cat_id, page=page)
                else:
                    page_data = first_page

                if not page_data:
                    continue

                products = self.parser.extract_products_from_category(page_data)

                for product in products:
                    prod_id = str(product.get('id', ''))
                    if prod_id and prod_id not in self.seen_product_ids:
                        self.seen_product_ids.add(prod_id)
                        # Extract partial data from category API
                        extracted = ps.extract_from_category_data(product)
                        self._products_collected.append(extracted)
                        self.product_id_to_index[prod_id] = len(self._products_collected) - 1

                        # Check max products
                        if self.max_products and len(self.seen_product_ids) >= self.max_products:
                            break

                if self.max_products and len(self.seen_product_ids) >= self.max_products:
                    break

        # Limit products
        if self.max_products:
            product_ids = list(self.seen_product_ids)[:self.max_products]
        else:
            product_ids = list(self.seen_product_ids)

        self.display.print_success(
            f"Total unique products: {len(product_ids)}"
        )

        return product_ids

    def fetch_product_details(self, product_ids: List[str]):
        """Fetch full details for products using worker pool."""
        self.display.print_header(
            f"STEP 3: Fetching Product Details ({self.num_workers} workers)"
        )

        # Create worker pool
        self.worker_pool = WorkerPool(
            num_workers=self.num_workers
        )

        # Start progress display
        self.display.start(len(product_ids))

        # Progress callback
        def progress_callback(worker_id, product_id, success, data):
            if success and data:
                # Update product with full details
                idx = self.product_id_to_index.get(product_id)
                if idx is not None:
                    self._products_collected[idx] = data

                # Checkpoint save
                if len(self._products_collected) % self.checkpoint_interval == 0:
                    self._save_checkpoint()
            else:
                # Log failed product fetch
                if hasattr(self, 'logger') and self.logger:
                    self.log_error("FETCH_FAILED", product_id, "Failed to fetch product details")

            self.display.update(self.worker_pool.state, self._products_collected)

        self.worker_pool.set_progress_callback(progress_callback)

        # Start workers
        self.worker_pool.start(product_ids)

        # Wait for completion
        self.worker_pool.wait()

        # Stop display
        self.display.stop()

        # Print summary
        pool_state = self.worker_pool.state
        self.display.print_summary(pool_state)
        self.display.print_worker_status(pool_state.worker_states)

    def auto_retry_failed(self, max_retries: int = 5):
        """
        Auto-retry loop: finds products without details, retries until all succeed or max retries.

        Instead of reading from log file (which accumulates ALL failures), we track which
        products currently don't have full details (has_details=True) and only retry those.

        Args:
            max_retries: Maximum number of retry cycles
        """
        retry_count = 0

        while retry_count < max_retries:
            # Find products that DON'T have details yet (these are current failures)
            products_with_details = set(
                str(p.get('id')) for p in self._products_collected
                if p.get('has_details', False)
            )

            current_failures = [
                pid for pid in self.seen_product_ids
                if pid not in products_with_details
            ]

            if not current_failures:
                self.display.print_success("All products have details!")
                return

            retry_count += 1
            self.display.print_header(f"STEP 4: Auto-Retry Failed Products (Round {retry_count})")
            self.display.print_info(f"Retrying {len(current_failures)} failed products...")

            # Fetch details for failed products
            self.fetch_product_details(current_failures)

            # Save checkpoint after retry to track progress
            self._save_checkpoint()

        self.display.print_warning(f"Max retries ({max_retries}) reached.")

    def save_results(self):
        """Save final results to CSV."""
        self.display.print_header("STEP 5: Saving Results")

        # Filter products with full details
        full_products = [p for p in self._products_collected if p.get('has_details', False)]

        # Save to CSV
        self.exporter.save_all(full_products)
        self.display.print_success(f"Saved {len(full_products)} products to {OUTPUT_CSV}")

        # Save partial backup
        self.exporter.save_partial(self._products_collected)
        self.display.print_info(f"Partial backup saved to {OUTPUT_CSV_PARTIAL}")

        # Delete checkpoint on success
        self.checkpoint_manager.delete()

    def run(self):
        """Run the complete crawling process."""
        self.display.print_header("TIKI BOOK CRAWLER")
        self.display.print_info(f"Workers: {self.num_workers}")
        if self.max_products:
            self.display.print_info(f"Max Products: {self.max_products}")

        # Auto-detect checkpoint
        checkpoint = self._has_checkpoint_data()

        if checkpoint and self.resume_from_checkpoint is not False:
            # Load checkpoint automatically
            self.resume_from_checkpoint = True

        if self.resume_from_checkpoint and checkpoint:
            self.display.print_info("Mode: Resume from checkpoint")
            self._load_checkpoint()

            # Show what we have based on what's loaded
            if self._products_collected:
                products_with_details = sum(1 for p in self._products_collected if p.get('has_details', False))
                self.display.print_info(f"Loaded {len(self._products_collected)} products from checkpoint")
                self.display.print_info(f"  - With full details: {products_with_details}")
                self.display.print_info(f"  - Need details fetch: {len(self._products_collected) - products_with_details}")
            else:
                self.display.print_info(f"Loaded {len(self.seen_product_ids)} product IDs from checkpoint")

        start_time = time.time()

        try:
            # Step 1: Discover categories (always needed for structure)
            if not self.categories:
                if not self.discover_categories():
                    return

            # Step 2: Collect products (skip if resuming with products)
            if self.resume_from_checkpoint and checkpoint:
                # Skip category collection, just get product IDs
                product_ids = list(self.seen_product_ids)
                if self.max_products:
                    product_ids = product_ids[:self.max_products]
                self.display.print_info(f"Loaded {len(product_ids)} product IDs from checkpoint")
            else:
                product_ids = self.collect_products()

            if not product_ids:
                self.display.print_error("No products to process!")
                return

            # Step 3: Fetch full details (only for products needing it)
            if self.resume_from_checkpoint and checkpoint:
                # Only fetch details for products that don't have them
                product_ids = self._get_products_needing_details(product_ids)
                self.display.print_info(f"Fetching details for {len(product_ids)} products (skipping {len(self._products_collected) - len(product_ids)} already done)")

            if product_ids:
                self.fetch_product_details(product_ids)

            # Step 4: Auto-retry failed products
            if not self.skip_auto_retry:
                self.auto_retry_failed()

            # Step 5: Save results
            self.save_results()

            # Final stats
            elapsed = time.time() - start_time
            self.display.print_success(
                f"\nTotal time: {time.strftime('%H:%M:%S', time.gmtime(elapsed))}"
            )

        except Exception as e:
            self.display.print_error(f"Error: {e}")
            # Save checkpoint on error
            self._save_checkpoint()
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Crawl book products from Tiki.vn",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                           Run with defaults
  python main.py --workers 2                Use 2 workers
  python main.py --max-products 100         Limit to 100 products
  python main.py --resume                   Resume from checkpoint
  python main.py --no-resume                 Start fresh
  python main.py --checkpoint-every 50      Checkpoint every 50 products
        """
    )

    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of workers (default: {DEFAULT_WORKERS}, max: {MAX_WORKERS})"
    )

    parser.add_argument(
        "--max-products", "-m",
        type=int,
        default=None,
        help="Maximum number of products to crawl (default: unlimited)"
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        default=None,
        help="Resume from checkpoint (default: ask)"
    )

    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore existing checkpoint"
    )

    parser.add_argument(
        "--checkpoint-every", "-c",
        type=int,
        default=CHECKPOINT_INTERVAL,
        help=f"Save checkpoint every N products (default: {CHECKPOINT_INTERVAL})"
    )

    parser.add_argument(
        "--no-retry",
        action="store_true",
        help="Skip auto-retry of failed products"
    )

    args = parser.parse_args()

    # Handle resume flag
    resume = None
    if args.no_resume:
        resume = False
    elif args.resume:
        resume = True

    # Create and run crawler
    crawler = TikiCrawler(
        num_workers=args.workers,
        max_products=args.max_products,
        checkpoint_interval=args.checkpoint_every,
        resume=resume,
        skip_auto_retry=args.no_retry
    )

    crawler.run()


if __name__ == "__main__":
    main()
