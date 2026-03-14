#!/usr/bin/env python3
"""
- Collect all product IDs from categories
- Save checkpoint with product IDs only

Usage:
    python collect_ids.py                    # Run with defaults
    python collect_ids.py --max-products 100 # Limit to 100 products
    python collect_ids.py --resume           # Resume from checkpoint
    python collect_ids.py --no-resume         # Start fresh
"""

import argparse
import logging
import signal
import sys
import time
from typing import List, Dict, Set, Optional

from config.settings import (
    CHECKPOINT_INTERVAL,
)
from core.session import create_session
from core.fetcher import Fetcher
from core.parser import Parser
from services.category_service import CategoryService
from services.product_service import ProductService
from services.checkpoint import CheckpointManager, CrawlerCheckpoint
from ui.status_display import StatusDisplay


class IDCollector:
    """
    Collects product IDs from Tiki categories.

    Runs Step 1 (discover categories) + Step 2 (collect products).
    Saves checkpoint with product IDs only.
    """

    def __init__(
        self,
        max_products: int = None,
        checkpoint_manager: CheckpointManager = None,
        resume: bool = None
    ):
        """
        Initialize ID collector.

        Args:
            max_products: Maximum products to collect
            checkpoint_manager: Checkpoint manager instance
            resume: Whether to resume from checkpoint (None = ask)
        """
        self.max_products = max_products

        # Initialize components
        self.session = create_session()
        self.fetcher = Fetcher(self.session)
        self.parser = Parser()
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.display = StatusDisplay()

        # Data storage
        self.categories: List[Dict] = []
        self.seen_product_ids: Set[str] = set()
        self._products_collected: List[Dict] = []
        self.product_id_to_index: Dict[str, int] = {}

        # Progress tracking
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

    def _save_checkpoint(self):
        """Save current state to checkpoint."""
        checkpoint = CrawlerCheckpoint(
            categories=self.categories,
            product_ids=list(self.seen_product_ids),
            processed_ids=self.seen_product_ids,
            products_data=self._products_collected,
            failed_ids=[],
            progress={
                'total_products': len(self.seen_product_ids),
                'collected': len(self._products_collected),
            }
        )
        self.checkpoint_manager.save(checkpoint)

    def _load_checkpoint(self):
        """Load state from checkpoint."""
        self.checkpoint = self.checkpoint_manager.load()
        if self.checkpoint:
            self.categories = self.checkpoint.categories
            self.seen_product_ids = set(self.checkpoint.product_ids)
            self._products_collected = self.checkpoint.products_data
            # Build product_id_to_index
            for idx, product in enumerate(self._products_collected):
                prod_id = str(product.get('id', ''))
                if prod_id:
                    self.product_id_to_index[prod_id] = idx
            self.display.print_info(
                f"Resumed with {len(self._products_collected)} products already collected"
            )

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
                    self.logger.error(f"CATEGORY_FETCH_FAILED - Category ID: {cat_id} - Failed to fetch category: {cat_name}")
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

                        # Check max products - return early if reached
                        if self.max_products and len(self.seen_product_ids) >= self.max_products:
                            # Limit collected products to max_products
                            self._products_collected = self._products_collected[:self.max_products]
                            product_ids = list(self.seen_product_ids)[:self.max_products]
                            self.display.print_success(
                                f"Total unique products: {len(product_ids)}"
                            )
                            return product_ids

                if self.max_products and len(self.seen_product_ids) >= self.max_products:
                    # Limit collected products to max_products
                    self._products_collected = self._products_collected[:self.max_products]
                    product_ids = list(self.seen_product_ids)[:self.max_products]
                    self.display.print_success(
                        f"Total unique products: {len(product_ids)}"
                    )
                    return product_ids

        # Limit products
        if self.max_products:
            product_ids = list(self.seen_product_ids)[:self.max_products]
            self._products_collected = self._products_collected[:self.max_products]
        else:
            product_ids = list(self.seen_product_ids)

        self.display.print_success(
            f"Total unique products: {len(product_ids)}"
        )

        return product_ids

    def run(self):
        """Run the complete ID collection process."""
        self.display.print_header("TIKI ID COLLECTOR")
        self.display.print_info("Collecting product IDs (Step 1 + Step 2)")
        if self.max_products:
            self.display.print_info(f"Max Products: {self.max_products}")

        start_time = time.time()

        try:
            # Check for checkpoint
            if self.resume_from_checkpoint and self.checkpoint_manager.exists():
                self._load_checkpoint()

                # Show what we have
                self.display.print_info(f"Loaded {len(self._products_collected)} product IDs from checkpoint")

            # Step 1: Discover categories (always needed for structure)
            if not self.categories:
                if not self.discover_categories():
                    return

                # Save checkpoint after discovering categories
                self._save_checkpoint()

            # Step 2: Collect products
            if self.resume_from_checkpoint and self.checkpoint and self.checkpoint.product_ids:
                # Skip collection, just use loaded IDs
                product_ids = list(self.seen_product_ids)
                if self.max_products:
                    product_ids = product_ids[:self.max_products]
                self.display.print_info(f"Using {len(product_ids)} product IDs from checkpoint")
            else:
                product_ids = self.collect_products()
                # Save checkpoint after collecting products
                self._save_checkpoint()

            if not product_ids:
                self.display.print_error("No products to process!")
                return

            # Final checkpoint save
            self._save_checkpoint()

            # Print completion message
            elapsed = time.time() - start_time
            self.display.print_success(
                f"\nID Collection Complete!"
            )
            self.display.print_info(f"Total product IDs: {len(product_ids)}")
            self.display.print_info(f"Time: {time.strftime('%H:%M:%S', time.gmtime(elapsed))}")
            self.display.print_info("\nNext step: Run main.py to fetch product details")

        except Exception as e:
            self.display.print_error(f"Error: {e}")
            # Save checkpoint on error
            self._save_checkpoint()
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Collect product IDs from Tiki.vn (Steps 1 + 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python collect_ids.py                    Run with defaults
  python collect_ids.py --max-products 100 Limit to 100 products
  python collect_ids.py --resume           Resume from checkpoint
  python collect_ids.py --no-resume        Start fresh

Note: This script collects product IDs only. Run main.py to fetch details.
        """
    )

    parser.add_argument(
        "--max-products", "-m",
        type=int,
        default=None,
        help="Maximum number of products to collect (default: unlimited)"
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

    args = parser.parse_args()

    # Handle resume flag
    resume = None
    if args.no_resume:
        resume = False
    elif args.resume:
        resume = True

    # Create and run collector
    collector = IDCollector(
        max_products=args.max_products,
        resume=resume
    )

    collector.run()


if __name__ == "__main__":
    main()
