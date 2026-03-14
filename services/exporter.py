"""CSV export service for saving product data."""

import csv
import threading
from typing import List, Dict

from models.product import ProductExtractor
from config.settings import OUTPUT_CSV, OUTPUT_CSV_PARTIAL


class Exporter:
    """
    Service for exporting product data to CSV files.

    Supports incremental saves for checkpoint functionality.
    """

    def __init__(self, output_file: str = OUTPUT_CSV, partial_file: str = OUTPUT_CSV_PARTIAL):
        """
        Initialize exporter.

        Args:
            output_file: Main output CSV file path
            partial_file: Partial/backup CSV file path
        """
        self.output_file = output_file
        self.partial_file = partial_file
        self.fieldnames = ProductExtractor.get_csv_fieldnames()
        self._lock = threading.Lock()

    def save(self, products: List[Dict], filepath: str = None, mode: str = 'w'):
        """
        Save products to CSV file.

        Args:
            products: List of product dicts
            filepath: Override output file path
            mode: Write mode ('w' for overwrite, 'a' for append)
        """
        target = filepath or self.output_file

        with self._lock:
            with open(target, mode, encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames, extrasaction='ignore')

                if mode == 'w':
                    writer.writeheader()

                for product in products:
                    cleaned = {k: ('' if v is None else v) for k, v in product.items()}
                    writer.writerow(cleaned)

    def save_partial(self, products: List[Dict]):
        """Save products to partial CSV file."""
        self.save(products, filepath=self.partial_file, mode='w')

    def append(self, products: List[Dict]):
        """Append products to output CSV."""
        self.save(products, mode='a')

    def append_unique(self, products: List[Dict]):
        """
        Append products to output CSV, skipping duplicates.

        Reads existing product IDs from the CSV and only adds products
        that are not already present.

        Args:
            products: List of product dicts to potentially add
        """
        # Get existing product IDs
        existing_ids = self._get_existing_ids()

        # Filter out duplicates
        new_products = [p for p in products if str(p.get('id', '')) not in existing_ids]

        if new_products:
            self.save(new_products, mode='a')
            # print(f"Added {len(new_products)} new products (skipped {len(products) - len(new_products)} duplicates)")
        else:
            pass 

    def _get_existing_ids(self) -> set:
        """Get set of existing product IDs from output CSV."""
        existing_ids = set()
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_ids.add(row.get('id', ''))
        except FileNotFoundError:
            pass
        return existing_ids

    def save_all(self, products: List[Dict]):
        """Save all products to output CSV (overwrite)."""
        self.save(products, mode='w')
