"""Product service for fetching and managing product data."""

import time
from typing import List, Dict, Optional

from core.session import create_session
from core.fetcher import Fetcher
from models.product import ProductExtractor
from config.settings import PRODUCT_API_DELAY


class ProductService:
    """
    Service for fetching product details from Tiki.

    Handles product details API calls with rate limiting.
    """

    def __init__(self, session=None, proxy: str = None):
        """
        Initialize product service.

        Args:
            session: requests.Session to use (creates new if None)
            proxy: Proxy string for session creation
        """
        self.session = session or create_session(proxy)
        self.fetcher = Fetcher(self.session)
        self.extractor = ProductExtractor()

    def get_product_details(self, product_id: str) -> Optional[Dict]:
        """
        Fetch product details by ID.

        Args:
            product_id: Product ID to fetch

        Returns:
            Product details dict, or None if failed
        """
        time.sleep(PRODUCT_API_DELAY)
        return self.fetcher.fetch_product(product_id)

    def get_products_details(
        self,
        product_ids: List[str],
        progress_callback=None
    ) -> List[Dict]:
        """
        Fetch details for multiple products.

        Args:
            product_ids: List of product IDs to fetch
            progress_callback: Optional callback(product_id, success) for progress updates

        Returns:
            List of extracted product dicts
        """
        results = []

        for i, product_id in enumerate(product_ids):
            product_data = self.get_product_details(product_id)

            if product_data:
                extracted = self.extractor.extract_from_details(product_data)
                results.append(extracted)

                if progress_callback:
                    progress_callback(product_id, True)
            else:
                if progress_callback:
                    progress_callback(product_id, False)

        return results

    def extract_from_category_data(self, product_data: Dict) -> Dict:
        """
        Extract fields from category API response.

        Args:
            product_data: Product data from category API

        Returns:
            Extracted fields dict
        """
        return self.extractor.extract_from_category(product_data)

    def extract_from_details_data(self, product_data: Dict) -> Dict:
        """
        Extract fields from product details API response.

        Args:
            product_data: Product data from details API

        Returns:
            Extracted fields dict
        """
        return self.extractor.extract_from_details(product_data)
