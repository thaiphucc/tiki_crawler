"""Category service for discovering book categories."""

import time
from typing import List, Dict, Optional

from core.session import create_session
from core.fetcher import Fetcher
from core.parser import Parser
from config.settings import CATEGORY_API_DELAY


class CategoryService:
    """
    Service for discovering and managing book categories from Tiki.

    Extracts book-related categories from Tiki's category sitemaps.
    """

    def __init__(self, session=None, proxy: str = None):
        """
        Initialize category service.

        Args:
            session: requests.Session to use (creates new if None)
            proxy: Proxy string for session creation
        """
        self.session = session or create_session(proxy)
        self.fetcher = Fetcher(self.session)
        self.parser = Parser()

        # Category sitemap URLs
        self.category_sitemaps = [
            "https://tiki.vn/clover/sitemap_categories_1.xml",
            "https://tiki.vn/clover/sitemap_categories_2.xml",
            "https://tiki.vn/clover/sitemap_categories_3.xml",
        ]

    def discover_categories(self) -> List[Dict]:
        """
        Discover book-related categories from Tiki sitemaps.

        Returns:
            List of category dicts with 'id', 'url', 'name' keys
        """
        all_categories = []
        seen_ids = set()

        for sitemap_url in self.category_sitemaps:
            content = self.fetcher.fetch(sitemap_url)

            if not content:
                continue

            categories = self.parser.extract_book_categories(content)

            for cat in categories:
                if cat['id'] not in seen_ids:
                    seen_ids.add(cat['id'])
                    all_categories.append(cat)

            time.sleep(CATEGORY_API_DELAY)

        return all_categories

    def get_category_products(
        self,
        category_id: str,
        page: int = 1,
        limit: int = 48
    ) -> Optional[Dict]:
        """
        Fetch products for a specific category.

        Args:
            category_id: Category ID
            page: Page number
            limit: Products per page

        Returns:
            Category products response dict
        """
        return self.fetcher.fetch_category_products(category_id, page, limit)

    def get_all_category_products(
        self,
        category_id: str,
        max_pages: int = None
    ) -> List[Dict]:
        """
        Fetch all products from a category across all pages.

        Args:
            category_id: Category ID
            max_pages: Maximum pages to fetch (None for all)

        Returns:
            List of product dicts
        """
        products = []

        first_page = self.get_category_products(category_id, page=1)

        if not first_page:
            return products

        # Extract pagination info
        pagination = self.parser.get_category_pagination(first_page)
        total_pages = pagination['last_page']

        if max_pages:
            total_pages = min(total_pages, max_pages)

        # Fetch first page products
        page_products = self.parser.extract_products_from_category(first_page)
        products.extend(page_products)

        # Fetch remaining pages
        for page in range(2, total_pages + 1):
            time.sleep(CATEGORY_API_DELAY)
            page_data = self.get_category_products(category_id, page=page)

            if page_data:
                page_products = self.parser.extract_products_from_category(page_data)
                products.extend(page_products)

        return products
