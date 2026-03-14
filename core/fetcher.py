"""URL fetching with retry logic and error handling."""

import json
import time
import random
from typing import Optional, Dict
import requests

from config.settings import REQUEST_TIMEOUT, REQUEST_DELAY, RETRY_MAX_ATTEMPTS, RETRY_BASE_WAIT, RETRY_RATE_LIMIT_WAIT, RETRY_JITTER


class Fetcher:
    """
    Handles HTTP requests to Tiki API with retry logic.

    Supports rate limiting (429) handling with exponential backoff
    and JSON decode error recovery.
    """

    def __init__(self, session: requests.Session, max_retries: int = 3):
        """
        Initialize fetcher with a session.

        Args:
            session: requests.Session to use for requests
            max_retries: Maximum retry attempts for failed requests
        """
        self.session = session
        self.max_retries = max_retries

    def fetch(self, url: str, params: Dict = None, max_retries: int = None) -> Optional[str]:
        """
        Fetch URL content with error handling and retry logic.

        Args:
            url: URL to fetch
            params: Query parameters
            max_retries: Override default max_retries

        Returns:
            Response text content, or None if all retries failed.
        """
        retries = max_retries if max_retries is not None else self.max_retries

        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    if attempt < retries - 1:
                        base_wait = (2 ** attempt) * RETRY_RATE_LIMIT_WAIT
                        jitter = RETRY_JITTER + random.random()
                        wait_time = base_wait * jitter
                        time.sleep(wait_time)
                        continue
                    else:
                        return None

                response.raise_for_status()
                return response.text

            except requests.exceptions.HTTPError as e:
                # Only apply long delays for rate limiting (429)
                if e.response is not None and e.response.status_code == 429:
                    if attempt < retries - 1:
                        wait_time = (2 ** attempt) * RETRY_BASE_WAIT
                        time.sleep(wait_time)
                        continue
                # For other HTTP errors, retry after short delay
                elif attempt < retries - 1:
                    time.sleep(0.5) 
                    continue
                return None

            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    time.sleep((2 ** attempt) * RETRY_BASE_WAIT)
                    continue
                return None

            except requests.exceptions.RequestException:
                if attempt < retries - 1:
                    time.sleep((2 ** attempt) * RETRY_BASE_WAIT)
                    continue
                return None

            # JSON decode errors
            except json.JSONDecodeError:
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) * RETRY_BASE_WAIT
                    time.sleep(wait_time)
                    continue
                return None

        return None

    def fetch_json(self, url: str, params: Dict = None) -> Optional[Dict]:
        """
        Fetch URL and parse JSON response.

        Args:
            url: URL to fetch
            params: Query parameters

        Returns:
            Parsed JSON dict, or None if failed.
        """
        content = self.fetch(url, params)
        if content:
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass
        return None

    def fetch_product(self, product_id: str, base_url: str = "https://tiki.vn/api/v2/products") -> Optional[Dict]:
        """
        Fetch product details by ID.

        Args:
            product_id: Product ID to fetch
            base_url: Product API base URL

        Returns:
            Product data dict, or None if failed.
        """
        url = f"{base_url}/{product_id}?platform=web"
        return self.fetch_json(url)

    def fetch_category_products(
        self,
        category_id: str,
        page: int = 1,
        limit: int = 48,
        base_url: str = "https://tiki.vn/api/personalish/v1/blocks/listings"
    ) -> Optional[Dict]:
        """
        Fetch products for a category.

        Args:
            category_id: Category ID
            page: Page number
            limit: Products per page
            base_url: Category API base URL

        Returns:
            Category products response dict, or None if failed.
        """
        params = {
            'category': category_id,
            'page': page,
            'limit': limit,
            'urlKey': f'c{category_id}'
        }
        return self.fetch_json(base_url, params)
