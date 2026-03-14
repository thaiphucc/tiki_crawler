"""Product data models and extraction."""

import re
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

# Pre-compiled regex for HTML tag removal 
_HTML_TAG_RE = re.compile(r'<[^>]+>')


@dataclass
class Product:
    """Product data model for Tiki books."""

    id: str = ""
    name: str = ""
    price: float = 0.0
    original_price: float = 0.0
    discount_rate: int = 0
    url: str = ""
    author: str = ""
    publisher: str = ""
    isbn: str = ""
    cover_format: str = ""
    page_count: str = ""
    language: str = ""
    publication_date: str = ""
    dimensions: str = ""
    rating_average: float = 0.0
    review_count: int = 0
    like_count: int = 0
    inventory_quantity: int = 0
    thumbnail_url: str = ""
    image_urls: str = ""
    categories: str = ""
    primary_category: str = ""
    short_description: str = ""
    meta_title: str = ""
    meta_description: str = ""
    created_at: str = ""
    updated_at: str = ""
    brand_name: str = ""
    seller_name: str = ""
    seller_id: str = ""
    current_spid: str = ""
    url_path: str = ""
    has_details: bool = False

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'Product':
        """Create Product from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class ProductExtractor:
    """
    Extracts product fields from Tiki API responses.

    Handles both category API (partial data) and product details API (full data).
    """

    @staticmethod
    def safe_get(d: Dict, *keys, default=None):
        """Safely get nested dictionary values."""
        result = d
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key, default)
            else:
                return default
        return result

    @staticmethod
    def extract_spec_attributes(product_data: Dict) -> Dict:
        """Extract specifications attributes from product data."""
        specifications = product_data.get('specifications', [])
        spec_attributes = {}
        if isinstance(specifications, list):
            for spec_group in specifications:
                if isinstance(spec_group, dict) and 'attributes' in spec_group:
                    for attr in spec_group.get('attributes', []):
                        if isinstance(attr, dict):
                            spec_attributes[attr.get('code', '')] = attr.get('value', '')
        return spec_attributes

    @staticmethod
    def extract_from_category(product_data: Dict) -> Dict:
        """
        Extract fields from category API response (90% of needed data).

        Args:
            product_data: Product data from category API

        Returns:
            Extracted fields dict
        """
        product_id = product_data.get('id', '')
        url_path = product_data.get('url_path', '')
        product_url = f"https://tiki.vn/{url_path}-p{product_id}.html" if url_path else f"https://tiki.vn/product-p{product_id}.html"

        extracted = {
            'id': product_id,
            'name': product_data.get('name'),
            'price': product_data.get('price'),
            'original_price': product_data.get('original_price'),
            'discount_rate': product_data.get('discount_rate'),
            'url': product_url,
            # Authors
            'author': ', '.join([
                a.get('name', '') for a in product_data.get('authors', [])
                if isinstance(a, dict)
            ]) if isinstance(product_data.get('authors'), list) else str(product_data.get('authors', '')),
            'publisher': '',
            'isbn': '',
            'cover_format': '',
            'page_count': '',
            'language': '',
            'publication_date': '',
            'dimensions': '',
            'rating_average': product_data.get('rating_average'),
            'review_count': product_data.get('review_count'),
            'like_count': product_data.get('like_count'),
            'inventory_quantity': ProductExtractor.safe_get(
                product_data, 'stock_item', 'qty', default=0
            ),
            'thumbnail_url': ProductExtractor.safe_get(
                product_data, 'thumbnail_url', default=''
            ),
            'image_urls': '',
            'categories': '',
            'primary_category': ProductExtractor.safe_get(
                product_data, 'categories', 'name', default=''
            ),
            'short_description': ProductExtractor._clean_html(
                product_data.get('short_description', '')
            ),
            'meta_title': '',
            'meta_description': '',
            'created_at': '',
            'updated_at': '',
            'brand_name': ProductExtractor.safe_get(
                product_data, 'brand', 'name', default=''
            ),
            'seller_name': ProductExtractor.safe_get(
                product_data, 'current_seller', 'name', default=''
            ),
            'seller_id': ProductExtractor.safe_get(
                product_data, 'current_seller', 'id', default=''
            ),
            'current_spid': product_data.get('spid'),
            'url_path': url_path,
            'has_details': False
        }

        return extracted

    @staticmethod
    def extract_from_details(product_data: Dict) -> Dict:
        """
        Extract fields from product details API response (full data).

        Args:
            product_data: Product data from details API

        Returns:
            Extracted fields dict
        """
        spec_attributes = ProductExtractor.extract_spec_attributes(product_data)

        product_id = product_data.get('id', '')
        url_path = product_data.get('url_path', '')
        product_url = f"https://tiki.vn/{url_path}-p{product_id}.html" if url_path else f"https://tiki.vn/product-p{product_id}.html"

        extracted = {
            'id': product_id,
            'name': product_data.get('name'),
            'price': product_data.get('price'),
            'original_price': product_data.get('original_price'),
            'discount_rate': product_data.get('discount_rate'),
            'url': product_url,
            'author': ', '.join([
                a.get('name', '') for a in product_data.get('authors', [])
                if isinstance(a, dict)
            ]) if isinstance(product_data.get('authors'), list) else str(product_data.get('authors', '')),
            'publisher': spec_attributes.get('publisher_vn', ''),
            'isbn': spec_attributes.get('isbn', ''),
            'cover_format': spec_attributes.get('book_cover', ''),
            'page_count': spec_attributes.get('number_of_pages', ''),
            'language': spec_attributes.get('language', ''),
            'publication_date': spec_attributes.get('publication_date', ''),
            'dimensions': spec_attributes.get('dimensions', ''),
            'rating_average': product_data.get('rating_average'),
            'review_count': product_data.get('review_count'),
            'like_count': product_data.get('like_count'),
            'inventory_quantity': ProductExtractor.safe_get(
                product_data, 'stock_item', 'qty', default=0
            ),
            'thumbnail_url': ProductExtractor.safe_get(
                product_data, 'thumbnail_url', default=''
            ),
            'image_urls': '|'.join([
                img.get('base_url', '') for img in product_data.get('images', [])[:5]
                if isinstance(img, dict)
            ]) if product_data.get('images') else '',
            'categories': ' > '.join([
                c.get('name', '') for c in product_data.get('breadcrumbs', [])
                if isinstance(c, dict)
            ]) if product_data.get('breadcrumbs') else '',
            'primary_category': ProductExtractor.safe_get(
                product_data, 'categories', 'name', default=''
            ),
            'short_description': ProductExtractor._clean_html(
                product_data.get('short_description', '')
            ),
            'meta_title': product_data.get('meta_title', ''),
            'meta_description': product_data.get('meta_description', ''),
            'created_at': str(product_data.get('created_at', '')),
            'updated_at': str(product_data.get('updated_at', '')),
            'brand_name': ProductExtractor.safe_get(
                product_data, 'brand', 'name', default=''
            ),
            'seller_name': ProductExtractor.safe_get(
                product_data, 'current_seller', 'name', default=''
            ),
            'seller_id': ProductExtractor.safe_get(
                product_data, 'current_seller', 'id', default=''
            ),
            'current_spid': product_data.get('spid'),
            'url_path': url_path,
            'has_details': True
        }

        return extracted

    @staticmethod
    def _clean_html(text: str, max_length: int = 500) -> str:
        """Remove HTML tags from text."""
        if not text:
            return ''
        return _HTML_TAG_RE.sub('', text)[:max_length]

    @staticmethod
    def get_csv_fieldnames() -> List[str]:
        """Define CSV column order."""
        return [
            'id', 'name', 'price', 'original_price', 'discount_rate',
            'author', 'publisher', 'isbn', 'cover_format', 'page_count', 'language',
            'publication_date', 'dimensions',
            'rating_average', 'review_count', 'like_count',
            'inventory_quantity',
            'thumbnail_url', 'image_urls',
            'categories', 'primary_category',
            'short_description',
            'meta_title', 'meta_description',
            'created_at', 'updated_at',
            'brand_name', 'seller_name', 'seller_id',
            'current_spid', 'url_path',
            'url'
        ]
