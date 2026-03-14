"""Sitemap and API parsing utilities."""

import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional

from config.settings import TIKI_SITEMAP_INDEX


class Parser:
    """
    Parser for Tiki sitemaps and API responses.

    Handles XML sitemap parsing and extracts category/product data
    from Tiki's APIs.
    """

    # Pre-compiled regex patterns 
    _XML_NAMESPACE_RE = re.compile(r'xmlns[^=]*="[^"]*"')
    _XML_NAMESPACE_PREFIXED_RE = re.compile(r'xmlns:[a-z]+="[^"]*"')

    # Book keywords for filtering category URLs
    BOOK_KEYWORDS = [
        "architecture",
        "graphic-design",
        "religion-culture",
        "photography",
        "decorative-arts-design",
        "other-arts",
        "drawing-painting",
        "fashion",
        "history-criticism",
        "sculpture",
        "music",
        "performing-arts",
        "business-leaders",
        "celebrities-famous-people",
        "family-childhood",
        "historical-political-military",
        "autobiography-memoirs",
        "travelers-explorers",
        "arts-literature",
        "true-crime",
        "sports-outdoor",
        "survival",
        "finance",
        "economics",
        "investment",
        "job-career",
        "management-leadership",
        "marketing-sales-advertising",
        "personal-finance",
        "processes-infrastructure",
        "skills",
        "business-and-entrepreneurship",
        "communication-social-skills",
        "creativity",
        "emotions",
        "motivational",
        "personal-improvement",
        "relationship",
        "religion-spirituality",
        "stress-management",
        "success",
        "anxieties-phobias",
        "art-therapy-relaxation",
        "happiness",
        "baby-toddler",
        "colouring-sticker-activity",
        "early-learning",
        "education-reference",
        "picture-storybooks",
        "science-nature-how-it-works",
        "growing-up-facts-of-life",
        "history-geography-cultures",
        "literature-fiction",
        "series-books",
        "english-language-dictionary",
        "bilingual-dictionary",
        "childrens-school-dictionary",
        "teaching-learning",
        "elts",
        "grammar-vocabulary-skills",
        "graded-readers",
        "test-preparation",
        "childrens-action-adventure",
        "classic",
        "contemporary",
        "dramas-plays",
        "historical-fiction",
        "humor-satire",
        "literary",
        "mystery-thriller-suspense",
        "mythology-folk-tales",
        "romance",
        "short-stories-anthologies",
        "poetry",
        "history-criticism",
        "comics-graphic-novels",
        "womens-fiction",
        "science-fiction-fantasy",
        "ancient-medieval-literature",
        "world-literature",
        "magazines",
        "basic-medical-sciences",
        "medical-research",
        "medicine",
        "nursing",
        "marriage-adult-relationships",
        "babysitting-day-care-child-care",
        "parenting",
        "family-health-relationship",
        "crafts-hobbies",
        "health-fitness-sports",
        "home-living",
        "psychology",
        "professions",
        "atlases-encyclopedia",
        "gardening",
        "foreign-language-reference",
        "jobs-careers",
        "quotation-reference-books",
        "astronomy-space-science",
        "agriculture-forestry",
        "environment",
        "mathematics",
        "nature-ecology",
        "human-biology-science",
        "internet-computers",
        "others",
        "chemistry",
        "physics",
        "anthropology",
        "philosophy",
        "politics-current-affairs",
        "social-sciences",
        "sociology",
        "history",
        "cooking",
        "europe",
        "vietnam",
        "guidebook-series",
        "discovery-exploration",
        "drinks-beverages",
        "ingredients-methods-appliances",
        "regional-international",
        "baking-desserts",
        "vegetarian-vegan",
        "beverages-wine",
        "cooking-education-reference",
        "du-ky", "light-novel", "phe-binh-ly-luan-van-hoc", "phong-su-ky-su",
        "tac-pham-kinh-dien", "tho", "tieu-su-hoi-ky", "tieu-thuyet",
        "tranh-truyen", "truyen-co-tich-ngu-ngon", "truyen-cuoi", "truyen-dai",
        "truyen-dam-my", "truyen-gia-tuong-huyen-bi-phieu-luu",
        "kiem-hiep-vo-hiep", "truyen-kinh-di", "truyen-ngan-tan-van-tap-van",
        "ngon-tinh", "trinh-tham", "ca-dao-tuc-ngu-thanh-ngu", "sach-anh",
        "bai-hoc-kinh-doanh", "sach-doanh-nhan", "sach-khoi-nghiep",
        "sach-kinh-te-hoc", "bao-hiem", "chinh-sach-kinh-te-phat-trien",
        "kinh-te-vi-mo", "sach-kinh-te-hoc-khac", "thue", "tien-ky-thuat-so",
        "sach-ky-nang-lam-viec", "sach-marketing-ban-hang",
        "sach-quan-tri-lanh-dao", "sach-quan-tri-nhan-luc", "sach-tai-chinh",
        "ke-toan", "sach-tai-chinh-khac", "dau-tu", "huy-dong-von",
        "ky-thuat-tai-chinh", "quan-ly-rui-ro-tai-chinh", "quan-ly-tai-san",
        "tai-chinh-ca-nhan", "tai-chinh-cong", "tai-chinh-doanh-nghiep",
        "bai-hoc-dao-duc", "kien-thuc-ky-nang", "to-mau-luyen-chu",
        "truyen-co-tich", "truyen-ke-cho-be", "ehon-nhat-ban",
        "truyen-tranh-thieu-nhi", "van-hoc-thieu-nhi",
        "sach-chuyen-dong-tuong-tac", "sach-ky-nang-song", "sach-huong-nghiep",
        "sach-ky-nang-mem", "sach-tam-ly-hoc", "sach-thai-giao",
        "sach-ky-nang-cho-be", "cam-nang-lam-cha-me",
        "sach-dinh-duong-suc-khoe-cho-be", "giao-duc-tuoi-teen",
        "phuong-phap-giao-duc-tre", "sach-giao-khoa-cap-1", "sach-giao-khoa-cap-2",
        "sach-giao-khoa-cap-3", "gia-trinh-dai-hoc-cao-dang",
        "sach-hoc-tieng-anh", "sach-hoc-tieng-nhat", "sach-hoc-tieng-han",
        "sach-hoc-tieng-hoa", "ngoai-ngu-khac", "sach-tham-khao-cap-i",
        "sach-tham-khao-cap-ii", "sach-tham-khao-cap-iii",
        "sach-luyen-thi-dai-hoc-cao-dang", "the-luyen-thi", "sach-chuyen-de",
        "tu-dien-tieng-anh", "tu-dien-tieng-viet", "tu-dien-tieng-nhat",
        "tu-dien-tieng-han", "tu-dien-tieng-trung", "tu-dien-tieng-phap",
        "tu-dien-ngon-ngu-khac", "kien-thuc-bach-khoa", "linh-vuc-khac",
        "sach-chiem-tinh-horoscope", "sach-giao-duc", "sach-phong-thuy-kinh-dich",
        "triet-hoc", "sach-khoa-hoc", "sach-ky-thuat", "lich-su-viet-nam",
        "lich-su-the-gioi", "truyen-tranh", "phap-luat", "ly-luan-chinh-tri",
        "nong-lam-ngu-nghiep", "tap-chi-catalogue", "am-nhac",
        "my-thuat-kien-truc", "sach-to-mau-danh-cho-nguoi-lon",
        "cong-giao", "phat-giao", "ton-giao-tam-linh-khac", "sach-danh-nhan",
        "sach-dia-danh-du-lich", "sach-phong-tuc-tap-quan", "tin-hoc-van-phong",
        "lap-trinh", "thiet-ke-do-hoa", "sach-y-hoc-co-truyen",
        "sach-y-hoc-hien-dai", "sach-y-hoc-khac", "sach-hon-nhan-gioi-tinh",
        "sach-tam-ly-tuoi-teen", "cham-soc-suc-khoe", "may-theu-thoi-trang",
        "sach-lam-dep", "sach-day-nau-an", "chay-bo", "doi-khang",
        "the-duc-the-thao-khac", "the-thao-tri-tue", "yoga"
    ]

    @staticmethod
    def parse_sitemap_urls(content: str) -> List[str]:
        """
        Parse XML sitemap and extract all URL loc entries.

        Args:
            content: XML content string

        Returns:
            List of URLs found in sitemap
        """
        if not content:
            return []

        urls = []
        try:
            # Handle XML namespace
            content = Parser._XML_NAMESPACE_RE.sub('', content)
            content = Parser._XML_NAMESPACE_PREFIXED_RE.sub('', content)

            root = ET.fromstring(content)
            tag_name = root.tag.replace('{http://www.sitemaps.org/schemas/sitemap/0.9}', '')

            if tag_name == 'sitemapindex':
                for sitemap in root.findall('.//sitemap'):
                    loc = sitemap.find('loc')
                    if loc is not None and loc.text:
                        urls.append(loc.text)
            elif tag_name == 'urlset':
                for url_elem in root.findall('.//url'):
                    loc = url_elem.find('loc')
                    if loc is not None and loc.text:
                        urls.append(loc.text)

        except Exception:
            pass

        return urls

    @staticmethod
    def extract_book_categories(content: str) -> List[Dict]:
        """
        Extract book-related category IDs from category sitemap content.

        Args:
            content: XML sitemap content

        Returns:
            List of dicts with 'id', 'url', 'name' keys
        """
        if not content:
            return []

        categories = []
        seen_ids = set()

        try:
            # Clean XML namespaces
            content = Parser._XML_NAMESPACE_RE.sub('', content)
            content = Parser._XML_NAMESPACE_PREFIXED_RE.sub('', content)

            root = ET.fromstring(content)
            tag_name = root.tag.replace('{http://www.sitemaps.org/schemas/sitemap/0.9}', '')

            if tag_name == 'urlset':
                for url_elem in root.findall('.//url'):
                    loc = url_elem.find('loc')
                    if loc is None or not loc.text:
                        continue

                    url = loc.text.lower()

                    # Check if it's a book category
                    is_book = any(f"/{pattern}/c" in url for pattern in Parser.BOOK_KEYWORDS)

                    if is_book:
                        match = re.search(r'/c(\d+)', loc.text)
                        if match:
                            cat_id = match.group(1)
                            if cat_id not in seen_ids:
                                seen_ids.add(cat_id)
                                categories.append({
                                    'id': cat_id,
                                    'url': loc.text,
                                    'name': Parser.extract_category_name(loc.text)
                                })

        except Exception:
            pass

        return categories

    @staticmethod
    def extract_category_name(url: str) -> str:
        """
        Extract category name from URL path.

        URL format: https://tiki.vn/sach-kinh-doanh/c2549

        Args:
            url: Category URL

        Returns:
            Extracted category name
        """
        parts = url.split('/')
        for idx, part in enumerate(parts):
            if part.startswith('c') and part[1:].isdigit():
                if idx > 0:
                    return parts[idx - 1].replace('-', ' ').title()
        return parts[-2].replace('-', ' ').title() if len(parts) > 1 else 'Unknown'

    @staticmethod
    def get_category_pagination(data: Dict) -> Dict:
        """
        Extract pagination info from category API response.

        Args:
            data: Category API response dict

        Returns:
            Dict with 'last_page', 'total', 'current_page' keys
        """
        paging = data.get('paging', {})
        return {
            'last_page': paging.get('last_page', 1),
            'total': paging.get('total', 0),
            'current_page': paging.get('current_page', 1)
        }

    @staticmethod
    def extract_products_from_category(data: Dict) -> List[Dict]:
        """
        Extract product list from category API response.

        Args:
            data: Category API response dict

        Returns:
            List of product dicts
        """
        return data.get('data', [])
