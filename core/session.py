"""Session management with proxy support."""

import requests
from requests.adapters import HTTPAdapter
from urllib.parse import urlparse
from typing import Optional

from config.settings import HEADERS, REQUEST_TIMEOUT


def parse_proxy(proxy_string: str) -> dict:
    """
    Parse proxy string in format 'user:pass@ip:port'.

    Returns dict with user, pass, ip, port keys.
    """
    if not proxy_string or "@" not in proxy_string:
        return {"user": "", "pass": "", "ip": "", "port": ""}

    try:
        auth, host = proxy_string.split("@")
        user, password = auth.split(":")
        ip, port = host.split(":")

        return {
            "user": user,
            "pass": password,
            "ip": ip,
            "port": port
        }
    except (ValueError, AttributeError):
        return {"user": "", "pass": "", "ip": "", "port": ""}


def create_session(proxy: str = None) -> requests.Session:
    """
    Create a requests session with proper headers and optional proxy.

    Args:
        proxy: Proxy string in format 'user:pass@ip:port', or None/empty for direct.

    Returns:
        Configured requests.Session object.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    session.max_redirects = 5  # Limit redirects 

    # Add connection pooling
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=0  
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    if proxy:
        parsed = parse_proxy(proxy)

        if parsed["user"] and parsed["pass"]:
            proxy_url = f"http://{parsed['user']}:{parsed['pass']}@{parsed['ip']}:{parsed['port']}"
        else:
            proxy_url = f"http://{parsed['ip']}:{parsed['port']}"

        session.proxies = {
            'http': proxy_url,
            'https': proxy_url
        }

    return session


def get_proxy_display(proxy: str) -> str:
    """Get a display-friendly version of the proxy (mask password)."""
    if not proxy:
        return "Direct (No Proxy)"

    parsed = parse_proxy(proxy)
    if parsed["pass"]:
        return f"{parsed['ip']}:{parsed['port']} (****)"
    return f"{parsed['ip']}:{parsed['port']}"
