"""Base worker class for multi-worker crawling."""

import time
import threading
from enum import Enum
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field

from core.session import create_session, get_proxy_display
from core.fetcher import Fetcher
from models.product import ProductExtractor
from config.settings import PRODUCT_API_DELAY


class WorkerStatus(Enum):
    """Worker status states."""
    IDLE = "Idle"
    RUNNING = "Running"
    COMPLETED = "Completed"
    ERROR = "Error"
    STOPPED = "Stopped"


@dataclass
class WorkerState:
    """State of a worker."""
    worker_id: int
    status: WorkerStatus = WorkerStatus.IDLE
    proxy: str = ""
    category: str = ""
    products_processed: int = 0
    products_total: int = 0
    success_count: int = 0
    failed_count: int = 0
    current_page: int = 0
    total_pages: int = 0
    error_message: str = ""


class BaseWorker:
    """
    Base worker class for crawling products.

    Each worker has its own session with a dedicated proxy (or direct connection).
    """

    def __init__(
        self,
        worker_id: int,
        proxy: str = None,
        request_delay: float = PRODUCT_API_DELAY
    ):
        """
        Initialize worker.

        Args:
            worker_id: Unique worker identifier
            proxy: Proxy string (None for direct connection)
            request_delay: Delay between requests
        """
        self.worker_id = worker_id
        self.proxy = proxy or ""
        self.request_delay = request_delay

        # Create session with proxy
        self.session = create_session(self.proxy)
        self.fetcher = Fetcher(self.session)
        self.extractor = ProductExtractor()

        # Worker state
        self._state = WorkerState(
            worker_id=worker_id,
            proxy=get_proxy_display(proxy) if proxy else "Direct (No Proxy)"
        )

        # Thread control
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Callback for progress updates
        self._progress_callback: Optional[Callable] = None

    @property
    def state(self) -> WorkerState:
        """Get current worker state."""
        with self._lock:
            return self._state

    def set_progress_callback(self, callback: Callable):
        """Set callback for progress updates."""
        self._progress_callback = callback

    def _update_state(self, **kwargs):
        """Update worker state thread-safely."""
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self._state, key):
                    setattr(self._state, key, value)

    def start(self, product_ids: List[str]):
        """
        Start processing product IDs.

        Args:
            product_ids: List of product IDs to process
        """
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._update_state(
            products_total=len(product_ids),
            products_processed=0,
            success_count=0,
            failed_count=0,
            status=WorkerStatus.RUNNING
        )

        self._thread = threading.Thread(
            target=self._process,
            args=(product_ids,),
            daemon=True
        )
        self._thread.start()

    def stop(self):
        """Stop the worker."""
        self._stop_event.set()
        self._update_state(status=WorkerStatus.STOPPED)

    def wait(self):
        """Wait for worker to complete."""
        if self._thread:
            self._thread.join()

    def _process(self, product_ids: List[str]):
        """
        Process product IDs.

        Args:
            product_ids: List of product IDs to fetch
        """
        try:
            for i, product_id in enumerate(product_ids):
                if self._stop_event.is_set():
                    break

                # Fetch product details
                product_data = self.fetcher.fetch_product(product_id)

                if product_data and self.request_delay > 0:
                    time.sleep(self.request_delay)

                if product_data:
                    extracted = self.extractor.extract_from_details(product_data)

                    self._update_state(
                        products_processed=i + 1,
                        success_count=self._state.success_count + 1
                    )

                    if self._progress_callback:
                        self._progress_callback(self.worker_id, product_id, True, extracted)
                else:
                    self._update_state(
                        products_processed=i + 1,
                        failed_count=self._state.failed_count + 1
                    )

                    if self._progress_callback:
                        self._progress_callback(self.worker_id, product_id, False, None)

            if not self._stop_event.is_set():
                self._update_state(status=WorkerStatus.COMPLETED)
            else:
                self._update_state(status=WorkerStatus.STOPPED)

        except Exception as e:
            self._update_state(
                status=WorkerStatus.ERROR,
                error_message=str(e)
            )

    def set_category(self, category: str):
        """Set current category being processed."""
        self._update_state(category=category)

    def set_page(self, current: int, total: int):
        """Set current page info."""
        self._update_state(current_page=current, total_pages=total)
