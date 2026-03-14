"""Worker pool for managing multiple workers."""

import threading
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, field

from workers.base import BaseWorker, WorkerStatus, WorkerState
from core.session import get_proxy_display
from config.settings import PROXIES, NO_PROXY, DEFAULT_WORKERS, PRODUCT_API_DELAY


@dataclass
class PoolState:
    """State of the worker pool."""
    total_workers: int = 0
    active_workers: int = 0
    total_products: int = 0
    processed_products: int = 0
    success_count: int = 0
    failed_count: int = 0
    is_running: bool = False
    start_time: float = 0.0
    worker_states: List[WorkerState] = field(default_factory=list)


class WorkerPool:
    """
    Manages multiple workers for parallel crawling.

    Each worker gets its own session with a proxy assigned in round-robin.
    """

    def __init__(
        self,
        num_workers: int = DEFAULT_WORKERS,
        proxies: List[str] = None,
        request_delay: float = None
    ):
        """
        Initialize worker pool.

        Args:
            num_workers: Number of workers to create
            proxies: List of proxy strings (round-robin assignment)
            request_delay: Delay between requests per worker
        """
        self.num_workers = num_workers
        self.proxies = proxies or PROXIES
        self.request_delay = request_delay if request_delay is not None else PRODUCT_API_DELAY

        self._workers: List[BaseWorker] = []
        self._state = PoolState(total_workers=num_workers)
        self._lock = threading.Lock()

        # Callback for global progress updates
        self._progress_callback: Optional[Callable] = None

        # Initialize workers with proxy assignment
        self._create_workers()

    def _create_workers(self):
        """Create workers with round-robin proxy assignment."""
        self._workers = []

        for i in range(self.num_workers):
            # Round-robin proxy assignment
            if self.proxies:
                proxy = self.proxies[i % len(self.proxies)]
            else:
                proxy = NO_PROXY

            worker = BaseWorker(
                worker_id=i + 1,
                proxy=proxy,
                request_delay=self.request_delay
            )

            # Set progress callback to aggregate to pool level
            worker.set_progress_callback(self._on_worker_progress)

            self._workers.append(worker)

        # Initialize worker states
        with self._lock:
            self._state.worker_states = [w.state for w in self._workers]

    def set_progress_callback(self, callback: Callable):
        """Set global progress callback."""
        self._progress_callback = callback

    def _on_worker_progress(
        self,
        worker_id: int,
        product_id: str,
        success: bool,
        data: Dict
    ):
        """Handle worker progress updates."""
        with self._lock:
            self._state.processed_products += 1
            if success:
                self._state.success_count += 1
            else:
                self._state.failed_count += 1

            # Update worker state
            if worker_id - 1 < len(self._workers):
                self._state.worker_states[worker_id - 1] = self._workers[worker_id - 1].state

        if self._progress_callback:
            self._progress_callback(worker_id, product_id, success, data)

    def start(self, product_ids: List[str]):
        """
        Start all workers with product IDs.

        Args:
            product_ids: List of all product IDs to process
        """
        import time

        if not product_ids:
            return

        with self._lock:
            self._state.is_running = True
            self._state.start_time = time.time()
            self._state.total_products = len(product_ids)
            self._state.processed_products = 0
            self._state.success_count = 0
            self._state.failed_count = 0

        # Split product IDs among workers
        chunk_size = len(product_ids) // self.num_workers
        if chunk_size == 0:
            chunk_size = 1

        for i, worker in enumerate(self._workers):
            start_idx = i * chunk_size
            if i == self.num_workers - 1:
                # Last worker gets remaining products
                worker_ids = product_ids[start_idx:]
            else:
                worker_ids = product_ids[start_idx:start_idx + chunk_size]

            if worker_ids:
                worker.start(worker_ids)

        with self._lock:
            self._state.active_workers = sum(
                1 for w in self._workers
                if w.state.status == WorkerStatus.RUNNING
            )

    def stop(self):
        """Stop all workers."""
        for worker in self._workers:
            worker.stop()

        with self._lock:
            self._state.is_running = False

    def wait(self):
        """Wait for all workers to complete."""
        for worker in self._workers:
            worker.wait()

        with self._lock:
            self._state.is_running = False
            self._state.active_workers = 0

    @property
    def state(self) -> PoolState:
        """Get current pool state."""
        with self._lock:
            # Update worker states
            self._state.worker_states = [w.state for w in self._workers]
            return self._state

    def is_running(self) -> bool:
        """Check if pool is running."""
        with self._lock:
            return self._state.is_running

    def get_worker_states(self) -> List[WorkerState]:
        """Get all worker states."""
        with self._lock:
            return [w.state for w in self._workers]

    def get_workers_info(self) -> List[Dict]:
        """Get worker information for display."""
        workers_info = []
        for worker in self._workers:
            state = worker.state
            workers_info.append({
                'id': state.worker_id,
                'proxy': state.proxy,
                'status': state.status.value,
                'category': state.category,
                'products_processed': state.products_processed,
                'products_total': state.products_total,
                'success_count': state.success_count,
                'failed_count': state.failed_count,
                'current_page': state.current_page,
                'total_pages': state.total_pages,
            })
        return workers_info
