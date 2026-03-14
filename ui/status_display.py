"""Live status display using Rich library."""

import time
from typing import List, Dict, Optional
from dataclasses import dataclass

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn
from rich.live import Live
from rich.text import Text
from rich.style import Style

from workers.base import WorkerState
from workers.pool import PoolState


class StatusDisplay:
    """
    Live status display using Rich library.

    Shows real-time progress of all workers with a nice console UI.
    """

    def __init__(self):
        """Initialize status display."""
        self.console = Console()
        self._progress = None
        self._live = None
        self._start_time = None

    def start(self, total_products: int):
        """
        Start the live display.

        Args:
            total_products: Total number of products to process
        """
        self._start_time = time.time()

        # Create progress bar
        self._progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )

        self._progress.start()
        self._task_id = self._progress.add_task(
            "[cyan]Overall Progress",
            total=total_products
        )

    def update(
        self,
        pool_state: PoolState,
        products_collected: List[Dict] = None
    ):
        """
        Update display with current pool state.

        Args:
            pool_state: Current state of worker pool
            products_collected: List of collected products
        """
        if not self._progress:
            return

        # Update overall progress
        self._progress.update(
            self._task_id,
            completed=pool_state.processed_products,
            total=pool_state.total_products
        )

    def update_worker(
        self,
        worker_id: int,
        product_id: str,
        success: bool,
        data: Dict
    ):
        """
        Update progress for a single worker.

        Args:
            worker_id: Worker ID
            product_id: Product ID processed
            success: Whether fetch was successful
            data: Product data if successful
        """
        pass  # Progress bar handles this

    def stop(self):
        """Stop the live display."""
        if self._progress:
            self._progress.stop()
            self._progress = None

    def print_summary(self, pool_state: PoolState):
        """
        Print final summary.

        Args:
            pool_state: Final state of worker pool
        """
        elapsed = time.time() - self._start_time if self._start_time else 0

        self.console.print("\n")
        self.console.print(Panel.fit(
            "[bold green]CRAWL COMPLETE[/bold green]",
            border_style="green"
        ))

        # Summary table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Products", str(pool_state.total_products))
        table.add_row("Successful", str(pool_state.success_count))
        table.add_row("Failed", str(pool_state.failed_count))
        success_rate = (pool_state.success_count / pool_state.total_products * 100
                       if pool_state.total_products > 0 else 0)
        table.add_row("Success Rate", f"{success_rate:.1f}%")
        table.add_row("Elapsed Time", time.strftime("%H:%M:%S", time.gmtime(elapsed)))

        self.console.print(table)

    def print_worker_status(self, worker_states: List[WorkerState]):
        """
        Print detailed worker status.

        Args:
            worker_states: List of worker states
        """
        table = Table(title="Worker Status", show_header=True)
        table.add_column("Worker", style="cyan")
        table.add_column("Proxy", style="dim")
        table.add_column("Status", style="green")
        table.add_column("Processed", style="yellow")
        table.add_column("Success", style="green")
        table.add_column("Failed", style="red")

        for state in worker_states:
            status_style = "green" if state.status.value == "Running" else "yellow"
            table.add_row(
                f"Worker {state.worker_id}",
                state.proxy[:20] + "..." if len(state.proxy) > 20 else state.proxy,
                f"[{status_style}]{state.status.value}[/{status_style}]",
                f"{state.products_processed}/{state.products_total}",
                str(state.success_count),
                str(state.failed_count)
            )

        self.console.print(table)

    def print_categories(self, categories: List[Dict]):
        """
        Print discovered categories.

        Args:
            categories: List of category dicts
        """
        table = Table(title="Discovered Categories", show_header=True)
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="green")

        for cat in categories[:20]:  # Show first 20
            table.add_row(cat.get('id', ''), cat.get('name', ''))

        if len(categories) > 20:
            table.add_row("...", f"... and {len(categories) - 20} more")

        self.console.print(table)

    def print_info(self, message: str, style: str = "cyan"):
        """
        Print info message.

        Args:
            message: Message to print
            style: Rich style color
        """
        self.console.print(f"[{style}]{message}[/{style}]")

    def print_error(self, message: str):
        """Print error message."""
        self.console.print(f"[bold red]ERROR:[/bold red] {message}")

    def print_warning(self, message: str):
        """Print warning message."""
        self.console.print(f"[bold yellow]WARNING:[/bold yellow] {message}")

    def print_success(self, message: str):
        """Print success message."""
        self.console.print(f"[bold green]SUCCESS:[/bold green] {message}")

    def print_header(self, title: str):
        """
        Print header.

        Args:
            title: Header title
        """
        self.console.print(f"\n[bold cyan]{'=' * 50}[/bold cyan]")
        self.console.print(f"[bold cyan]{title:^50}[/bold cyan]")
        self.console.print(f"[bold cyan]{'=' * 50}[/bold cyan]\n")


class SimpleStatusDisplay(StatusDisplay):
    """
    Simplified status display without Live (for compatibility).

    Uses basic print updates instead of Rich Live rendering.
    """

    def __init__(self):
        """Initialize simple display."""
        super().__init__()
        self._last_update = 0
        self._update_interval = 2.0  # Update every 2 seconds

    def update(
        self,
        pool_state: PoolState,
        products_collected: List[Dict] = None
    ):
        """
        Update display (throttled).

        Args:
            pool_state: Current state of worker pool
            products_collected: List of collected products
        """
        current_time = time.time()

        # Throttle updates
        if current_time - self._last_update < self._update_interval:
            return

        self._last_update = current_time
        elapsed = time.time() - self._start_time if self._start_time else 0

        # Print progress line
        pct = (pool_state.processed_products / pool_state.total_products * 100
               if pool_state.total_products > 0 else 0)

        self.console.print(
            f"\rProgress: {pool_state.processed_products}/{pool_state.total_products} "
            f"({pct:.1f}%) | "
            f"Success: {pool_state.success_count} | "
            f"Failed: {pool_state.failed_count} | "
            f"Elapsed: {time.strftime('%H:%M:%S', time.gmtime(elapsed))}",
            end=""
        )
