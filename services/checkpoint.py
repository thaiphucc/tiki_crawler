"""Checkpoint service for saving and resuming crawler state."""

import json
import os
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Optional, Set

from config.settings import CHECKPOINT_FILE


@dataclass
class CrawlerCheckpoint:
    """Checkpoint data structure for crawler state."""
    timestamp: str = ""
    categories: List[Dict] = field(default_factory=list)
    product_ids: List[str] = field(default_factory=list)
    processed_ids: Set[str] = field(default_factory=set)
    products_data: List[Dict] = field(default_factory=list)
    failed_ids: List[str] = field(default_factory=list)
    worker_states: Dict = field(default_factory=dict)
    progress: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        # Convert set to list for JSON
        data['processed_ids'] = list(self.processed_ids)
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'CrawlerCheckpoint':
        """Create from dictionary."""
        if 'processed_ids' in data and isinstance(data['processed_ids'], list):
            data['processed_ids'] = set(data['processed_ids'])
        return cls(**data)


class CheckpointManager:
    """
    Manages checkpoint save/load for crawler state.

    Features:
    - Auto-save at intervals
    - Resume detection
    - Incremental saves
    """

    def __init__(self, checkpoint_file: str = CHECKPOINT_FILE):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_file: Path to checkpoint JSON file
        """
        self.checkpoint_file = checkpoint_file

    def exists(self) -> bool:
        """Check if checkpoint file exists."""
        return os.path.exists(self.checkpoint_file)

    def save(self, checkpoint: CrawlerCheckpoint):
        """
        Save checkpoint to JSON file using atomic write (temp file + rename).

        Args:
            checkpoint: CrawlerCheckpoint to save
        """
        import tempfile
        import os

        checkpoint.timestamp = datetime.now().isoformat()
        data = checkpoint.to_dict()

        # Atomic write: write to temp file, then rename
        temp_fd, temp_path = tempfile.mkstemp(suffix='.json', prefix='checkpoint_')
        try:
            with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            # Atomic rename
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
            os.rename(temp_path, self.checkpoint_file)
        except Exception:
            # Clean up temp file on failure
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def load(self) -> Optional[CrawlerCheckpoint]:
        """
        Load checkpoint from JSON file.

        Returns:
            CrawlerCheckpoint if exists, None otherwise
        """
        if not self.exists():
            return None

        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return CrawlerCheckpoint.from_dict(data)
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    def delete(self):
        """Delete checkpoint file after successful completion."""
        if self.exists():
            os.remove(self.checkpoint_file)

    def ask_resume(self) -> Optional[bool]:
        """
        Ask user if they want to resume from checkpoint.

        Returns:
            True if user wants to resume, False otherwise, None if no checkpoint
        """
        if not self.exists():
            return None

        try:
            # Check if running interactively
            response = input("Found checkpoint. Resume from previous run? (Y/n): ").strip().lower()
            if response in ('n', 'no'):
                return False
            return True
        except EOFError:
            return True
