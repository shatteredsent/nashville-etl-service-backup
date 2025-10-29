"""
Base extractor interface following Open/Closed Principle.
All document extractors must inherit from this class.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pathlib import Path


class BaseExtractor(ABC):
    """Abstract base class for all document extractors."""

    SUPPORTED_EXTENSIONS = set()

    def __init__(self, file_path: str, logger=None):
        """
        Initialize extractor with file path.

        Args:
            file_path: Path to document file
            logger: Optional logger instance

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file extension not supported
        """
        self.file_path = Path(file_path)
        self.logger = logger
        self._validate_file()

    def _validate_file(self) -> None:
        """Validate file exists and has correct extension."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if self.file_path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file extension: {self.file_path.suffix}. "
                f"Expected: {self.SUPPORTED_EXTENSIONS}"
            )

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract structured data from document.

        Returns:
            List of dictionaries containing extracted items

        Raises:
            Exception: If extraction fails
        """
        pass

    def _log(self, message: str, level: str = 'info') -> None:
        """Log message if logger available."""
        if self.logger:
            getattr(self.logger, level)(message)

    def _create_item(self, **kwargs) -> Dict[str, Any]:
        """
        Create standardized item dictionary.

        Returns:
            Dictionary with standardized keys
        """
        return {
            'source': 'document_upload',
            'name': kwargs.get('name'),
            'venue_name': kwargs.get('venue_name'),
            'venue_address': kwargs.get('venue_address'),
            'venue_city': kwargs.get('venue_city', 'Nashville'),
            'description': kwargs.get('description'),
            'url': kwargs.get('url'),
            'event_date': kwargs.get('event_date'),
            'category': kwargs.get('category', 'document_extracted'),
            'latitude': kwargs.get('latitude'),
            'longitude': kwargs.get('longitude'),
        }
