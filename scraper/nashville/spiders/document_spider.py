"""
Unified document spider for processing multiple file types.
Delegates to specialized extractors based on file extension.
"""
import os
from pathlib import Path
from typing import List, Dict, Any
import scrapy
from scraper.nashville.items import BusinessItem
from scraper.nashville.extractors import (
    PDFExtractor,
    CSVExtractor,
    ExcelExtractor,
    WordExtractor,
)


class DocumentSpider(scrapy.Spider):
    """
    Spider for processing uploaded documents.
    Supports PDF, CSV, Excel, and Word files.
    """

    name = 'document'

    # Map file extensions to extractor classes
    EXTRACTOR_MAP = {
        '.pdf': PDFExtractor,
        '.csv': CSVExtractor,
        '.tsv': CSVExtractor,
        '.txt': CSVExtractor,
        '.xlsx': ExcelExtractor,
        '.xlsm': ExcelExtractor,
        '.xls': ExcelExtractor,
        '.docx': WordExtractor,
    }

    def __init__(self, file_paths=None, *args, **kwargs):
        """
        Initialize spider with file paths.

        Args:
            file_paths: Comma-separated list of file paths, or single path
        """
        super().__init__(*args, **kwargs)

        if not file_paths:
            raise ValueError("file_paths argument is required")

        # Handle both single file and multiple files
        if isinstance(file_paths, str):
            self.file_paths = [p.strip() for p in file_paths.split(',')]
        else:
            self.file_paths = file_paths

        self._validate_files()

    def _validate_files(self) -> None:
        """Validate all files exist and are supported."""
        for file_path in self.file_paths:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            ext = Path(file_path).suffix.lower()
            if ext not in self.EXTRACTOR_MAP:
                raise ValueError(
                    f"Unsupported file type: {ext}. "
                    f"Supported: {list(self.EXTRACTOR_MAP.keys())}"
                )

    def start_requests(self):
        """Entry point - process all files."""
        for file_path in self.file_paths:
            yield scrapy.Request(
                url=f'file://{os.path.abspath(file_path)}',
                callback=self.parse,
                meta={'file_path': file_path},
                dont_filter=True
            )

    def parse(self, response):
        """Main parsing logic - delegate to appropriate extractor."""
        file_path = response.meta['file_path']
        file_ext = Path(file_path).suffix.lower()

        self.logger.info(f"Processing {file_ext} file: {file_path}")

        try:
            items = self._extract_items(file_path, file_ext)
            self.logger.info(f"Extracted {len(items)} items from {file_path}")

            for item_data in items:
                yield self._create_business_item(item_data)

        except Exception as e:
            self.logger.error(f"Failed to process {file_path}: {e}")

    def _extract_items(self, file_path: str, file_ext: str) -> List[Dict[str, Any]]:
        """
        Extract items using appropriate extractor.

        Args:
            file_path: Path to document
            file_ext: File extension

        Returns:
            List of extracted items
        """
        extractor_class = self.EXTRACTOR_MAP[file_ext]
        extractor = extractor_class(file_path, logger=self.logger)
        return extractor.extract()

    def _create_business_item(self, item_data: Dict[str, Any]) -> BusinessItem:
        """Create Scrapy BusinessItem from extracted data."""
        item = BusinessItem()

        # Map all fields
        item['source'] = item_data.get('source', 'document_upload')
        item['name'] = item_data.get('name', '').strip()
        item['venue_name'] = item_data.get(
            'venue_name', item_data.get('name', '')).strip()
        item['venue_address'] = item_data.get('venue_address', '').strip()
        item['venue_city'] = item_data.get('venue_city', 'Nashville')
        item['description'] = self._clean_description(
            item_data.get('description'))
        item['url'] = item_data.get('url', '').strip()
        item['event_date'] = item_data.get('event_date')
        item['category'] = item_data.get('category', 'document_extracted')
        item['latitude'] = item_data.get('latitude')
        item['longitude'] = item_data.get('longitude')

        return item

    def _clean_description(self, description: str) -> str:
        """Clean and truncate description."""
        if not description:
            return None

        cleaned = ' '.join(description.split())

        if len(cleaned) > 500:
            cleaned = cleaned[:497] + '...'

        return cleaned
