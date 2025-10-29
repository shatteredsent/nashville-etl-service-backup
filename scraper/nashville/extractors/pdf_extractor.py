"""
PDF document extractor using PyMuPDF.
Handles both simple and complex PDF layouts.
"""
import re
import pymupdf
from typing import List, Dict, Any, Optional
from .base_extractor import BaseExtractor


class PDFExtractor(BaseExtractor):
    """Extract structured data from PDF documents."""

    SUPPORTED_EXTENSIONS = {'.pdf'}

    # Patterns for identifying data types
    PHONE_PATTERN = re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
    URL_PATTERN = re.compile(r'https?://[^\s]+')
    DATE_PATTERNS = [
        re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}'),
        re.compile(r'\d{4}-\d{2}-\d{2}'),
        re.compile(
            r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]* \d{1,2}', re.IGNORECASE),
    ]

    ADDRESS_INDICATORS = frozenset([
        'street', 'st', 'avenue', 'ave', 'road', 'rd',
        'boulevard', 'blvd', 'drive', 'dr', 'nashville', 'tn'
    ])

    def extract(self) -> List[Dict[str, Any]]:
        """Extract structured data from PDF."""
        try:
            text = self._extract_text()
            if not text:
                self._log("No text extracted from PDF", 'warning')
                return []

            lines = self._clean_lines(text)
            self._log(f"Extracted {len(lines)} lines from PDF")

            items = self._parse_lines(lines)
            self._log(f"Parsed {len(items)} items from PDF")

            return [self._create_item(**item) for item in items if self._is_valid(item)]

        except Exception as e:
            self._log(f"PDF extraction failed: {e}", 'error')
            raise

    def _extract_text(self) -> str:
        """Extract all text from PDF pages."""
        try:
            doc = pymupdf.open(str(self.file_path))
            text_parts = [page.get_text() for page in doc]
            doc.close()
            return '\n'.join(text_parts)
        except Exception as e:
            raise RuntimeError(f"Failed to read PDF: {e}")

    def _clean_lines(self, text: str) -> List[str]:
        """Split text into clean, non-empty lines."""
        return [
            line.strip()
            for line in text.split('\n')
            if line.strip() and len(line.strip()) > 2
        ]

    def _parse_lines(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse lines into structured items."""
        items = []
        current_item = {}

        for line in lines:
            if self._is_structured_line(line):
                current_item = self._handle_structured_line(
                    line, current_item, items)
            else:
                current_item = self._handle_unstructured_line(
                    line, current_item, items)

        if current_item and current_item.get('name'):
            items.append(current_item)

        return items

    def _is_structured_line(self, line: str) -> bool:
        """Check if line has 'Label: Value' format."""
        return ':' in line and not line.startswith('http')

    def _handle_structured_line(
        self,
        line: str,
        current_item: Dict,
        items: List[Dict]
    ) -> Dict:
        """Process structured 'Label: Value' line."""
        label, value = self._split_label_value(line)

        if self._is_name_label(label):
            if current_item and current_item.get('name'):
                items.append(current_item)
            return {'name': value, 'venue_name': value}

        field = self._map_label_to_field(label)
        if field:
            current_item[field] = value
        else:
            self._append_to_description(current_item, line)

        return current_item

    def _handle_unstructured_line(
        self,
        line: str,
        current_item: Dict,
        items: List[Dict]
    ) -> Dict:
        """Process unstructured line by identifying its type."""
        line_type = self._identify_line_type(line)

        if line_type == 'name':
            if current_item and current_item.get('name'):
                items.append(current_item)
            return {'name': line, 'venue_name': line}

        if line_type in ['address', 'phone', 'url', 'date']:
            field_map = {
                'address': 'venue_address',
                'phone': 'phone',
                'url': 'url',
                'date': 'event_date'
            }
            current_item[field_map[line_type]] = line
        else:
            self._append_to_description(current_item, line)

        return current_item

    def _split_label_value(self, line: str) -> tuple[str, str]:
        """Split 'Label: Value' line."""
        parts = line.split(':', 1)
        label = parts[0].strip().lower()
        value = parts[1].strip() if len(parts) > 1 else ''
        return label, value

    def _is_name_label(self, label: str) -> bool:
        """Check if label indicates a name field."""
        return label in ['venue', 'location', 'place', 'event', 'name']

    def _map_label_to_field(self, label: str) -> Optional[str]:
        """Map label to standardized field name."""
        field_mapping = {
            'address': 'venue_address',
            'location address': 'venue_address',
            'venue address': 'venue_address',
            'date': 'event_date',
            'event date': 'event_date',
            'when': 'event_date',
            'phone': 'phone',
            'telephone': 'phone',
            'contact': 'phone',
            'website': 'url',
            'url': 'url',
            'web': 'url',
            'link': 'url',
        }
        return field_mapping.get(label)

    def _append_to_description(self, item: Dict, text: str) -> None:
        """Append text to item description."""
        if 'description' not in item:
            item['description'] = text
        else:
            item['description'] += ' ' + text

    def _identify_line_type(self, line: str) -> str:
        """Identify what type of data a line contains."""
        if self.PHONE_PATTERN.search(line):
            return 'phone'
        if self.URL_PATTERN.search(line):
            return 'url'
        if self._is_address(line):
            return 'address'
        if self._is_date(line):
            return 'date'
        if self._is_business_name(line):
            return 'name'
        return 'text'

    def _is_address(self, text: str) -> bool:
        """Check if text looks like an address."""
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in self.ADDRESS_INDICATORS)

    def _is_date(self, text: str) -> bool:
        """Check if text contains a date."""
        return any(pattern.search(text) for pattern in self.DATE_PATTERNS)

    def _is_business_name(self, text: str) -> bool:
        """Check if text looks like a business name."""
        if not (5 <= len(text) <= 100):
            return False

        name_indicators = ['&', 'and', "'s", 'the ', 'restaurant',
                           'bar', 'cafe', 'hotel', 'venue']
        text_lower = text.lower()

        has_indicator = any(ind in text_lower for ind in name_indicators)
        has_title_case = text[0].isupper()

        return has_indicator or has_title_case

    def _is_valid(self, item: Dict) -> bool:
        """Validate item has required fields."""
        return bool(item.get('name') and len(item['name']) >= 3)
