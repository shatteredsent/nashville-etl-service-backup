"""
CSV document extractor.
Handles various CSV formats with flexible column mapping.
"""
import csv
from typing import List, Dict, Any, Set
from .base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """Extract structured data from CSV files."""

    SUPPORTED_EXTENSIONS = {'.csv', '.tsv', '.txt'}

    # Common column name variations
    NAME_COLUMNS = frozenset(
        ['name', 'event', 'venue', 'title', 'event_name', 'venue_name'])
    ADDRESS_COLUMNS = frozenset(
        ['address', 'venue_address', 'location', 'street'])
    DATE_COLUMNS = frozenset(
        ['date', 'event_date', 'start_date', 'datetime', 'when'])
    DESCRIPTION_COLUMNS = frozenset(
        ['description', 'desc', 'details', 'info', 'notes'])
    URL_COLUMNS = frozenset(['url', 'website', 'link', 'web', 'webpage'])
    PHONE_COLUMNS = frozenset(
        ['phone', 'telephone', 'contact', 'phone_number'])
    CITY_COLUMNS = frozenset(['city', 'venue_city', 'location_city'])

    def extract(self) -> List[Dict[str, Any]]:
        """Extract structured data from CSV."""
        try:
            delimiter = self._detect_delimiter()
            rows = self._read_csv(delimiter)

            if not rows:
                self._log("No data rows found in CSV", 'warning')
                return []

            header = self._extract_header(rows)
            if not header:
                self._log("No header found in CSV", 'warning')
                return []

            column_mapping = self._map_columns(header)
            self._log(f"Mapped columns: {column_mapping}")

            items = self._parse_rows(rows[1:], column_mapping)
            self._log(f"Extracted {len(items)} items from CSV")

            return [self._create_item(**item) for item in items if self._is_valid(item)]

        except Exception as e:
            self._log(f"CSV extraction failed: {e}", 'error')
            raise

    def _detect_delimiter(self) -> str:
        """Detect CSV delimiter from file extension and content."""
        if self.file_path.suffix.lower() == '.tsv':
            return '\t'

        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                sample = f.read(1024)
                sniffer = csv.Sniffer()
                return sniffer.sniff(sample).delimiter
        except Exception:
            return ','

    def _read_csv(self, delimiter: str) -> List[List[str]]:
        """Read CSV file into list of rows."""
        rows = []
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                rows = [row for row in reader if any(
                    cell.strip() for cell in row)]
        except UnicodeDecodeError:
            with open(self.file_path, 'r', encoding='latin-1') as f:
                reader = csv.reader(f, delimiter=delimiter)
                rows = [row for row in reader if any(
                    cell.strip() for cell in row)]

        return rows

    def _extract_header(self, rows: List[List[str]]) -> List[str]:
        """Extract and normalize header row."""
        if not rows:
            return []
        return [col.strip().lower() for col in rows[0]]

    def _map_columns(self, header: List[str]) -> Dict[str, str]:
        """Map CSV columns to standardized field names."""
        mapping = {}

        for idx, col in enumerate(header):
            if col in self.NAME_COLUMNS:
                mapping['name'] = idx
            elif col in self.ADDRESS_COLUMNS:
                mapping['venue_address'] = idx
            elif col in self.DATE_COLUMNS:
                mapping['event_date'] = idx
            elif col in self.DESCRIPTION_COLUMNS:
                mapping['description'] = idx
            elif col in self.URL_COLUMNS:
                mapping['url'] = idx
            elif col in self.PHONE_COLUMNS:
                mapping['phone'] = idx
            elif col in self.CITY_COLUMNS:
                mapping['venue_city'] = idx

        return mapping

    def _parse_rows(
        self,
        rows: List[List[str]],
        column_mapping: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Parse data rows into structured items."""
        items = []

        for row in rows:
            if not row or not any(cell.strip() for cell in row):
                continue

            item = {}
            for field, col_idx in column_mapping.items():
                if col_idx < len(row):
                    value = row[col_idx].strip()
                    if value:
                        item[field] = value

            if item.get('name'):
                item['venue_name'] = item['name']
                items.append(item)

        return items

    def _is_valid(self, item: Dict) -> bool:
        """Validate item has required fields."""
        return bool(item.get('name') and len(item['name']) >= 2)
