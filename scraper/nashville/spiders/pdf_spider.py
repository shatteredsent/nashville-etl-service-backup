import os
import json
from typing import Dict, Any, List
import pymupdf
from scraper.nashville.items import BusinessItem
from dotenv import load_dotenv
import scrapy
import re

load_dotenv()


class PDFSpider(scrapy.Spider):
    name = 'pdf'

    def __init__(self, pdf_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pdf_path = pdf_path
        if not self.pdf_path:
            raise ValueError("pdf_path argument is required")
        if not os.path.exists(self.pdf_path):
            raise FileNotFoundError(f"PDF file not found: {self.pdf_path}")

    def start_requests(self):
        """Entry point - process the PDF file"""
        yield scrapy.Request(
            url='file://' + os.path.abspath(self.pdf_path),
            callback=self.parse,
            dont_filter=True
        )

    def parse(self, response):
        """Main parsing logic - extract and yield items"""
        text_content = self._extract_text_from_pdf()
        if not text_content:
            self.logger.error("No text extracted from PDF")
            return
        lines = self._split_into_lines(text_content)
        self.logger.info(f"Extracted {len(lines)} lines from PDF")
        parsed_data = self._parse_structured_data(lines)
        self.logger.info(f"Parsed {len(parsed_data)} items from lines")
        for item_data in parsed_data:
            if self._is_valid_item(item_data):
                yield self._create_business_item(item_data)
            else:
                self.logger.debug(
                    f"Skipping invalid item: {item_data.get('name', 'no name')}")

    def _extract_text_from_pdf(self) -> str:
        """Extract all text from PDF using PyMuPDF"""
        try:
            doc = pymupdf.open(self.pdf_path)
            text_parts = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                text_parts.append(page.get_text())
            doc.close()
            return '\n'.join(text_parts)
        except Exception as e:
            self.logger.error(f"Failed to extract text from PDF: {e}")
            return ""

    def _split_into_lines(self, text: str) -> List[str]:
        """Split text into clean lines"""
        lines = text.split('\n')
        clean_lines = []
        for line in lines:
            cleaned = line.strip()
            if cleaned and len(cleaned) > 3:
                clean_lines.append(cleaned)
        return clean_lines

    def _parse_structured_data(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Parse lines into structured data items"""
        items = []
        current_item = {}
        for i, line in enumerate(lines):
            # Check for structured format (Label: Value)
            if ':' in line and not line.startswith('http'):
                parts = line.split(':', 1)
                label = parts[0].strip().lower()
                value = parts[1].strip() if len(parts) > 1 else ''
                # Handle structured labels
                if label in ['venue', 'location', 'place']:
                    if current_item and current_item.get('name'):
                        items.append(current_item)
                        current_item = {}
                    current_item['name'] = value
                    current_item['venue_name'] = value
                elif label in ['address', 'location address', 'venue address']:
                    current_item['venue_address'] = value
                elif label in ['date', 'event date', 'when']:
                    current_item['event_date'] = value
                elif label in ['phone', 'telephone', 'contact']:
                    current_item['phone'] = value
                elif label in ['website', 'url', 'web', 'link']:
                    current_item['url'] = value
                elif label in ['category', 'type', 'genre']:
                    # Store but don't make it the name
                    if 'description' not in current_item:
                        current_item['description'] = f"Category: {value}"
                    else:
                        current_item['description'] += f" | Category: {value}"
                else:
                    # Unknown label - add to description
                    if 'description' not in current_item:
                        current_item['description'] = line
                    else:
                        current_item['description'] += ' ' + line
            else:
                # No colon - classify normally
                line_type = self._identify_line_type(line)
                if line_type == 'name':
                    if current_item and current_item.get('name'):
                        items.append(current_item)
                        current_item = {}
                    current_item['name'] = line
                elif line_type == 'address':
                    current_item['venue_address'] = line
                elif line_type == 'phone':
                    current_item['phone'] = line
                elif line_type == 'url':
                    current_item['url'] = line
                elif line_type == 'date':
                    current_item['event_date'] = line
                else:
                    if 'description' not in current_item:
                        current_item['description'] = line
                    else:
                        current_item['description'] += ' ' + line
        if current_item and current_item.get('name'):
            items.append(current_item)
        return items

    def _identify_line_type(self, line: str) -> str:
        """Identify what type of data a line contains"""
        if self._is_phone_number(line):
            return 'phone'
        if self._is_url(line):
            return 'url'
        if self._is_address(line):
            return 'address'
        if self._is_date(line):
            return 'date'
        if self._is_business_name(line):
            return 'name'
        return 'text'

    def _is_phone_number(self, text: str) -> bool:
        """Check if text is a phone number"""
        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        return bool(re.search(phone_pattern, text))

    def _is_url(self, text: str) -> bool:
        """Check if text is a URL"""
        url_pattern = r'https?://[^\s]+'
        return bool(re.search(url_pattern, text))

    def _is_address(self, text: str) -> bool:
        """Check if text is a Nashville address"""
        address_indicators = ['street', 'st', 'avenue', 'ave', 'road', 'rd',
                              'boulevard', 'blvd', 'drive', 'dr', 'nashville']
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in address_indicators)

    def _is_date(self, text: str) -> bool:
        """Check if text contains a date"""
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{2,4}',
            r'\d{4}-\d{2}-\d{2}',
            r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]* \d{1,2}'
        ]
        text_lower = text.lower()
        return any(re.search(pattern, text_lower) for pattern in date_patterns)

    def _is_business_name(self, text: str) -> bool:
        """Check if text looks like a business name"""
        if len(text) < 5 or len(text) > 100:
            return False
        name_indicators = ['&', 'and', "'s", 'the ', 'restaurant', 'bar',
                           'cafe', 'hotel', 'venue', 'theater', 'museum']
        text_lower = text.lower()
        has_indicator = any(
            indicator in text_lower for indicator in name_indicators)
        has_title_case = text[0].isupper()
        return has_indicator or has_title_case

    def _is_valid_item(self, item_data: Dict[str, Any]) -> bool:
        """Validate that item has minimum required fields"""
        if not item_data.get('name'):
            return False
        if len(item_data.get('name', '')) < 3:
            return False
        return True

    def _create_business_item(self, item_data: Dict[str, Any]) -> BusinessItem:
        """Create a BusinessItem from parsed data"""
        item = BusinessItem()
        item['source'] = 'pdf_upload'
        item['name'] = item_data.get('name', '').strip()
        item['venue_name'] = item_data.get('name', '').strip()
        item['venue_address'] = item_data.get('venue_address', '').strip()
        item['venue_city'] = 'Nashville'
        item['description'] = self._clean_description(
            item_data.get('description', ''))
        item['url'] = item_data.get('url', '').strip()
        item['event_date'] = item_data.get('event_date')
        item['category'] = 'pdf_extracted'
        return item

    def _clean_description(self, description: str) -> str:
        """Clean and truncate description"""
        if not description:
            return None
        cleaned = ' '.join(description.split())
        if len(cleaned) > 500:
            cleaned = cleaned[:497] + '...'
        return cleaned
