from datetime import datetime
import pytz
import re
def standardize_date(raw_date: str, source: str = None) -> str:
    if not raw_date:
        return None    
    source = source or ""
    if source in ['ticketmaster', 'seatgeek']:
        try:
            iso_date_str = raw_date.replace(' ', 'T')
            dt_object = datetime.fromisoformat(iso_date_str)
            return dt_object.isoformat()
        except (ValueError, TypeError):
            pass
    elif 'nashville.com' in source:
        try:
            match = re.search(r"(\w+\s\d+)\s*@\s*([\d:]+\s*[ap]m)", raw_date, re.IGNORECASE)
            if match:
                date_part, time_part = match.groups()
                full_date_str = f"{date_part} {datetime.now().year} {time_part}"
                dt_object = datetime.strptime(full_date_str, "%B %d %Y %I:%M %p")
                tz = pytz.timezone('America/Chicago')
                dt_localized = tz.localize(dt_object)
                return dt_localized.isoformat()
        except (ValueError, TypeError):
            pass
    elif source == 'underdog':
        try:
            date_part, time_part = raw_date.split('|')
            date_part = date_part.strip()
            time_part = time_part.strip()            
            timezone_map = {
                'CDT': 'America/Chicago', 'CST': 'America/Chicago',
                'EDT': 'America/New_York', 'EST': 'America/New_York'            }
            tz_match = re.search(r'(CDT|CST|EDT|EST)', time_part)
            tz_str = tz_match.group(1) if tz_match else 'CST'
            tz = pytz.timezone(timezone_map.get(tz_str, 'America/Chicago'))            
            time_clean = time_part.replace(tz_str, '').strip()
            time_format = "%I:%M%p" if ':' in time_clean else "%I%p"            
            dt_str = f"{date_part} {time_clean}"
            dt = datetime.strptime(dt_str, f"%B %d, %Y {time_format}")
            dt_localized = tz.localize(dt)
            return dt_localized.isoformat()
        except Exception as e:
            print(f"Error parsing date '{raw_date}' for source '{source}': {e}")
            return None
    elif source == 'yelp':
        return None
    return raw_date
def standardize_venue_name(name: str) -> str:
    if not name:
        return None
    name = ' '.join(name.split())
    name = re.sub(r'\s+(venue|hall|theater|theatre)$', '', name, flags=re.IGNORECASE)
    return name.title()

def standardize_price(price: str) -> float:
    if not price:
        return None
    price_lower = price.lower()
    if 'free' in price_lower:
        return 0.0
    match = re.search(r'\d+\.?\d*', price)
    if match:
        return float(match.group())
    return None