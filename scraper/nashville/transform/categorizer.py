def categorize_event(name: str, description: str = "", venue: str = "") -> tuple[str, str]:
    name_lower = name.lower() if name else ""
    desc_lower = description.lower() if description else ""
    venue_lower = venue.lower() if venue else ""
    combined = f"{name_lower} {desc_lower} {venue_lower}"
    if any(word in combined for word in ['fest', 'festival']):
        return 'festival', _detect_genre(combined)
    if any(word in combined for word in ['comedy', 'comedian', 'stand-up', 'standup']):
        return 'comedy', None
    if any(word in combined for word in ['theater', 'theatre', 'play', 'musical', 'broadway']):
        return 'theater', None
    if any(word in combined for word in ['game', 'match', 'tournament', 'sports']):
        return 'sports', None
    return 'music', _detect_genre(combined)


def _detect_genre(text: str) -> str:
    genre_keywords = {
        'country': ['country', 'honky tonk', 'twang', 'bluegrass', 'americana'],
        'rock': ['rock', 'punk', 'metal', 'alternative', 'indie rock'],
        'jazz': ['jazz', 'swing', 'bebop'],
        'blues': ['blues', 'rhythm and blues', 'r&b'],
        'electronic': ['electronic', 'edm', 'house', 'techno', 'dubstep'],
        'hip-hop': ['hip hop', 'hip-hop', ' rap ', ' trap '],
        'folk': ['folk', 'acoustic', 'singer-songwriter'],
        'pop': ['pop', 'top 40'],
        'classical': ['classical', 'orchestra', 'symphony'],
    }
    for genre, keywords in genre_keywords.items():
        if any(keyword in text for keyword in keywords):
            return genre
    return 'general'
