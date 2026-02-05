import feedparser
from datetime import datetime, UTC

def fetch_rss(url):
    feed = feedparser.parse(url)
    fetched_at = datetime.now(UTC)
    return [
        {
            'title': entry.get('title', ''),
            'link': entry.get('link', ''),
            'description': entry.get('summary', '') or entry.get('description', ''),
            'published': entry.get('published', '') or entry.get('updated', ''),
            'author': entry.get('author', ''),
            'categories': [tag.get('term', '') for tag in entry.get('tags', [])],
            '_id': url + '/' + (entry.get('id', '') or entry.get('guid', '')),
            'source': url,
            'fetched_at': fetched_at,
        }
        for entry in feed.entries
    ]


SOURCES = {
    'bleepingcomputer': 'https://www.bleepingcomputer.com/feed/',
    'arxiv_cs_cr': 'https://rss.arxiv.org/atom/cs.cr',
    'arxiv_cs_ai': 'https://rss.arxiv.org/atom/cs.ai',
}
