import sys
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from bignews.util import fetch_rss

load_dotenv()

SOURCES = {
    'bleepingcomputer': 'https://www.bleepingcomputer.com/feed/',
    'arxiv_cs_cr': 'https://rss.arxiv.org/atom/cs.cr',
    'arxiv_cs_ai': 'https://rss.arxiv.org/atom/cs.ai',
}

def main():
    source = sys.argv[1]
    entries = fetch_rss(SOURCES[source])
    print(f'#entris: {len(entries)}')
    collection = MongoClient(os.getenv('MONGO_URI')).bignews.articles

    try:
        result = collection.insert_many(entries, ordered=False)
        print(f"insert: {len(result.inserted_ids)}")
    except BulkWriteError as e:
        print(f"insert: {e.details['nInserted']}")

if __name__ == '__main__':
    main()
