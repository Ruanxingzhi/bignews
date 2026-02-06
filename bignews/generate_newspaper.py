import json
import os
from datetime import datetime, UTC, timedelta
from retry import retry

from openai import OpenAI
from jinja2 import Template
from dotenv import load_dotenv
from pymongo import MongoClient

from bignews.util import SOURCES

load_dotenv()

@retry(tries=3, delay=2, backoff=2)
def query_llm(prompt):
    client = OpenAI()

    completion = client.chat.completions.create(
        model="gemini-3-flash-preview",
        messages=[
            {"role": "developer", "content": open('bignews/prompt/sys.j2', encoding='utf-8').read()},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
    )

    print('usage: ', completion.usage)

    res = completion.choices[0].message.content
    print(res)
    return res

@retry(tries=3, delay=2, backoff=2)
def main():
    articles = MongoClient(os.getenv('MONGO_URI')).bignews.articles
    newspapers = MongoClient(os.getenv('MONGO_URI')).bignews.newspapers

    if l := newspapers.find_one(sort=[("generated_at", -1)]):
        last_newspaper_time = l['generated_at']
    else:
        last_newspaper_time = datetime.now(UTC) - timedelta(days=2)

    print(f"{last_newspaper_time = }")

    def get_articles(source):
        return list(articles.find({"fetched_at": {"$gt": last_newspaper_time}, "source": SOURCES[source]}))[:200]

    def enrich(id_intro, full):
        res = []
        for it in id_intro:
            _id, intro = it['_id'], it['intro']
            print(f"{_id = }, {intro = }")
            p = next(filter(lambda i: i['_id'] == _id, full))

            res.append({**p, "intro": intro})

            print(intro, p, '\n')
        return res

    def gen_arxiv(source):
        with open('bignews/prompt/arxiv.j2', encoding='utf-8') as f:
            arxiv_j2 = f.read()
            docs = get_articles(source)
            if not docs:
                return []
            print(docs)

            prompt = Template(arxiv_j2).render(articles=docs)
            return enrich(json.loads(query_llm(prompt)), docs)

    def gen_bleepingcomputer():
        with open('bignews/prompt/bleepingcomputer.j2', encoding='utf-8') as f:
            bleepingcomputer_j2 = f.read()
            docs = get_articles('bleepingcomputer')
            if not docs:
                return []
            print(docs)

            prompt = Template(bleepingcomputer_j2).render(articles=docs)
            return enrich(json.loads(query_llm(prompt)), docs)

    result = {
        'generated_at': datetime.now(UTC),
        'article_start_at': last_newspaper_time,
        'articles': {
            'bleepingcomputer': gen_bleepingcomputer(),
            'arxiv_cs_cr': gen_arxiv('arxiv_cs_cr'),
            'arxiv_cs_ai': gen_arxiv('arxiv_cs_ai')
        }
    }

    newspapers.insert_one(result)


if __name__ == "__main__":
    main()
