import json
import os
from datetime import datetime, UTC, timedelta
from openai import OpenAI
from jinja2 import Template
from dotenv import load_dotenv
from pymongo import MongoClient

from bignews.util import SOURCES

load_dotenv()



def query_llm(prompt):
    client = OpenAI()

    completion = client.chat.completions.create(
        model="gemini-3-pro-preview",
        messages=[
            {"role": "developer", "content": open('bignews/prompt/sys.j2', encoding='utf-8').read()},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )

    print('usage: ', completion.usage)

    return completion.choices[0].message.content


def main():
    articles = MongoClient(os.getenv('MONGO_URI')).bignews.articles
    newspapers = MongoClient(os.getenv('MONGO_URI')).bignews.newspapers

    last_newspaper_time = newspapers.find_one(sort=[("generated_at", -1)]) or datetime.now(UTC) - timedelta(days=2)
    print(f"{last_newspaper_time = }")

    def get_articles(source):
        return list(articles.find({"fetched_at": {"$gt": last_newspaper_time}, "source": SOURCES[source]}))[:100]

    def gen_arxiv(source):
        with open('bignews/prompt/arxiv.j2', encoding='utf-8') as f:
            arxiv_j2 = f.read()
            docs = get_articles(source)
            if not docs:
                return []
            print(docs)

            prompt = Template(arxiv_j2).render(articles=docs)
            resp = query_llm(prompt)

            for it in json.loads(resp):
                _id, intro = it['_id'], it['intro']
                print(f"{_id = }, {intro = }")
                p = next(filter(lambda i: i['_id'] == _id, docs))
                # print(intro, p)
                # print('\n\n')

    gen_arxiv('arxiv_cs_cr')


if __name__ == "__main__":
    main()
