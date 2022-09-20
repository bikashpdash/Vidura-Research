import feedparser
import httpx
import asyncio
import json
from logger import log
import hashlib
from datetime import datetime
import meilisearch_python_async as aiomeilisearch
import meilisearch
import logging
from python_liftbridge import Lift, Message, Stream, ErrStreamExists

logging.basicConfig(level=logging.INFO)


@log
async def fetch_feed(url: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        return r


@log
async def parse_feed(resp: str):
    d = feedparser.parse(resp)
    return d


@log
async def to_cache(doc: feedparser.FeedParserDict):
    for entry in doc.entries:
        with open('data/cache/feeds.txt', 'a') as f:
            sys_id = hashlib.sha256(entry["link"].encode('utf-8')).hexdigest()
            f.write("----------" + sys_id + "-----------\n")
            f.write("Link: " + entry["link"] + "\n")
            f.write("ID: " + entry["id"] + "\n")
            f.write("Title: " + entry["title"] + "\n")
            f.write("Summary: " +
                    entry["summary"] if "summary" in entry.keys() else "" +
                    "\n")
            f.write("Timestamp :" +
                    datetime.now().strftime("%y-%m-%dT%H-%M-%S") + "\n")
            f.write("---------------------------------------------------\n")
            f.close()


@log
async def to_index(doc: feedparser.FeedParserDict):
    meili = meilisearch.Client(
        'http://ip172-18-0-13-cckp8ba3icp00083pabg-7700.direct.labs.play-with-docker.com',
        'MASTER_KEY')
    index = meili.index('news-feed')
    docs = []
    for entry in doc.entries:
        sys_id = hashlib.sha256(entry["link"].encode('utf-8')).hexdigest()
        doc = {
            "id": sys_id,
            "link": entry["link"],
            "title": entry["title"],
            "summary": entry["summary"] if "summary" in entry.keys() else "",
            "timestamp": datetime.now().strftime("%y-%m-%dT%H-%M-%S")
        }
        docs.append(doc)
        logging.info(entry["link"])
    with open('data/cache/docs.txt', 'a') as d:
        d.write("###########################\n")
        d.write(json.dumps(docs) + "\n")
        d.close()
    index.add_documents(docs)


@log
async def to_aioindex(doc: feedparser.FeedParserDict):
    async with aiomeilisearch.Client(
            'http://ip172-18-0-13-cckp8ba3icp00083pabg-7700.direct.labs.play-with-docker.com',
            'MASTER_KEY') as meili:
        index = meili.index('news-feed')
        docs = []
        for entry in doc.entries:
            if "summary" in entry.keys() and entry["summary"] is not None:
                sys_id = hashlib.sha256(
                    entry["link"].encode('utf-8')).hexdigest()
                doc = {
                    "id": sys_id,
                    "link": entry["link"],
                    "title": entry["title"],
                    "summary":
                    entry["summary"] if "summary" in entry.keys() else "",
                    "timestamp": datetime.now().strftime("%y-%m-%dT%H-%M-%S")
                }
                docs.append(doc)
        await index.add_documents(docs)


async def to_stream(doc: feedparser.FeedParserDict):
    # Create a Liftbridge client.
    client = Lift(ip_address='localhost:9292', timeout=5)
    # Create a Liftbridge stream with name "foo-stream"
    try:
        client.create_stream(Stream(subject='feed', name='feed.stream.1'))
    except ErrStreamExists:
        logging.info('This stream already exists!')

    for entry in doc.entries:
        if "summary" in entry.keys() and entry["summary"] is not None:
            sys_id = hashlib.sha256(entry["link"].encode('utf-8')).hexdigest()
            doc = {
                "id": sys_id,
                "link": entry["link"],
                "title": entry["title"],
                "summary":
                entry["summary"] if "summary" in entry.keys() else "",
                "timestamp": datetime.now().strftime("%y-%m-%dT%H-%M-%S")
            }
            # Publish a message to the stream with the name "foo-stream".
            client.publish(
                Message(value=json.dumps(doc), stream='feed.stream.1'))


async def flow():
    urls = []
    with open('feed-crawler/feed-source.txt', 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            print(line.strip())
            urls.append(line.strip())

    for url in urls:
        print(url)
        r = await fetch_feed(url)
        d = await parse_feed(r)
        await to_stream(d)
        print("#######################################")


asyncio.run(flow())
