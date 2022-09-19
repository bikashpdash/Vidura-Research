import feedparser
import httpx
import asyncio
import json
from logger import log
import hashlib
from datetime import datetime
import meilisearch

import logging
logging.basicConfig(level=logging.DEBUG)
meili = meilisearch.Client('http://ip172-18-0-73-ccj3kuoja8q000d0dj1g-7700.direct.labs.play-with-docker.com','MASTER_KEY')


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
async def to_file(doc: feedparser.FeedParserDict):
    for entry in doc.entries:
        with open('feeds.txt','a') as f:
            sys_id=hashlib.sha256(entry["link"].encode('utf-8')).hexdigest()
            f.write("----------"+sys_id+"-----------\n")
            f.write("Link: "+entry["link"]+"\n")
            f.write("ID: "+entry["id"]+"\n")
            f.write("Title: "+entry["title"]+"\n")
            f.write("Summary: "+entry["summary"]+"\n")
            f.write("Timestamp :"+datetime.now().strftime("%y-%m-%dT%H-%M-%S")+"\n")            
            f.write("---------------------------------------------------\n")
            f.close()

@log
async def to_index(doc: feedparser.FeedParserDict):
    for entry in doc.entries:
        sys_id=hashlib.sha256(entry["link"].encode('utf-8')).hexdigest()
        doc={
            "id":sys_id,
            "link": entry["link"],
            "title":entry["title"],
            "summary": entry["summary"],
            "Timestamp": datetime.now().strftime("%y-%m-%dT%H-%M-%S")                
        }
        meili.index('news').add_documents(doc)


async def flow():
    urls = []
    with open('feedsource.txt', 'r') as f:
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
        await to_index(d)
        print("#######################################")


asyncio.run(flow())
