import threading 
from python_liftbridge import Lift, Stream
import meilisearch
import json

# Create a Liftbridge client.

broker_address='localhost:9292'
meili_api = 'https://data-ops.bpdash.repl.co'
meili_key = 'VIDURA_KEY'

client = Lift(ip_address=broker_address, timeout=5)

def to_index(doc):
    meili = meilisearch.Client(meili_api , meili_key)
    index = meili.index('news-feed')
    print(doc)
    index.add_documents(doc)

def subscribe_feeds():
    # Subscribe to the stream starting from the beginning.
    for message in client.subscribe(
            Stream(
                subject='feed',
                name='feed.stream.1',
            ).start_at_earliest_received(), ):
        if message != None and len(message.value)>0:        
            data=message.value.decode('UTF-8')
            print("Received: '{}'".format(data))
            to_index(json.loads(data))

def subscribe_events():
    # Subscribe to the stream starting from the beginning.
    for message in client.subscribe(
            Stream(
                subject='event',
                name='event.stream.1',
            ).start_at_earliest_received(), ):
        print("Received: '{}'".format(message.value))

t1=threading.Thread(target = subscribe_feeds)
t2=threading.Thread(target = subscribe_events)
t1.start()
# t2.start()

