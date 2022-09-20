import meilisearch
import json

client = meilisearch.Client('http://ip172-18-0-74-ccj3kuoja8q000d0dj1g-7700.direct.labs.play-with-docker.com')

doc={
    "_id":'1',
    "title":'bikash',
    "content":'Hi Search , my name is bikash'
}
client.index('movies').add_documents(doc)