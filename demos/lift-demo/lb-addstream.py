from python_liftbridge import Lift, Stream, ErrStreamExists
import logging

# Create a Liftbridge client.
client = Lift(ip_address='localhost:9292', timeout=5)

# Create a Liftbridge stream with name "foo-stream"
try:
    client.create_stream(Stream(subject='feed', name='feed.stream.1'))
except ErrStreamExists:
    logging.info('This stream already exists!')

# Create a Liftbridge stream with name "foo-stream"
try:
    client.create_stream(Stream(subject='event', name='event.stream.1'))
except ErrStreamExists:
    logging.info('This stream already exists!')