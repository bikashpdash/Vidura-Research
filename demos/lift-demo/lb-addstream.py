from python_liftbridge import Lift, Message, Stream, ErrStreamExists

# Create a Liftbridge client.
client = Lift(ip_address='localhost:9292', timeout=5)

# Create a Liftbridge stream with name "foo-stream"
try:
    client.create_stream(Stream(subject='foo', name='foo-stream'))
except ErrStreamExists:
    print('This stream already exists!')