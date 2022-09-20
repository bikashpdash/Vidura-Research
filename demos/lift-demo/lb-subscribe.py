from python_liftbridge import Lift, Stream

# Create a Liftbridge client.
client = Lift(ip_address='localhost:9292', timeout=5)

# Subscribe to the stream starting from the beginning.
for message in client.subscribe(
        Stream(
            subject='feed',
            name='feed.stream.1',
        ).start_at_earliest_received(), ):
    print("Received: '{}'".format(message.value))