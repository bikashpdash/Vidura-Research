from python_liftbridge import Lift, Message, Stream, ErrStreamExists

# Create a Liftbridge client.
client = Lift(ip_address='localhost:9292', timeout=5)

# Publish a message to the stream with the name "foo-stream".
client.publish(Message(value='hello', stream='foo-stream'))