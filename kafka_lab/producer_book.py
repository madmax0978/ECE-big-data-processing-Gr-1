# %%
import os
import socket
import urllib.request
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic = 'book'
book_path = 'around_the_world_in_80_days.txt'
book_url = 'https://www.gutenberg.org/cache/epub/103/pg103.txt'

# Feature 1: auto-fetch the book from Project Gutenberg if it isn't cached locally yet
if not os.path.exists(book_path):
    print(f"'{book_path}' not found, downloading from Project Gutenberg...")
    urllib.request.urlretrieve(book_url, book_path)
    print("Download complete.")
else:
    print(f"Using existing local copy: {book_path}")

# %% Stream the book line by line instead of a timed loop
sent_count = 0
with open(book_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()  # remove trailing newline / spaces
        if not line:
            continue  # skip blank lines between paragraphs
        producer.produce(topic=topic, value=line)
        sent_count += 1

# %%
# Feature 2 (part A): poison pill - a sentinel message telling the consumer
# "the stream is over", so it can stop immediately instead of waiting on a timeout.
# Safe here because the topic has a single partition, so message order is preserved
# and this message is guaranteed to arrive after every book line.
producer.produce(topic=topic, value='__END_OF_STREAM__')

producer.flush()
print(f"Sent {sent_count} lines + 1 end-of-stream marker to topic '{topic}'")
