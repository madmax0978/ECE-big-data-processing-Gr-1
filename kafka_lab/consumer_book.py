# %%
import string
from collections import Counter
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-reader',      # dedicated group so offsets don't clash with 'foo'
        'auto.offset.reset': 'smallest'}  # read from the beginning if no offset stored yet

consumer = Consumer(conf)

# %%
topic = 'book'
consumer.subscribe([topic])

# %%
# Same cleaning approach as the word count lab: lowercase + strip punctuation + drop stopwords
stopwords = {"the", "a", "an", "of", "and", "to", "in", "is", "it", "that",
             "on", "for", "with", "as", "was", "were", "at", "by", "this",
             "be", "or", "which", "from", "are", "he", "she", "his", "her",
             "they", "i", "you", "we", "but", "not", "had", "have", "has"}


def clean_line(line):
    words = line.split(' ')
    cleaned = [w.lower().strip(string.punctuation) for w in words]
    cleaned = [w for w in cleaned if w != '' and w not in stopwords]
    return cleaned


output_path = 'cleaned_book_output.txt'
output_file = open(output_path, 'w', encoding='utf-8')  # fresh file for each run

# %%
END_MARKER = '__END_OF_STREAM__'  # must match the poison pill sent by the producer

# Feature 2 (part B): tumbling window word-frequency aggregation.
# Every WINDOW_SIZE processed lines, print the top words seen in THAT window only,
# then reset the counter - mirrors the "Windows" aggregation concept from the lecture.
WINDOW_SIZE = 500
window_counter = Counter()
window_number = 1

# %%
# Configuration
MAX_EMPTY_POLLS = 15  # safety net if the poison pill is ever lost
MAX_ERRORS = 5         # ends after 5 consecutive errors
empty_polls = 0
error_count = 0
lines_written = 0

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        empty_polls += 1
        if empty_polls >= MAX_EMPTY_POLLS:
            print("Closing: No new messages received (safety timeout).")
            break
        continue

    if msg.error():
        error_count += 1
        print(f"Consumer error: {msg.error()}")
        if error_count >= MAX_ERRORS:
            print("Closing: Too many consecutive errors.")
            break
        continue

    empty_polls = 0
    error_count = 0

    raw_line = msg.value().decode('utf-8')

    # Poison pill received: stop immediately, no need to wait for a timeout
    if raw_line == END_MARKER:
        print("Closing: end-of-stream marker received.")
        break

    cleaned_words = clean_line(raw_line)
    if cleaned_words:
        output_file.write(' '.join(cleaned_words) + '\n')
        lines_written += 1
        window_counter.update(cleaned_words)

    # Window is full: report it and start a fresh one
    if lines_written > 0 and lines_written % WINDOW_SIZE == 0:
        top5 = window_counter.most_common(5)
        print(f"[Window {window_number}] top words: {top5}")
        window_counter = Counter()
        window_number += 1

# Report whatever is left in the last (incomplete) window
if window_counter:
    print(f"[Window {window_number} - final, partial] top words: {window_counter.most_common(5)}")

# Clean up
output_file.close()
consumer.close()
print(f"Wrote {lines_written} cleaned lines to {output_path}")
