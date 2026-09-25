# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config = {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
# New topic dedicated to the book streaming exercise (kept separate from 'timer')
topic = 'book'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

# create_topics() is async: wait for the broker to confirm before moving on,
# otherwise the list_topics() call below can run before the topic actually exists
for t, f in futures.items():
    try:
        f.result()
        print(f"Topic '{t}' created.")
    except Exception as e:
        print(f"Topic '{t}' not created (maybe it already exists): {e}")

# %%
# List all topics to confirm 'book' was created
x = admin_client.list_topics()
for t in x.topics.keys():
    print(t)

# %%
# Uncomment to delete the topic and start fresh
# admin_client.delete_topics([topic])

# %%
