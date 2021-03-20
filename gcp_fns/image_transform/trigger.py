from google.cloud import pubsub_v1
import json

project = "merchwatch"
topic_name = "dev_niche_data"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project, topic_name)

data = {"key1": "value1", "key2": "value2"}
data = str(json.dumps(data))

data = data.encode('utf-8')
publisher.publish(topic_path, data=data)