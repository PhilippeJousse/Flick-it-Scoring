import os, firebase_admin
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "google_key.json"

firebase_admin.initialize_app(firebase_admin.credentials.Certificate('serviceAccountCredentials.json'))
db = firebase_admin.firestore.client()

def getData(id):
    data = db.collection('metadata').document(id).get()
    data = data.to_dict()
    return data

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    id = message.data.decode("utf-8")
    message.ack()
    data = getData(id)
    time = data["timeUTC"]
    point = data["point"]
    hour = int(time[2:4])
    hour /= 60
    point = point*(1-hour/2)
    return 0

subscriber = pubsub_v1.SubscriberClient()
streaming_pull_future = subscriber.subscribe("projects/third-essence-365119/subscriptions/launch-ranking-sub", callback=callback)

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

