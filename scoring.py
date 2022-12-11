import os, json,pyrebase
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "google_key.json"

firebaseConfig = {
  "apiKey": "AIzaSyBnz6wws3EjTRnFOG7NvefKSr9CsaOlcxY",
  "authDomain": "flick-it-users-storage.firebaseapp.com",
  "databaseURL": "https://flick-it-users-storage-default-rtdb.europe-west1.firebasedatabase.app",
  "projectId": "flick-it-users-storage",
  "storageBucket": "flick-it-users-storage.appspot.com",
  "messagingSenderId": "1046722019798",
  "appId": "1:1046722019798:web:905b021820e1922f95a477",
  "measurementId": "G-J3T9K8WPV2",
  "serviceAccount": "serviceAccountCredentials.json"
}

firebase = pyrebase.initialize_app(firebaseConfig)
db = firebase.database()

def getMetadata(id):
    data = db.child("metadata").child(id).get()
    return json.loads(json.dumps(data.val()))

def getUserData(userId):
    userData = db.child('users').child(userId).get()
    return json.loads(json.dumps(userData.val()))

def pointCalculation(data):
    time = data["timeUTC"]
    point = data["point"]
    hour = int(time[2:4])
    hour /= 60
    point = int(point*(1-hour/2))
    return point

def updatePoint(dataUser,userId,point):
    curPoint = dataUser["totalPoint"]
    newPoint = curPoint + point
    db.child('users').child(userId).update({"totalPoint":newPoint})
    return 0

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    id = message.data.decode("utf-8")
    message.ack()
    data = getMetadata(id)
    dataUser = getUserData(data["userId"])
    point = pointCalculation(data)
    updatePoint(dataUser,data["userId"],point)
    return 0

subscriber = pubsub_v1.SubscriberClient()
streaming_pull_future = subscriber.subscribe("projects/third-essence-365119/subscriptions/launch-scoring-sub", callback=callback)
print(f"Listening for messages on projects/third-essence-365119/subscriptions/launch-scoring-sub...\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.