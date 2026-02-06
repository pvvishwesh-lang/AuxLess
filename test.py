# add_user_to_firestore.py
import os
from google.cloud import firestore

# Replace these with your actual environment variables
USER_ID = os.environ.get("YT_USER_ID", "my_google_account")  # any identifier for this user
REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")  # GitHub secret or local env variable

PROJECT_ID = os.environ.get("PROJECT_ID")  # your GCP project ID

if not REFRESH_TOKEN:
    raise RuntimeError("Refresh token not found. Make sure YT_REFRESH_TOKEN is set in your env.")

# Initialize Firestore client
db = firestore.Client(project=PROJECT_ID)
collection_name = "users"

# Add user document
doc_ref = db.collection(collection_name).document(USER_ID)
doc_ref.set({
    "refresh_token": REFRESH_TOKEN,
    "last_active": firestore.SERVER_TIMESTAMP,
    'active': True
})

print(f"User '{USER_ID}' added to Firestore with refresh token.")
