import functions_framework
import os
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.events.cloud.firestore_v1 import DocumentEventData

PROJECT_ID = 'main-shade-485500-a0'
PUBSUB_TOPIC = 'CloudFunctionPUBSUB'

db=firestore.Client(database='auxless')
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

def claim_session(session_id):
    doc_ref = db.collection("sessions").document(session_id)
    @firestore.transactional
    def transaction_claim(transaction):
        snapshot = doc_ref.get(transaction=transaction)
        if not snapshot.exists:
            return False
        status = snapshot.get("status")
        if status != "pending":
            return False
        transaction.update(doc_ref, {"status": "running"})
        return True
    transaction = db.transaction()
    return transaction_claim(transaction)

@functions_framework.cloud_event
def firestore_session_trigger(cloud_event):
    CLOUD_RUN_URL=os.environ.get("CLOUD_RUN_URL","https://auxless-610684648990.europe-west1.run.app/run_pipeline")
    firestore_payload = DocumentEventData.deserialize(cloud_event.data)    
    new_doc = firestore_payload.value
    session_id = new_doc.name.split("/")[-1]
    if not new_doc: 
        return
    new_fields = new_doc.fields
    new_status = new_fields.get("status").string_value if "status" in new_fields else None    
    if new_status == "pending":
        claimed=claim_session(session_id)
        if not claimed:
            print('Session claimed. Skipping')
            return
        future = publisher.publish(topic_path, session_id.encode("utf-8"))
        print(f"Published session {session_id} to Pub/Sub: {future.result()}")
    else:
        print(f"Ignoring update. Status is {new_status}")
