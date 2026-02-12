from google.cloud import firestore

class FirestoreClient:
    def __init__(self,project_id,database_id):
        self.db=firestore.Client(project=project_id,database=database_id)

    def get_session_users(self,session_id):
        doc=self.db.collection('sessions').document(session_id).get()
        if not doc.exists:
            return RuntimeError(f'Session {session_id} does not exist...')
        
        data = doc.to_dict()
        users = data.get("users", [])

        return [
            (u["user_id"], u["refresh_token"])
            for u in users
            if u.get("isactive", True)
        ]

    def get_session_status(self, session_id):
        doc = self.db.collection("sessions").document(session_id).get()
        if not doc.exists:
            return None
        return doc.to_dict().get("status")

    
    def update_session_status(self, session_id, status):
        self.db.collection("sessions").document(session_id).update({
            "status": status
        })
