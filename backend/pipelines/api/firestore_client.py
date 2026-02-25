from google.cloud import firestore

class FirestoreClient:
    def __init__(self, project_id: str, database_id: str):
        self.db = firestore.Client(project=project_id, database=database_id)
    def get_session_users(self, session_id: str) -> list:
        doc = self.db.collection("sessions").document(session_id).get()
        if not doc.exists:
            raise RuntimeError(f"Session '{session_id}' does not exist in Firestore.")
        data = doc.to_dict()
        users = data.get("users", [])
        return [
            (u["user_id"], u["refresh_token"])
            for u in users
            if u.get("isactive", True) and u.get("user_id") and u.get("refresh_token")
        ]
    def get_session_status(self, session_id: str) -> str | None:
        doc = self.db.collection("sessions").document(session_id).get()
        if not doc.exists:
            return None
        return doc.to_dict().get("status")

    def update_session_status(self, session_id: str, status: str):
        self.db.collection("sessions").document(session_id).update({"status": status})
