from google.cloud import firestore

class FirestoreClient:
    def __init__(self,project_id):
        self.db=firestore.Client(project=project_id)
        self.collection_name='users'
    
    def add_users(self,user_id,refresh_token):
        doc_ref=self.db.collection(self.collection_name).document(user_id)
        doc_ref.set({
            'refresh_token':refresh_token,
            'last_active':firestore.SERVER_TIMESTAMP
        })

    def get_all_users(self):
        users=self.db.collection(self.collection_name).stream()
        return [(user.id,user.to_dict()['refresh_token']) for user in users]
