import requests
class GoogleAuthClient:
    def __init__(self,token_uri,client_id,client_secret,redirect_uri,refresh_token):
        self.token_uri=token_uri
        self.client_id=client_id
        self.client_secret=client_secret
        self.redirect_uri=redirect_uri
        self.refresh_token=refresh_token

    def get_access_token(self):
        payload={
            'refresh_token':self.refresh_token,
            'client_id':self.client_id,
            'client_secret':self.client_secret,
            'redirect_uri':self.redirect_uri,
            'grant_type':'refresh_token'
        }
        res=requests.post(self.token_uri,data=payload)
        res.raise_for_status()
        return res.json['access_token']
