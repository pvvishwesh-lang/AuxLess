from flask import Flask, redirect, request
import requests, os, base64
from nacl import encoding, public

app = Flask(__name__)

GITHUB_TOKEN = os.environ['GH_TOKEN']
REPO = "pvvishwesh-lang/AuxLess"  
CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
REDIRECT_URI = os.environ['REDIRECT_URIS']

@app.route("/login")
def login():
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={CLIENT_ID}&"
        f"redirect_uri={REDIRECT_URI}&"
        "response_type=code&"
        "scope=https://www.googleapis.com/auth/youtube.readonly&"
        "access_type=offline&"
        "prompt=consent"
    )
    return redirect(auth_url)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    res = requests.post(token_url, data=data)
    tokens = res.json()
    access_token = tokens['access_token']
    refresh_token = tokens.get('refresh_token')
    

    url = f"https://api.github.com/repos/{REPO}/actions/secrets/YOUTUBE_REFRESH_TOKEN"
    
    key_url = f"https://api.github.com/repos/{REPO}/actions/secrets/public-key"
    r = requests.get(key_url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
    r.raise_for_status()
    key_data = r.json()
    public_key = key_data["key"]
    key_id = key_data["key_id"]

    def encrypt_secret(public_key: str, secret_value: str) -> str:
        public_key = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        return base64.b64encode(encrypted).decode("utf-8")

    encrypted_value = encrypt_secret(public_key, refresh_token)

    payload = {"encrypted_value": encrypted_value, "key_id": key_id}
    r = requests.put(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, json=payload)
    r.raise_for_status()

    return "Refresh token saved to GitHub Secrets successfully!"



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

