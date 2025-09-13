import requests
from typing import Dict

TOKEN_URL = "https://login.trackmanbaseball.com/connect/token"

def get_access_token(client_id: str, client_secret: str) -> Dict:
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    resp = requests.post(TOKEN_URL, data=payload)
    resp.raise_for_status()
    return resp.json()
