import requests

DISCOVERY_URL = "https://dataapi.trackmanbaseball.com/api/v1/discovery/game/sessions"

def get_game_sessions(access_token: str, date_from: str, date_to: str, session_type: str = "All"):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "text/plain",
        "Content-Type": "application/json-patch+json"
    }
    body = {
        "sessionType": session_type,
        "utcDateFrom": date_from,
        "utcDateTo": date_to
    }
    resp = requests.post(DISCOVERY_URL, headers=headers, json=body)
    print(" Payload sent:", body)      # <-- debug
    print(" Response text:", resp.text) # <-- debug
    resp.raise_for_status()
    return resp.json()
