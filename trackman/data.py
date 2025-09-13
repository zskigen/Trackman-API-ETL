import requests

BASE_URL = "https://dataapi.trackmanbaseball.com/api/v1/data/game"

def get_game_balls(access_token: str, session_id: str):
    url = f"{BASE_URL}/balls/{session_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def get_game_plays(access_token: str, session_id: str):
    url = f"{BASE_URL}/plays/{session_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()
