import pandas as pd
import httpx
import math
from pathlib import Path
from .utils import save_json


def read_event_ids(excel_path: str) -> list[str]:
    df = pd.read_excel(excel_path, sheet_name="event")
    return df["Event ID"].dropna().astype(str).tolist()


def fetch_events(event_ids: list[str], base_url: str, auth: tuple, chunk_size: int = 50):
    client = httpx.Client(base_url=base_url, auth=auth)
    events = []

    for i in range(0, len(event_ids), chunk_size):
        chunk = event_ids[i:i+chunk_size]
        ids_param = ";".join(chunk)
        url = f"/api/tracker/events?event={ids_param}&fields=*&skipPaging=false"
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json().get("instances", [])
        found = next(
            (item for item in data if item["event"] in ids_param), None)
        if found:
            events.extend(data)
        print(f"{i+1}/{len(event_ids)}")
    return events


def run_fetch(excel_path: str, base_url: str, auth: tuple):
    event_ids = read_event_ids(excel_path)
    events = fetch_events(event_ids, base_url, auth)

    output_path = Path(excel_path).parent / "events.json"
    save_json({"events": events}, str(output_path))
    print(f"✅ Fetched {len(events)} events and saved to events.json")
