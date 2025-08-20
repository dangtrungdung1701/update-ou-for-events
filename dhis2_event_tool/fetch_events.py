import pandas as pd
import httpx
import math
import asyncio
from pathlib import Path
from .utils import save_json


def read_event_ids(excel_path: str) -> list[str]:
    df = pd.read_excel(excel_path, sheet_name="event")
    return df["Event ID"].dropna().astype(str).tolist()


async def fetch_chunk(client: httpx.AsyncClient, chunk: list[str], idx: int, total: int, semaphore: asyncio.Semaphore):
    ids_param = ";".join(chunk)
    url = f"/api/tracker/events?event={ids_param}&fields=*&skipPaging=false"
    async with semaphore:  # limit concurrent requests
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json().get("instances", [])
        print(f"{idx}/{total}")
        return data


async def fetch_events(event_ids: list[str], base_url: str, auth: tuple, chunk_size: int = 50, parallel: int = 20):
    events = []
    semaphore = asyncio.Semaphore(parallel)  # control concurrency

    async with httpx.AsyncClient(base_url=base_url, auth=auth, timeout=30.0) as client:
        # Split into chunks
        chunks = [event_ids[i:i+chunk_size]
                  for i in range(0, len(event_ids), chunk_size)]
        total_chunks = len(chunks)

        # Schedule all tasks at once, semaphore will enforce max concurrency
        tasks = [
            fetch_chunk(client, chunk, idx + 1, total_chunks, semaphore)
            for idx, chunk in enumerate(chunks)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # collect successful results
        for r in results:
            if isinstance(r, Exception):
                print("⚠️ Error:", r)
            else:
                events.extend(r)

    return events


# def fetch_events(event_ids: list[str], base_url: str, auth: tuple, chunk_size: int = 50):
#     client = httpx.Client(base_url=base_url, auth=auth)
#     events = []

#     for i in range(0, len(event_ids), chunk_size):
#         chunk = event_ids[i:i+chunk_size]
#         ids_param = ";".join(chunk)
#         url = f"/api/tracker/events?event={ids_param}&fields=*&skipPaging=false"
#         resp = client.get(url)
#         resp.raise_for_status()
#         data = resp.json().get("instances", [])
#         found = next(
#             (item for item in data if item["event"] in ids_param), None)
#         if found:
#             events.extend(data)
#         print(f"{math.ceil((i+1)/chunk_size)}/{math.ceil(len(event_ids)/chunk_size)}")
#     return events


def run_fetch(excel_path: str, base_url: str, auth: tuple):
    event_ids = read_event_ids(excel_path)
    events = asyncio.run(fetch_events(event_ids, base_url, auth))

    output_path = Path(excel_path).parent / "events.json"
    save_json({"events": events}, str(output_path))
    print(f"✅ Fetched {len(events)} events and saved to events.json")
