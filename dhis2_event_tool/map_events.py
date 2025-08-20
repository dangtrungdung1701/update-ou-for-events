import pandas as pd
from .utils import load_json, save_json
from pathlib import Path

# Example dataElement IDs (replace with real DHIS2 data elements)
PROVINCE_DE = "HYA9hIPGcm9"
DISTRICT_DE = "rMlPCCGRopu"
COMMUNE_DE = "OU1Pzn9LCBf"


def read_mapping(excel_path: str):
    df = pd.read_excel(excel_path, sheet_name="mapping").fillna("")
    return df.to_dict("records")


def upsert_data_value(data_values: list[dict], data_element: str, value: str):
    """Update value if dataElement exists, otherwise append a new one."""
    for dv in data_values:
        if dv.get("dataElement") == data_element:
            dv["value"] = value or ""
            return
    # not found → append new
    data_values.append({"dataElement": data_element, "value": value or ""})


def map_events(excel_path: str):
    mapping = read_mapping(excel_path)
    output_path = Path(excel_path).parent / "events.json"
    events = load_json(output_path)

    mapping_dict = {m["Current OU"]: m for m in mapping}

    updated_events = []
    for ev in events["events"]:
        ou = ev.get("orgUnit")
        if ou in mapping_dict:
            m = mapping_dict[ou]
            ev["orgUnit"] = m["New OU"]

            # make sure dataValues exists
            if "dataValues" not in ev or ev["dataValues"] is None:
                ev["dataValues"] = []

            upsert_data_value(ev["dataValues"], PROVINCE_DE,
                              m["Province (HYA9hIPGcm9)"])
            upsert_data_value(ev["dataValues"], DISTRICT_DE,
                              m["District (rMlPCCGRopu)"])
            upsert_data_value(ev["dataValues"], COMMUNE_DE,
                              m["Commune (OU1Pzn9LCBf)"])

        updated_events.append(ev)

    updated_output_path = Path(excel_path).parent / "events_updated.json"
    save_json({"events": updated_events}, updated_output_path)
    print(
        f"✅ Updated {len(updated_events)} events and saved to events_updated.json")
