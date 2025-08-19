import os
from dotenv import load_dotenv
from dhis2_event_tool.fetch_events import run_fetch

if __name__ == "__main__":
    # load .env file
    load_dotenv()

    BASE_URL = os.getenv("BASE_URL")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD")

    if not BASE_URL or not DHIS2_USERNAME or not DHIS2_PASSWORD:
        raise RuntimeError(
            "Missing BASE_URL, DHIS2_USERNAME, or DHIS2_PASSWORD in .env")

    AUTH = (DHIS2_USERNAME, DHIS2_PASSWORD)

    run_fetch("data/Update_event_Mapping.xlsx", BASE_URL, AUTH)
