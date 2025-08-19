import click
from dhis2_event_tool.fetch_events import run_fetch
from dhis2_event_tool.map_events import map_events
from dotenv import load_dotenv
# Load env vars
load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://play.dhis2.org/dev")
AUTH = (os.getenv("DHIS2_USERNAME", "admin"),
        os.getenv("DHIS2_PASSWORD", "district"))


@click.group()
def cli():
    """DHIS2 Tool - Manage fetch and map scripts."""
    pass


@cli.command()
@click.argument("excel_path", type=click.Path(exists=True))
def fetch(excel_path):
    """Fetch events from DHIS2 and save to events.json"""
    click.echo(f"Fetching events")
    run_fetch(excel_path, BASE_URL, AUTH)
    click.echo("✅ Events saved to events.json")


@cli.command()
@click.argument("excel_path", type=click.Path(exists=True))
def map(excel_path):
    """Map events.json to mapped_events.xlsx"""
    click.echo(f"Mapping events from {excel_path}...")
    map_events(excel_path)
    click.echo("✅ Events mapped to updated_events.xlsx")


if __name__ == "__main__":
    cli()
