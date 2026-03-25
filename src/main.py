"""AWS HubSpot Sync — sync HubSpot deals to AWS Partner Central (ACE Pipeline Manager)."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file at project root
load_dotenv(Path(__file__).parent.parent / ".env")

import typer

from .logger import get_logger, print_status

logger = get_logger("ACESync")
app = typer.Typer(help="AWS Partner Central (ACE) Pipeline Sync")


@app.command()
def sync(
    catalog: str = typer.Option("Sandbox", help="ACE catalog: Sandbox or AWS"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without writing to ACE or HubSpot"),
) -> None:
    """Sync eligible HubSpot deals to AWS Partner Central."""
    from .config import ACEConfig, validate_config
    from .sync import run_sync

    # Pre-flight check — catch misconfiguration early with clear messages
    config_errors = validate_config()
    if config_errors:
        print_status("Configuration errors found:", "error")
        for err in config_errors:
            print(f"  \u2022 {err}")
        print()
        print_status("Fix the above in your .env file. See .env.example for instructions.", "info")
        raise typer.Exit(code=1)

    config = ACEConfig(catalog=catalog, dry_run=dry_run)
    result = run_sync(config)

    if result.errors:
        raise typer.Exit(code=1)


@app.command()
def validate() -> None:
    """Check which deals are ready for ACE sync (read-only)."""
    from .config import ACEConfig
    from .sync import validate_deals

    config = ACEConfig()
    validate_deals(config)


@app.command(name="test-connection")
def test_connection(
    catalog: str = typer.Option("Sandbox", help="ACE catalog: Sandbox or AWS"),
) -> None:
    """Test AWS Partner Central connectivity."""
    from .ace_client import ACEClient
    from .config import ACEConfig

    config = ACEConfig(catalog=catalog)
    client = ACEClient(config)

    if client.test_connection():
        print_status(f"ACE connection successful (catalog={catalog})", "success")
    else:
        print_status("ACE connection failed — check credentials", "error")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
