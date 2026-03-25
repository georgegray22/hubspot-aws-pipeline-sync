"""AWS HubSpot Sync — sync HubSpot deals to AWS Partner Central (ACE Pipeline Manager)."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file at project root
load_dotenv(Path(__file__).parent.parent / ".env")

import typer  # noqa: E402

from .logger import get_logger, print_status  # noqa: E402

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


@app.command(name="list-stages")
def list_stages(
    pipeline_id: str = typer.Option("default", help="HubSpot pipeline ID to list stages for"),
) -> None:
    """List all deal pipelines and stages from HubSpot.

    Use this to find your stage IDs for configuring STAGE_MAPPING.
    Requires a HubSpot Private App token with crm.schemas.deals.read scope.
    """
    from .hubspot_client import HubSpotClient

    client = HubSpotClient()

    pipelines = client.get_deal_pipelines()
    if not pipelines:
        print_status("No deal pipelines found", "error")
        raise typer.Exit(code=1)

    for pipeline in pipelines:
        is_target = " ← current" if pipeline["id"] == pipeline_id else ""
        print_status(f"Pipeline: {pipeline['label']} (id: {pipeline['id']}){is_target}", "info")

        stages = sorted(pipeline.get("stages", []), key=lambda s: s.get("displayOrder", 0))
        for stage in stages:
            print(f"    {stage['id']:30s}  {stage['label']}")
        print()

    print_status(
        "Copy the stage IDs (left column) to build your STAGE_MAPPING in .env",
        "info",
    )
    print(
        "  Valid ACE stages: Qualified, Technical Validation, Business Validation, " "Committed, Launched, Closed Lost"
    )


@app.command(name="setup-hubspot")
def setup_hubspot() -> None:
    """Create all required custom deal properties in HubSpot.

    Run this once during initial setup. Requires a HubSpot Private App token
    with crm.objects.deals.write and crm.schemas.deals.read scopes.
    Already-existing properties are skipped safely.
    """
    from .hubspot_client import HubSpotClient

    client = HubSpotClient()

    group_name = "aws_partner_fields"

    # Create property group first
    group_result = client.create_deal_property_group(
        {"name": group_name, "label": "AWS Partner Fields", "displayOrder": -1}
    )
    if group_result is None:
        print_status("Property group 'AWS Partner Fields' — already exists, skipped", "info")
    else:
        print_status("Property group 'AWS Partner Fields' — created", "success")
    print()

    properties = [
        {
            "name": "submit_to_aws",
            "label": "Submit to AWS",
            "type": "bool",
            "fieldType": "booleancheckbox",
            "groupName": group_name,
            "description": "Check to sync this deal to AWS Partner Central (ACE)",
        },
        {
            "name": "ace_opportunity_id",
            "label": "ACE Opportunity ID",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS ACE opportunity ID (auto-populated by sync)",
        },
        {
            "name": "ace_sync_status",
            "label": "ACE Sync Status",
            "type": "enumeration",
            "fieldType": "select",
            "groupName": group_name,
            "description": "Current sync status with AWS ACE",
            "options": [
                {"label": "Not Synced", "value": "not_synced", "displayOrder": 0},
                {"label": "Pending Review", "value": "pending_review", "displayOrder": 1},
                {"label": "Synced", "value": "Synced", "displayOrder": 2},
                {"label": "Sync Error", "value": "Sync Error", "displayOrder": 3},
                {"label": "Rejected", "value": "Rejected", "displayOrder": 4},
            ],
        },
        {
            "name": "ace_last_sync",
            "label": "ACE Last Sync",
            "type": "datetime",
            "fieldType": "date",
            "groupName": group_name,
            "description": "Timestamp of last successful ACE sync",
        },
        {
            "name": "ace_sync_error",
            "label": "ACE Sync Error",
            "type": "string",
            "fieldType": "textarea",
            "groupName": group_name,
            "description": "Error message from last failed ACE sync attempt",
        },
        {
            "name": "ace_project_description",
            "label": "ACE Project Description",
            "type": "string",
            "fieldType": "textarea",
            "groupName": group_name,
            "description": "Project description sent to AWS ACE (min 20 characters)",
        },
        {
            "name": "ace_aws_account_manager",
            "label": "AWS Account Manager",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS Account Manager name (synced from ACE)",
        },
        {
            "name": "ace_aws_account_manager_email",
            "label": "AWS Account Manager Email",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS Account Manager email (synced from ACE)",
        },
        {
            "name": "ace_aws_sales_rep",
            "label": "AWS Sales Rep",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS Sales Rep name (synced from ACE)",
        },
        {
            "name": "ace_aws_sales_rep_email",
            "label": "AWS Sales Rep Email",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS Sales Rep email (synced from ACE)",
        },
        {
            "name": "ace_aws_partner_sales_manager",
            "label": "AWS Partner Sales Manager",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS Partner Sales Manager (synced from ACE)",
        },
        {
            "name": "ace_aws_partner_development_manager",
            "label": "AWS Partner Development Manager",
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
            "description": "AWS Partner Development Manager (synced from ACE)",
        },
    ]

    created = 0
    skipped = 0

    for prop in properties:
        result = client.create_deal_property(prop)
        if result is None:
            print_status(f"{prop['label']} — already exists, skipped", "info")
            skipped += 1
        else:
            print_status(f"{prop['label']} — created", "success")
            created += 1

    print()
    print_status(f"Done! Created {created}, skipped {skipped} (already existed)", "success")


if __name__ == "__main__":
    app()
