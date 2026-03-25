# CLAUDE.md

Guidance for Claude Code and AI assistants working with this project.

## Project Overview

AWS HubSpot Sync is a standalone Python tool that syncs HubSpot CRM deals to AWS Partner Central (ACE Pipeline Manager). It runs as a CLI application and can be scheduled via cron or GitHub Actions.

## Quick Reference

```bash
# Install
pip install -e ".[dev]"

# Run
python -m src.main sync --catalog Sandbox --dry-run
python -m src.main sync --catalog AWS
python -m src.main validate
python -m src.main test-connection

# Test
pytest -v

# Lint
black src/ tests/ --check
isort src/ tests/ --check
flake8 src/ tests/
```

## Architecture

```
src/main.py          → Typer CLI (sync, validate, test-connection commands)
src/config.py        → All configuration, loaded from environment variables
src/ace_client.py    → boto3 wrapper for AWS Partner Central Selling API
src/mapping.py       → HubSpot deal/company → ACE payload transformation
src/sync.py          → Core orchestration: fetch deals, create/update/withdraw
src/hubspot_client.py → Standalone HubSpot CRM API client (requests-based)
src/slack_client.py  → Standalone Slack Web API client (optional notifications)
src/logger.py        → Lightweight logging utilities
```

## Key Concepts

### Sync Flow

1. **Fetch eligible deals**: `submit_to_aws=true` + correct pipeline + eligible stage
2. **Create flow** (no ACE ID yet): Validate → CreateOpportunity → AssociateOpportunity → StartEngagement → Write ACE ID back to HubSpot
3. **Update flow** (has ACE ID): Fetch current ACE state → Build delta → UpdateOpportunity → Update HubSpot status
4. **Withdrawal**: When `submit_to_aws` is unchecked but ACE ID exists → Close ACE opportunity → Clear HubSpot fields
5. **Reverse sync**: Pull AWS-assigned team members back into HubSpot deal properties

### Stage Mapping

HubSpot stages are mapped to ACE stages via the `STAGE_MAPPING` environment variable. The mapping is loaded at startup from a semicolon-separated string: `"hs_stage_id=ACE Stage;hs_stage_id2=ACE Stage2"`.

Valid ACE stages (in order): Qualified → Technical Validation → Business Validation → Committed → Launched → Closed Lost.

Stage regression (moving backwards) is blocked automatically.

### ACE API Write Pattern

All writes enforce a 1-second delay (ACE rate limit). The create flow requires 3 sequential API calls: CreateOpportunity → AssociateOpportunity → StartEngagement. If associate or engage fail, they're retried on the next sync run.

### Configuration

Everything is configured via environment variables (loaded from `.env`). Key categories:

- **AWS credentials**: `AWS_ACE_ACCESS_KEY_ID`, `AWS_ACE_SECRET_ACCESS_KEY`
- **ACE settings**: `ACE_CATALOG`, `ACE_SOLUTION_ID`, `ACE_USER_AGENT`
- **HubSpot**: `HUBSPOT_API_KEY`, `HUBSPOT_PORTAL_ID`, `HUBSPOT_PIPELINE_ID`
- **Stage mapping**: `STAGE_MAPPING`, `STAGE_DISPLAY_NAMES`, `SYNC_ELIGIBLE_STAGES`, `SKIP_STAGES`
- **Slack** (optional): `SLACK_BOT_TOKEN`, `ACE_SLACK_CHANNEL`
- **Field name overrides**: `HS_SUBMIT_FIELD`, `HS_ACE_OPP_ID_FIELD`, etc.

## Common Tasks

### Adding a new HubSpot field to the sync

1. Add the HubSpot property name constant to `config.py`
2. Add it to `ACE_DEAL_PROPERTIES` or `ACE_COMPANY_PROPERTIES` list
3. Use it in `mapping.py` payload builders
4. Add test coverage in `tests/test_mapping.py`

### Adding a new ACE field to the payload

1. Update `build_create_payload()` and/or `build_update_payload()` in `mapping.py`
2. Check AWS documentation for which fields are required vs optional on create vs update
3. Check if the field becomes immutable after AWS approval (see `sync_deal_update` in `sync.py`)

### Changing the deal eligibility criteria

Edit `fetch_eligible_deals()` in `sync.py`. The current filter is: `submit_to_aws=true AND pipeline=PIPELINE_ID`, then client-side stage filtering.

### Adding a new notification channel

The Slack notification is in `_send_slack_summary()` in `sync.py`. Add similar functions for other channels (Teams, email, etc.).

## Testing

Tests use `pytest` with generic fixture data (no real company names). ACE client tests mock boto3 entirely.

```bash
# Run all tests
pytest -v

# Run specific test file
pytest tests/test_mapping.py -v

# Run with coverage
pytest --cov=src -v
```

## Environment Variables Reference

See `.env.example` for the complete list with descriptions. The minimum required for a sync run:

```
AWS_ACE_ACCESS_KEY_ID=...
AWS_ACE_SECRET_ACCESS_KEY=...
ACE_SOLUTION_ID=...
HUBSPOT_API_KEY=...
HUBSPOT_PIPELINE_ID=default
STAGE_MAPPING="qualified=Qualified;closedlost=Closed Lost"
SYNC_ELIGIBLE_STAGES="qualified,closedlost"
```

## Gotchas

- **HubSpot stage IDs are account-specific**: Every HubSpot account has its own internal stage IDs (some use slugs, some use numeric IDs). Always verify your stage IDs via the HubSpot API or Settings UI before configuring `STAGE_MAPPING`.
- **ACE Sandbox vs Production**: Use `--catalog Sandbox` for testing. The Sandbox has the same API but separate data.
- **OpportunityTeam is write-once**: Can only be set during create, not on updates.
- **Approved opportunities lock fields**: CompanyName, WebsiteUrl, Industry, Title, CustomerBusinessProblem become read-only but must still be sent on updates (pass back existing ACE values).
- **Closed Lost is terminal**: No further updates allowed on ACE side.
- **Write rate limit**: 1 request/second for ACE write operations. The client handles this.
