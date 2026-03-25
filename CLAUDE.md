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

## Interactive Setup Guide

When a user asks you to help them set up this project (e.g. "help me set up" or "guide me through setup"), follow this checklist. Read the relevant files first, then walk the user through each step interactively. Ask for one piece of information at a time — don't dump everything at once.

### Step 0: Prerequisites Check
Ask the user to confirm they have:
- [ ] Python 3.11+ installed
- [ ] AWS Partner Network membership with ACE API access enabled
- [ ] A HubSpot account with admin access (to create a Private App and custom properties)
- [ ] (Optional) A Slack workspace if they want notifications

### Step 1: Collect Credentials
Gather all credentials first, then write the `.env` file so that automated commands work in later steps.

#### 1a: AWS Credentials
Read the "AWS IAM Setup" section in `README.md` for the IAM policy. Walk the user through:
- [ ] Creating an IAM user with the `partnercentral-selling` permissions (show them the policy from the README)
- [ ] Getting the Access Key ID and Secret Access Key
- [ ] Ask them to provide: `AWS_ACE_ACCESS_KEY_ID` and `AWS_ACE_SECRET_ACCESS_KEY`

#### 1b: ACE Solution ID
- [ ] Tell the user: go to AWS Partner Central → Solutions → copy the Solution ID
- [ ] Ask them to provide: `ACE_SOLUTION_ID`

#### 1c: HubSpot Private App
Read the "HubSpot Setup" section in `README.md`. Walk the user through:
- [ ] Creating a Private App in HubSpot Settings → Integrations → Private Apps
- [ ] Required scopes (all of these are needed):
  - `crm.objects.deals.read` — read deals for sync
  - `crm.objects.deals.write` — write ACE IDs and sync status back to deals
  - `crm.objects.companies.read` — read company data for ACE payloads
  - `crm.objects.contacts.read` — read contact data for ACE payloads
  - `crm.schemas.deals.read` — read pipeline/stage definitions (used by `list-stages` command)
  - `crm.schemas.deals.write` — create custom deal properties (used by `setup-hubspot` command)
- [ ] Ask them to provide: `HUBSPOT_API_KEY` (the access token)
- [ ] Ask them to provide: `HUBSPOT_PORTAL_ID` (found in Settings → Account Information)

### Step 2: Write the Initial .env File
Write the `.env` file now with the credentials collected in Step 1. This is needed before running any CLI commands against HubSpot or AWS.
- [ ] Read `.env.example` for the template
- [ ] Create `.env` with the credentials filled in (stage mapping can use placeholders for now)
- [ ] Double-check nothing is missing by reading `src/config.py` and checking what `validate_config()` requires

**Important**: Never show or echo back credentials. Write the values directly into `.env` without displaying them in chat. Remind the user that `.env` is gitignored and should never be committed.

### Step 3: HubSpot Custom Properties
Now that `.env` exists with the HubSpot token, tell the user to run the automated setup command:
```bash
python -m src.main setup-hubspot
```
This creates all 12 required custom deal properties in a dedicated "AWS Partner Fields" group in HubSpot. Properties that already exist are safely skipped.

If their HubSpot uses different internal names for any of these properties, note they can override via `HS_*` env vars.

### Step 4: Stage Mapping (Most Important)
This is the hardest part. Read `src/config.py` to understand how `STAGE_MAPPING` works. Guide the user:
- [ ] Tell them to run `python -m src.main list-stages` to fetch their pipeline stages directly from HubSpot (no manual lookup needed)
- [ ] Explain the valid ACE stages: Qualified → Technical Validation → Business Validation → Committed → Launched → Closed Lost
- [ ] Ask them to map each of their HubSpot stages to an ACE stage
- [ ] Build the `STAGE_MAPPING`, `STAGE_DISPLAY_NAMES`, `SYNC_ELIGIBLE_STAGES`, and `SKIP_STAGES` values for them based on what they provide
- [ ] Update the `.env` file with the completed stage mapping values

### Step 5: Test
Run these commands in order and explain the output:
```bash
pip install -e ".[dev]"
python -m src.main test-connection        # Verify AWS credentials work
python -m src.main validate               # Check which deals are ready
python -m src.main sync --dry-run         # Preview without writing
```

### Step 6: Slack (Optional)
If the user wants Slack notifications:
- [ ] Guide them to create a Slack app with `chat:write` scope
- [ ] Ask for: `SLACK_BOT_TOKEN` and `ACE_SLACK_CHANNEL`
- [ ] Add to `.env`

### Step 7: Schedule (Optional)
Ask if they want to run on a schedule. Options:
- **GitHub Actions**: The included `.github/workflows/sync.yml` runs every 30 min during business hours. They need to add secrets to the repo (list which ones from the README).
- **Cron**: Give them the cron line from the README.

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
