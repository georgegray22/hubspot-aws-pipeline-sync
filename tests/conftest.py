"""Test fixtures for AWS HubSpot Sync project."""

import sys
import os
from pathlib import Path

import pytest

# Add parent of src to path so 'from src import ...' works
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Ensure required env vars for config loading
if not os.getenv("HUBSPOT_PIPELINE_ID"):
    os.environ["HUBSPOT_PIPELINE_ID"] = "default"
if not os.getenv("STAGE_MAPPING"):
    os.environ["STAGE_MAPPING"] = "qualified=Qualified;eval=Technical Validation"
if not os.getenv("SYNC_ELIGIBLE_STAGES"):
    os.environ["SYNC_ELIGIBLE_STAGES"] = "qualified,eval"
if not os.getenv("SKIP_STAGES"):
    os.environ["SKIP_STAGES"] = ""
if not os.getenv("HUBSPOT_PORTAL_ID"):
    os.environ["HUBSPOT_PORTAL_ID"] = "123456789"
if not os.getenv("HUBSPOT_API_KEY"):
    os.environ["HUBSPOT_API_KEY"] = "test-api-key-12345"


@pytest.fixture
def sample_deal_props():
    """Sample deal properties with generic company name."""
    return {
        "dealname": "Technology Partnership with Acme Corp",
        "dealstage": "qualified",
        "amount": "250000",
        "closedate": "2026-06-30T00:00:00Z",
        "pipeline": "default",
        "dealtype": "newbusiness",
        "description": "Cloud infrastructure evaluation and deployment",
        "contract_term__months_": "12",
        "hubspot_owner_id": "owner-123",
        "submit_to_aws": True,
        "ace_opportunity_id": None,
        "ace_sync_status": "not_synced",
        "ace_sync_error": None,
        "ace_project_description": "Comprehensive cloud migration and modernization initiative for enterprise customer",
        "loss_reason": None,
        "closed_lost_reason": None,
        "ace_aws_account_manager": None,
        "ace_aws_account_manager_email": None,
        "ace_aws_sales_rep": None,
        "ace_aws_sales_rep_email": None,
        "ace_aws_partner_sales_manager": None,
        "ace_aws_partner_development_manager": None,
    }


@pytest.fixture
def sample_company_props():
    """Sample company properties with generic name."""
    return {
        "name": "Acme Corporation",
        "website": "https://acme.com",
        "domain": "acme.com",
        "hs_country_code": "US",
        "country": "United States",
        "city": "San Francisco",
        "state": "California",
        "zip": "94105",
        "industry": "COMPUTER_SOFTWARE",
        "address": "535 Mission St",
    }


@pytest.fixture
def sample_deal_data(sample_deal_props):
    """Sample deal data wrapping properties."""
    return {
        "id": "12345",
        "properties": sample_deal_props,
    }


@pytest.fixture
def sample_owner():
    """Sample HubSpot deal owner."""
    return {
        "firstName": "John",
        "lastName": "Smith",
        "email": "john.smith@example.com",
        "phone": "+14155551234",
    }


@pytest.fixture
def sample_contact():
    """Sample contact associated with deal."""
    return {
        "firstname": "Jane",
        "lastname": "Doe",
        "email": "jane.doe@acme.com",
        "jobtitle": "Cloud Architect",
        "phone": "+14155555678",
    }


@pytest.fixture
def sample_ace_config():
    """Sample ACE configuration."""
    from src.config import ACEConfig

    return ACEConfig(
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        catalog="AWS",
        solution_id="sol-12345",
        dry_run=False,
    )
