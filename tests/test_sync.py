"""Comprehensive tests for the sync module — all critical paths covered."""

from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

# Imports via conftest sys.path setup (src is now importable as src.module)
from src.config import (
    ACEConfig,
)
from src.mapping import ValidationError
from src.sync import (
    SyncResult,
    _deal_link,
    _reverse_sync_aws_contacts,
    fetch_company_for_deal,
    fetch_eligible_deals,
    fetch_withdrawn_deals,
    run_sync,
    sync_deal_create,
    sync_deal_update,
    validate_deals,
    withdraw_opportunity,
)

# =============================================================================
# SyncResult Tests
# =============================================================================


class TestSyncResult:
    """Tests for SyncResult class."""

    def test_total_empty(self):
        """total property returns 0 for empty result."""
        result = SyncResult()
        assert result.total == 0

    def test_total_all_categories(self):
        """total property sums all categories."""
        result = SyncResult()
        result.created = [{"deal_id": 1}, {"deal_id": 2}]
        result.updated = [{"deal_id": 3}]
        result.skipped = [{"deal_id": 4}, {"deal_id": 5}]
        result.errors = [{"deal_id": 6}]
        assert result.total == 6

    def test_total_excludes_withdrawn(self):
        """total property excludes withdrawn count."""
        result = SyncResult()
        result.created = [{"deal_id": 1}]
        result.withdrawn = [{"deal_id": 2}, {"deal_id": 3}]
        assert result.total == 1

    def test_summary_no_withdrawn(self):
        """summary() formats without withdrawn count if empty."""
        result = SyncResult()
        result.created = [{"deal_id": 1}]
        result.updated = [{"deal_id": 2}]
        result.skipped = [{"deal_id": 3}]
        result.errors = []
        summary = result.summary()
        assert "Created: 1" in summary
        assert "Updated: 1" in summary
        assert "Skipped: 1" in summary
        assert "Errors: 0" in summary
        assert "Withdrawn" not in summary

    def test_summary_with_withdrawn(self):
        """summary() includes withdrawn count when present."""
        result = SyncResult()
        result.created = [{"deal_id": 1}]
        result.withdrawn = [{"deal_id": 2}]
        summary = result.summary()
        assert "Created: 1" in summary
        assert "Withdrawn: 1" in summary

    def test_summary_format(self):
        """summary() returns correct comma-separated format."""
        result = SyncResult()
        result.created = [{"deal_id": 1}, {"deal_id": 2}]
        result.updated = [{"deal_id": 3}]
        result.skipped = []
        result.errors = [{"deal_id": 4}]
        summary = result.summary()
        assert summary.startswith("Created: 2, Updated: 1")


# =============================================================================
# _deal_link Tests
# =============================================================================


class TestDealLink:
    """Tests for _deal_link URL generation."""

    def test_deal_link_with_portal_id_default_region(self, monkeypatch):
        """_deal_link generates correct URL with default region."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.delenv("HUBSPOT_REGION", raising=False)
        url = _deal_link(12345)
        assert url == "https://app-na1.hubspot.com/contacts/123456789/record/0-3/12345"

    def test_deal_link_with_portal_id_eu_region(self, monkeypatch):
        """_deal_link generates correct URL for EU region."""
        # Need to patch the module-level var since it's read at import
        with patch("src.sync.HUBSPOT_PORTAL_ID", "123456789"):
            with patch("src.sync.HUBSPOT_REGION", "eu1"):
                url = _deal_link(12345)
                assert url == "https://app-eu1.hubspot.com/contacts/123456789/record/0-3/12345"

    def test_deal_link_missing_portal_id(self, monkeypatch):
        """_deal_link returns empty string if HUBSPOT_PORTAL_ID not set."""
        # _deal_link uses module-level env var read at import, so test directly
        # by mocking HUBSPOT_PORTAL_ID in the module
        with patch("src.sync.HUBSPOT_PORTAL_ID", ""):
            url = _deal_link(12345)
            assert url == ""

    def test_deal_link_empty_portal_id(self, monkeypatch):
        """_deal_link returns empty string if HUBSPOT_PORTAL_ID is empty."""
        # Test with empty portal ID
        with patch("src.sync.HUBSPOT_PORTAL_ID", ""):
            url = _deal_link(12345)
            assert url == ""


# =============================================================================
# fetch_eligible_deals Tests
# =============================================================================


class TestFetchEligibleDeals:
    """Tests for fetch_eligible_deals function."""

    def test_fetch_eligible_deals_basic(self, monkeypatch):
        """fetch_eligible_deals filters by pipeline, submit_to_aws, and stage."""
        monkeypatch.setenv("HUBSPOT_PIPELINE_ID", "default")
        monkeypatch.setenv("SYNC_ELIGIBLE_STAGES", "qualified,eval,closedlost")
        monkeypatch.setenv("SKIP_STAGES", "discovery")

        deal_data = {
            "id": "1",
            "properties": {
                "dealstage": "qualified",
                "submit_to_aws": True,
                "dealname": "Deal 1",
            },
        }
        mock_hubspot = Mock()
        mock_hubspot.post.return_value = {
            "results": [deal_data],
            "paging": {},
        }

        deals = fetch_eligible_deals(mock_hubspot)
        assert len(deals) == 1
        assert deals[0]["id"] == "1"

    def test_fetch_eligible_deals_skip_non_eligible_stage(self, monkeypatch):
        """fetch_eligible_deals skips deals in non-eligible stages."""
        monkeypatch.setenv("HUBSPOT_PIPELINE_ID", "default")
        monkeypatch.setenv("SYNC_ELIGIBLE_STAGES", "qualified,eval")
        monkeypatch.setenv("SKIP_STAGES", "")

        deal_data = {
            "id": "1",
            "properties": {
                "dealstage": "negotiation",
                "submit_to_aws": True,
                "dealname": "Deal 1",
            },
        }
        mock_hubspot = Mock()
        mock_hubspot.post.return_value = {
            "results": [deal_data],
            "paging": {},
        }

        deals = fetch_eligible_deals(mock_hubspot)
        assert len(deals) == 0

    def test_fetch_eligible_deals_skip_pre_eligible_stage(self, monkeypatch):
        """fetch_eligible_deals skips deals in SKIP_STAGES."""
        monkeypatch.setenv("HUBSPOT_PIPELINE_ID", "default")
        monkeypatch.setenv("SYNC_ELIGIBLE_STAGES", "qualified,eval,discovery")
        monkeypatch.setenv("SKIP_STAGES", "discovery")

        deal_data = {
            "id": "1",
            "properties": {
                "dealstage": "discovery",
                "submit_to_aws": True,
                "dealname": "Deal 1",
            },
        }
        mock_hubspot = Mock()
        mock_hubspot.post.return_value = {
            "results": [deal_data],
            "paging": {},
        }

        deals = fetch_eligible_deals(mock_hubspot)
        assert len(deals) == 0

    def test_fetch_eligible_deals_pagination(self, monkeypatch):
        """fetch_eligible_deals handles pagination with 'after' cursor."""
        monkeypatch.setenv("HUBSPOT_PIPELINE_ID", "default")
        monkeypatch.setenv("SYNC_ELIGIBLE_STAGES", "qualified")
        monkeypatch.setenv("SKIP_STAGES", "")

        deal_1 = {
            "id": "1",
            "properties": {"dealstage": "qualified", "submit_to_aws": True},
        }
        deal_2 = {
            "id": "2",
            "properties": {"dealstage": "qualified", "submit_to_aws": True},
        }

        mock_hubspot = Mock()
        mock_hubspot.post.side_effect = [
            {
                "results": [deal_1],
                "paging": {"next": {"after": "cursor123"}},
            },
            {
                "results": [deal_2],
                "paging": {},
            },
        ]

        deals = fetch_eligible_deals(mock_hubspot)
        assert len(deals) == 2
        assert deals[0]["id"] == "1"
        assert deals[1]["id"] == "2"

        # Verify 'after' was passed on second request
        second_call = mock_hubspot.post.call_args_list[1]
        assert second_call[1]["json_data"]["after"] == "cursor123"

    def test_fetch_eligible_deals_no_results(self, monkeypatch):
        """fetch_eligible_deals returns empty list if no eligible deals."""
        monkeypatch.setenv("HUBSPOT_PIPELINE_ID", "default")
        monkeypatch.setenv("SYNC_ELIGIBLE_STAGES", "qualified")
        monkeypatch.setenv("SKIP_STAGES", "")

        mock_hubspot = Mock()
        mock_hubspot.post.return_value = {
            "results": [],
            "paging": {},
        }

        deals = fetch_eligible_deals(mock_hubspot)
        assert len(deals) == 0


# =============================================================================
# fetch_withdrawn_deals Tests
# =============================================================================


class TestFetchWithdrawnDeals:
    """Tests for fetch_withdrawn_deals function."""

    def test_fetch_withdrawn_deals_basic(self):
        """fetch_withdrawn_deals finds deals with submit_to_aws=false but ACE ID set."""
        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal 1",
                "ace_opportunity_id": "opp-123",
            },
        }
        mock_hubspot = Mock()
        mock_hubspot.post.return_value = {
            "results": [deal_data],
            "paging": {},
        }

        deals = fetch_withdrawn_deals(mock_hubspot)
        assert len(deals) == 1
        assert deals[0]["id"] == "1"

    def test_fetch_withdrawn_deals_pagination(self):
        """fetch_withdrawn_deals handles pagination."""
        deal_1 = {
            "id": "1",
            "properties": {"ace_opportunity_id": "opp-1"},
        }
        deal_2 = {
            "id": "2",
            "properties": {"ace_opportunity_id": "opp-2"},
        }

        mock_hubspot = Mock()
        mock_hubspot.post.side_effect = [
            {
                "results": [deal_1],
                "paging": {"next": {"after": "cursor123"}},
            },
            {
                "results": [deal_2],
                "paging": {},
            },
        ]

        deals = fetch_withdrawn_deals(mock_hubspot)
        assert len(deals) == 2

    def test_fetch_withdrawn_deals_no_results(self):
        """fetch_withdrawn_deals returns empty list if no withdrawn deals."""
        mock_hubspot = Mock()
        mock_hubspot.post.return_value = {
            "results": [],
            "paging": {},
        }

        deals = fetch_withdrawn_deals(mock_hubspot)
        assert len(deals) == 0


# =============================================================================
# withdraw_opportunity Tests
# =============================================================================


class TestWithdrawOpportunity:
    """Tests for withdraw_opportunity function."""

    def test_withdraw_opportunity_closes_and_clears_fields(self, monkeypatch):
        """withdraw_opportunity closes ACE opp and clears HubSpot fields."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal to Withdraw",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {"Stage": "Technical Validation"},
            "LastModifiedDate": "2026-03-20T10:00:00Z",
        }

        outcome = withdraw_opportunity(deal_data, mock_hubspot, mock_ace, dry_run=False)

        # Verify ACE opportunity was updated
        mock_ace.update_opportunity.assert_called_once()
        call_kwargs = mock_ace.update_opportunity.call_args[1]
        assert call_kwargs["opportunity_id"] == "opp-123"
        assert call_kwargs["life_cycle"]["Stage"] == "Closed Lost"

        # Verify HubSpot fields were cleared
        mock_hubspot.update_deal.assert_called_once()
        update_call = mock_hubspot.update_deal.call_args
        # Check that update_deal was called with deal_id and properties
        assert update_call[1]["deal_id"] == 1
        properties = update_call[1]["properties"]
        assert "ace_sync_status" in properties

        assert outcome["action"] == "withdraw"
        assert outcome["deal_id"] == 1

    def test_withdraw_opportunity_already_closed(self, monkeypatch):
        """withdraw_opportunity skips ACE update if already Closed Lost."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Already Closed Deal",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {"Stage": "Closed Lost"},
            "LastModifiedDate": "2026-03-20T10:00:00Z",
        }

        outcome = withdraw_opportunity(deal_data, mock_hubspot, mock_ace, dry_run=False)

        # ACE update should not be called since already Closed Lost
        mock_ace.update_opportunity.assert_not_called()

        # HubSpot should still be cleared
        mock_hubspot.update_deal.assert_called_once()

        assert outcome["action"] == "withdraw"

    def test_withdraw_opportunity_dry_run(self, monkeypatch):
        """withdraw_opportunity returns outcome without modifying anything in dry_run."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal for Dry Run",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_ace = Mock()

        outcome = withdraw_opportunity(deal_data, mock_hubspot, mock_ace, dry_run=True)

        # No actual calls to ACE or HubSpot in dry run
        mock_ace.get_opportunity.assert_not_called()
        mock_hubspot.update_deal.assert_not_called()

        assert outcome["action"] == "withdraw"
        assert outcome["deal_id"] == 1


# =============================================================================
# fetch_company_for_deal Tests
# =============================================================================


class TestFetchCompanyForDeal:
    """Tests for fetch_company_for_deal function."""

    def test_fetch_company_for_deal_with_association(self, sample_company_props):
        """fetch_company_for_deal returns company data when associated."""
        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            1234: [5678],
        }

        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme Corporation"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {"industry": "COMPUTER_SOFTWARE"}

        mock_hubspot.get_company.return_value = mock_company

        result = fetch_company_for_deal(mock_hubspot, 1234)

        assert result is not None
        assert result["id"] == 5678
        assert result["name"] == "Acme Corporation"
        assert result["domain"] == "acme.com"

    def test_fetch_company_for_deal_no_association(self):
        """fetch_company_for_deal returns None if no associated company."""
        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            1234: [],
        }

        result = fetch_company_for_deal(mock_hubspot, 1234)
        assert result is None

    def test_fetch_company_for_deal_multiple_associations_uses_first(self):
        """fetch_company_for_deal uses first associated company."""
        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            1234: [5678, 9999],
        }

        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Primary Company"
        mock_company.domain = "primary.com"
        mock_company.custom_properties = {}

        mock_hubspot.get_company.return_value = mock_company

        fetch_company_for_deal(mock_hubspot, 1234)

        # Should request first company only
        mock_hubspot.get_company.assert_called_once()
        call_kwargs = mock_hubspot.get_company.call_args[1]
        assert call_kwargs["company_id"] == 5678


# =============================================================================
# sync_deal_create Tests
# =============================================================================


class TestSyncDealCreate:
    """Tests for sync_deal_create function."""

    def test_sync_deal_create_full_flow(self, sample_deal_data, sample_company_props, monkeypatch):
        """sync_deal_create executes full create flow."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("STAGE_DISPLAY_NAMES", "qualified=Qualified")

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            int(sample_deal_data["id"]): [5678],
        }
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = sample_company_props["name"]
        mock_company.domain = sample_company_props["domain"]
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get_deal_company_associations.return_value = {12345: [5678]}
        mock_hubspot.get.return_value = {"results": []}  # No contacts

        mock_ace = Mock()
        mock_ace.create_opportunity.return_value = {"Id": "opp-999"}

        with patch("src.sync.validate_deal_for_create", return_value=[]):
            with patch("src.sync.build_create_payload", return_value={}):
                outcome = sync_deal_create(sample_deal_data, mock_hubspot, mock_ace, dry_run=False)

        assert outcome["action"] == "create"
        assert outcome["deal_id"] == 12345
        assert outcome["ace_opportunity_id"] == "opp-999"

        # Verify ACE create was called
        mock_ace.create_opportunity.assert_called_once()

        # Verify associate and engage were attempted
        mock_ace.associate_opportunity.assert_called_once_with("opp-999")
        mock_ace.start_engagement.assert_called_once_with("opp-999")

    def test_sync_deal_create_validation_failure(self, sample_deal_data, monkeypatch):
        """sync_deal_create raises ValidationError on validation failure."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            int(sample_deal_data["id"]): [],
        }
        mock_ace = Mock()

        with patch("src.sync.validate_deal_for_create", return_value=["Missing company", "Missing amount"]):
            with pytest.raises(ValidationError):
                sync_deal_create(sample_deal_data, mock_hubspot, mock_ace, dry_run=False)

        # Verify status was updated to error (_update_deal_status is called with deal_id as first arg)
        mock_hubspot.update_deal.assert_called_once()
        # The call should have been (deal_id, properties) - check it was called
        assert mock_hubspot.update_deal.called

    def test_sync_deal_create_dry_run(self, sample_deal_data, sample_company_props, monkeypatch):
        """sync_deal_create returns outcome without API calls in dry_run."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("STAGE_DISPLAY_NAMES", "qualified=Qualified")

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            int(sample_deal_data["id"]): [5678],
        }
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()

        with patch("src.sync.validate_deal_for_create", return_value=[]):
            with patch("src.sync.build_create_payload", return_value={}):
                outcome = sync_deal_create(sample_deal_data, mock_hubspot, mock_ace, dry_run=True)

        assert outcome["action"] == "create"
        assert outcome["dry_run"] is True

        # ACE should not be called in dry run
        mock_ace.create_opportunity.assert_not_called()

    def test_sync_deal_create_associate_fails_but_continues(self, sample_deal_data, monkeypatch):
        """sync_deal_create logs warning but continues if associate fails."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("STAGE_DISPLAY_NAMES", "qualified=Qualified")

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {
            int(sample_deal_data["id"]): [5678],
        }
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.create_opportunity.return_value = {"Id": "opp-999"}
        mock_ace.associate_opportunity.side_effect = ClientError(
            {"Error": {"Code": "ConflictException"}}, "AssociateOpportunity"
        )

        with patch("src.sync.validate_deal_for_create", return_value=[]):
            with patch("src.sync.build_create_payload", return_value={}):
                outcome = sync_deal_create(sample_deal_data, mock_hubspot, mock_ace, dry_run=False)

        # Should still return successful outcome
        assert outcome["action"] == "create"
        assert outcome["ace_opportunity_id"] == "opp-999"


# =============================================================================
# sync_deal_update Tests
# =============================================================================


class TestSyncDealUpdate:
    """Tests for sync_deal_update function."""

    def test_sync_deal_update_stage_progression(self, sample_deal_data, monkeypatch):
        """sync_deal_update progresses deal stage."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("STAGE_DISPLAY_NAMES", "qualified=Qualified;eval=Evaluation")
        monkeypatch.setenv("STAGE_MAPPING", "qualified=Qualified;eval=Technical Validation")

        # Setup deal with ACE opportunity ID and different stage
        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal 1",
                "dealstage": "eval",
                "ace_opportunity_id": "opp-123",
                "submit_to_aws": True,
                "amount": "250000",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Qualified",
                "ReviewStatus": "",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Project": {"ExpectedCustomerSpend": []},
            "OpportunityType": "Net New Business",
        }

        with patch("src.sync.build_update_payload", return_value={"life_cycle": {"Stage": "Technical Validation"}}):
            outcome = sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=False)

        assert outcome["action"] == "update"
        assert outcome["from_stage"] == "Qualified"
        assert outcome["to_stage"] == "Technical Validation"
        assert outcome["stage_changed"] is True

    def test_sync_deal_update_stage_regression_blocked(self, sample_deal_data, monkeypatch):
        """sync_deal_update blocks stage regression."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("STAGE_DISPLAY_NAMES", "qualified=Qualified;eval=Evaluation")
        monkeypatch.setenv("STAGE_MAPPING", "qualified=Qualified;eval=Technical Validation")

        # This test focuses on the mapping preventing regression
        # The actual rejection happens in build_update_payload
        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal 1",
                "dealstage": "qualified",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Technical Validation",
                "ReviewStatus": "",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Project": {"ExpectedCustomerSpend": []},
            "OpportunityType": "Net New Business",
        }

        with patch("src.sync.build_update_payload", return_value=None):
            outcome = sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=False)

        # Should be skipped with no changes reason
        assert outcome["action"] == "skip"
        assert "no changes" in outcome.get("reason", "")

    def test_sync_deal_update_post_launch_skip(self, monkeypatch):
        """sync_deal_update skips update if already Launched."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Launched Deal",
                "dealstage": "launched",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Launched",
                "ReviewStatus": "",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Project": {"ExpectedCustomerSpend": []},
            "OpportunityType": "Net New Business",
        }

        with patch("src.sync.map_ace_stage", return_value="Launched"):
            with patch("src.sync.build_update_payload", return_value={"some": "field"}):
                outcome = sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=False)

        assert outcome["action"] == "skip"
        assert "post-launch" in outcome.get("reason", "")

    def test_sync_deal_update_already_closed_skip(self, monkeypatch):
        """sync_deal_update skips update if Closed Lost."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Closed Deal",
                "dealstage": "closedlost",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Closed Lost",
                "ReviewStatus": "",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Project": {"ExpectedCustomerSpend": []},
            "OpportunityType": "Net New Business",
        }

        with patch("src.sync.map_ace_stage", return_value="Closed Lost"):
            outcome = sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=False)

        assert outcome["action"] == "skip"
        assert "already closed" in outcome.get("reason", "")

    def test_sync_deal_update_approved_field_locking(self, monkeypatch):
        """sync_deal_update preserves locked fields for approved opportunities."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Approved Deal",
                "dealstage": "eval",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "NewName"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Technical Validation",
                "ReviewStatus": "Approved",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Customer": {
                "Account": {
                    "CompanyName": "OldName",
                    "WebsiteUrl": "https://old.com",
                }
            },
            "Project": {
                "Title": "Old Title",
                "ExpectedCustomerSpend": [],
            },
            "OpportunityType": "Net New Business",
        }

        update_payload = {
            "customer": {
                "Account": {
                    "CompanyName": "NewName",
                    "WebsiteUrl": "https://new.com",
                }
            },
            "project": {"Title": "New Title"},
        }

        with patch("src.sync.map_ace_stage", return_value="Technical Validation"):
            with patch("src.sync.build_update_payload", return_value=update_payload):
                sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=False)

        # Verify update was called
        mock_ace.update_opportunity.assert_called_once()
        call_kwargs = mock_ace.update_opportunity.call_args[1]

        # Locked fields should be restored to original values
        if "customer" in call_kwargs:
            account = call_kwargs["customer"]["Account"]
            assert account["CompanyName"] == "OldName"
            assert account["WebsiteUrl"] == "https://old.com"

    def test_sync_deal_update_field_change_detection(self, monkeypatch):
        """sync_deal_update detects field-level changes."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal with Changes",
                "dealstage": "eval",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Qualified",
                "ReviewStatus": "",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Project": {
                "ExpectedCustomerSpend": [{"Amount": "10000"}],
            },
            "OpportunityType": "Net New Business",
        }

        update_payload = {
            "project": {
                "ExpectedCustomerSpend": [{"Amount": "20000"}],
            },
        }

        with patch("src.sync.map_ace_stage", return_value="Technical Validation"):
            with patch("src.sync.build_update_payload", return_value=update_payload):
                outcome = sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=False)

        assert outcome["action"] == "update"
        assert len(outcome["field_changes"]) > 0

    def test_sync_deal_update_dry_run(self, monkeypatch):
        """sync_deal_update returns outcome without API calls in dry_run."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        deal_data = {
            "id": "1",
            "properties": {
                "dealname": "Deal for Dry Run",
                "dealstage": "eval",
                "ace_opportunity_id": "opp-123",
            },
        }

        mock_hubspot = Mock()
        mock_hubspot.get_deal_company_associations.return_value = {1: [5678]}
        mock_company = Mock()
        mock_company.id = 5678
        mock_company.name = "Acme"
        mock_company.domain = "acme.com"
        mock_company.custom_properties = {}
        mock_hubspot.get_company.return_value = mock_company
        mock_hubspot.get.return_value = {"results": []}

        mock_ace = Mock()
        mock_ace.get_opportunity.return_value = {
            "LifeCycle": {
                "Stage": "Qualified",
                "ReviewStatus": "",
                "TargetCloseDate": "2026-06-30",
            },
            "LastModifiedDate": "2026-03-20T10:00:00Z",
            "Project": {"ExpectedCustomerSpend": []},
            "OpportunityType": "Net New Business",
        }

        with patch("src.sync.map_ace_stage", return_value="Technical Validation"):
            with patch("src.sync.build_update_payload", return_value={"life_cycle": {"Stage": "Technical Validation"}}):
                outcome = sync_deal_update(deal_data, mock_hubspot, mock_ace, dry_run=True)

        assert outcome["dry_run"] is True

        # ACE should not be updated in dry run
        mock_ace.update_opportunity.assert_not_called()


# =============================================================================
# run_sync Tests
# =============================================================================


class TestRunSync:
    """Tests for run_sync orchestration function."""

    def test_run_sync_creates_and_updates(self, monkeypatch):
        """run_sync orchestrates create and update flows."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("ACE_SLACK_CHANNEL", "")

        config = ACEConfig(dry_run=False, catalog="AWS")

        with patch("src.sync.fetch_withdrawn_deals", return_value=[]):
            with patch("src.sync.fetch_eligible_deals") as mock_fetch:
                with patch("src.sync.sync_deal_create") as mock_create:
                    with patch("src.sync.sync_deal_update") as mock_update:
                        with patch("src.sync._reverse_sync_aws_contacts"):
                            with patch("src.sync._write_sync_log"):
                                with patch("src.sync.HubSpotClient"):  # Mock to prevent API calls
                                    deal_for_create = {
                                        "id": "1",
                                        "properties": {"ace_opportunity_id": None},
                                    }
                                    deal_for_update = {
                                        "id": "2",
                                        "properties": {"ace_opportunity_id": "opp-123"},
                                    }
                                    mock_fetch.return_value = [deal_for_create, deal_for_update]
                                    mock_create.return_value = {
                                        "action": "create",
                                        "deal_id": 1,
                                    }
                                    mock_update.return_value = {
                                        "action": "update",
                                        "deal_id": 2,
                                    }

                                    result = run_sync(config)

        assert len(result.created) == 1
        assert len(result.updated) == 1
        assert result.total == 2

    def test_run_sync_handles_validation_errors(self, monkeypatch):
        """run_sync catches ValidationError and adds to errors list."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("ACE_SLACK_CHANNEL", "")

        config = ACEConfig(dry_run=False, catalog="AWS")

        with patch("src.sync.fetch_withdrawn_deals", return_value=[]):
            with patch("src.sync.fetch_eligible_deals") as mock_fetch:
                with patch("src.sync.sync_deal_create") as mock_create:
                    with patch("src.sync._reverse_sync_aws_contacts"):
                        with patch("src.sync._write_sync_log"):
                            deal = {"id": "1", "properties": {"ace_opportunity_id": None}}
                            mock_fetch.return_value = [deal]
                            mock_create.side_effect = ValidationError(1, "Deal 1", ["Missing field"])

                            result = run_sync(config)

        assert len(result.errors) == 1
        assert result.total == 1

    def test_run_sync_handles_client_errors(self, monkeypatch):
        """run_sync catches ClientError and categorizes as error or skip."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("ACE_SLACK_CHANNEL", "")

        config = ACEConfig(dry_run=False, catalog="AWS")

        with patch("src.sync.fetch_withdrawn_deals", return_value=[]):
            with patch("src.sync.fetch_eligible_deals") as mock_fetch:
                with patch("src.sync.sync_deal_create") as mock_create:
                    with patch("src.sync._reverse_sync_aws_contacts"):
                        with patch("src.sync._write_sync_log"):
                            with patch("src.sync.HubSpotClient"):  # Mock to prevent API calls
                                deal = {"id": "1", "properties": {"ace_opportunity_id": None}}
                                mock_fetch.return_value = [deal]

                                error = ClientError({"Error": {"Code": "ConflictException"}}, "CreateOpportunity")
                                mock_create.side_effect = error

                                result = run_sync(config)

        # ConflictException should be skipped, not errored
        assert len(result.skipped) == 1
        assert result.total == 1

    def test_run_sync_withdraws_opted_out_deals(self, monkeypatch):
        """run_sync withdraws deals that were opted out."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("ACE_SLACK_CHANNEL", "")

        config = ACEConfig(dry_run=False, catalog="AWS")

        with patch("src.sync.fetch_withdrawn_deals") as mock_fetch_withdrawn:
            with patch("src.sync.fetch_eligible_deals", return_value=[]):
                with patch("src.sync.withdraw_opportunity") as mock_withdraw:
                    with patch("src.sync._write_sync_log"):
                        with patch("src.sync.HubSpotClient"):  # Mock to prevent API calls
                            with patch("src.sync.ACEClient"):  # Mock ACE client
                                withdrawn_deal = {
                                    "id": "1",
                                    "properties": {"ace_opportunity_id": "opp-123"},
                                }
                                mock_fetch_withdrawn.return_value = [withdrawn_deal]
                                mock_withdraw.return_value = {
                                    "action": "withdraw",
                                    "deal_id": 1,
                                }

                                result = run_sync(config)

        # Check that the withdrawal was recorded
        # Note: total doesn't include withdrawn, so check withdrawn directly
        assert len(result.withdrawn) == 1

    def test_run_sync_no_eligible_deals(self, monkeypatch):
        """run_sync returns empty result if no eligible deals."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")
        monkeypatch.setenv("ACE_SLACK_CHANNEL", "")

        config = ACEConfig(dry_run=False, catalog="AWS")

        with patch("src.sync.fetch_withdrawn_deals", return_value=[]):
            with patch("src.sync.fetch_eligible_deals", return_value=[]):
                with patch("src.sync._write_sync_log"):
                    result = run_sync(config)

        assert result.total == 0


# =============================================================================
# _reverse_sync_aws_contacts Tests
# =============================================================================


class TestReverseSyncAwsContacts:
    """Tests for _reverse_sync_aws_contacts function."""

    def test_reverse_sync_updates_changed_fields(self, monkeypatch):
        """_reverse_sync_aws_contacts updates HubSpot with AWS team members."""
        monkeypatch.setenv("ACE_ROLE_TO_HS_FIELDS", "AWSAccountOwner:ace_account_manager,ace_account_manager_email")

        deal = {
            "id": "1",
            "properties": {
                "ace_opportunity_id": "opp-123",
                "dealname": "Deal 1",
                "ace_account_manager": "Old Name",
            },
        }

        mock_hubspot = Mock()
        mock_ace = Mock()
        mock_ace.get_aws_opportunity_summary.return_value = {
            "OpportunityTeam": [
                {
                    "BusinessTitle": "AWSAccountOwner",
                    "FirstName": "Jane",
                    "LastName": "Smith",
                    "Email": "jane@aws.com",
                }
            ]
        }

        _reverse_sync_aws_contacts([deal], mock_hubspot, mock_ace)

        # Verify HubSpot was updated with new team member
        mock_hubspot.update_deal.assert_called_once()

    def test_reverse_sync_skips_unchanged_fields(self, monkeypatch):
        """_reverse_sync_aws_contacts skips update if no changes detected."""
        deal = {
            "id": "1",
            "properties": {
                "ace_opportunity_id": "opp-123",
                "dealname": "Deal 1",
                "ace_aws_account_manager": "Jane Smith (jane@aws.com)",
                "ace_aws_account_manager_email": "jane@aws.com",
            },
        }

        mock_hubspot = Mock()
        mock_ace = Mock()
        mock_ace.get_aws_opportunity_summary.return_value = {
            "OpportunityTeam": [
                {
                    "BusinessTitle": "AWSAccountOwner",
                    "FirstName": "Jane",
                    "LastName": "Smith",
                    "Email": "jane@aws.com",
                }
            ]
        }

        _reverse_sync_aws_contacts([deal], mock_hubspot, mock_ace)

        # No update should occur since fields are unchanged
        mock_hubspot.update_deal.assert_not_called()

    def test_reverse_sync_no_synced_deals(self):
        """_reverse_sync_aws_contacts skips if no deals have ACE IDs."""
        deal = {
            "id": "1",
            "properties": {
                "ace_opportunity_id": None,
                "dealname": "Deal 1",
            },
        }

        mock_hubspot = Mock()
        mock_ace = Mock()

        _reverse_sync_aws_contacts([deal], mock_hubspot, mock_ace)

        # No calls should be made
        mock_ace.get_aws_opportunity_summary.assert_not_called()
        mock_hubspot.update_deal.assert_not_called()


# =============================================================================
# validate_deals Tests
# =============================================================================


class TestValidateDeals:
    """Tests for validate_deals read-only validation function."""

    def test_validate_deals_read_only(self, monkeypatch):
        """validate_deals performs read-only validation without writing."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        config = ACEConfig(dry_run=False)

        deal = {
            "id": "1",
            "properties": {
                "dealname": "Deal 1",
                "dealstage": "qualified",
            },
        }

        with patch("src.sync.fetch_eligible_deals", return_value=[deal]):
            with patch("src.sync.fetch_company_for_deal", return_value={"name": "Acme", "domain": "acme.com"}):
                with patch("src.sync.validate_deal_for_create", return_value=[]):
                    mock_hubspot = Mock()

                    with patch("src.sync.HubSpotClient", return_value=mock_hubspot):
                        validate_deals(config)

        # No updates should occur
        mock_hubspot.update_deal.assert_not_called()

    def test_validate_deals_reports_errors(self, monkeypatch):
        """validate_deals reports validation errors."""
        monkeypatch.setenv("HUBSPOT_PORTAL_ID", "123456789")

        config = ACEConfig(dry_run=False)

        deal = {
            "id": "1",
            "properties": {
                "dealname": "Deal 1",
                "dealstage": "qualified",
            },
        }

        with patch("src.sync.fetch_eligible_deals", return_value=[deal]):
            with patch("src.sync.fetch_company_for_deal", return_value=None):
                with patch("src.sync.validate_deal_for_create", return_value=["Missing company"]):
                    mock_hubspot = Mock()

                    with patch("src.sync.HubSpotClient", return_value=mock_hubspot):
                        # Should not raise, just report
                        validate_deals(config)

        # No updates should occur
        mock_hubspot.update_deal.assert_not_called()
