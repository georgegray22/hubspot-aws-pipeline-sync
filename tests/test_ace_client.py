"""Tests for the ACE client (boto3 wrapper)."""

from unittest.mock import patch

import pytest

from src.ace_client import ACEClient


@pytest.fixture
def mock_boto3_client():
    """Mock boto3 client."""
    with patch("src.ace_client.boto3.client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def ace_client(sample_ace_config, mock_boto3_client):
    """ACE client instance with mocked boto3."""
    return ACEClient(sample_ace_config)


class TestCreateOpportunity:
    """Test create_opportunity method."""

    def test_create_with_correct_params(self, ace_client, mock_boto3_client):
        """Create opportunity should pass correct parameters to boto3."""
        mock_boto3_client.create_opportunity.return_value = {"Id": "opp-12345"}

        ace_client.create_opportunity(
            client_token="token-123",
            customer={"Account": {"CompanyName": "Acme Corp"}},
            project={"Title": "Test Project"},
            life_cycle={"Stage": "Qualified"},
            opportunity_type="Net New Business",
        )

        # Verify boto3 was called
        assert mock_boto3_client.create_opportunity.called
        call_kwargs = mock_boto3_client.create_opportunity.call_args.kwargs
        assert call_kwargs["Catalog"] == "AWS"
        assert call_kwargs["ClientToken"] == "token-123"
        assert call_kwargs["OpportunityType"] == "Net New Business"

    def test_create_includes_opportunity_type(self, ace_client, mock_boto3_client):
        """Create should include OpportunityType in call."""
        mock_boto3_client.create_opportunity.return_value = {"Id": "opp-12345"}

        ace_client.create_opportunity(
            client_token="token-123",
            customer={"Account": {"CompanyName": "Acme Corp"}},
            project={"Title": "Test Project"},
            life_cycle={"Stage": "Qualified"},
            opportunity_type="Expansion",
        )

        call_kwargs = mock_boto3_client.create_opportunity.call_args.kwargs
        assert call_kwargs["OpportunityType"] == "Expansion"

    def test_create_with_optional_team(self, ace_client, mock_boto3_client):
        """Create should include optional OpportunityTeam."""
        mock_boto3_client.create_opportunity.return_value = {"Id": "opp-12345"}

        team = [{"FirstName": "John", "LastName": "Smith", "Email": "john@example.com"}]
        ace_client.create_opportunity(
            client_token="token-123",
            customer={"Account": {"CompanyName": "Acme Corp"}},
            project={"Title": "Test Project"},
            life_cycle={"Stage": "Qualified"},
            opportunity_team=team,
        )

        call_kwargs = mock_boto3_client.create_opportunity.call_args.kwargs
        assert call_kwargs["OpportunityTeam"] == team

    def test_create_returns_response(self, ace_client, mock_boto3_client):
        """Create should return boto3 response."""
        expected_response = {"Id": "opp-12345", "ClientToken": "token-123"}
        mock_boto3_client.create_opportunity.return_value = expected_response

        result = ace_client.create_opportunity(
            client_token="token-123",
            customer={"Account": {"CompanyName": "Acme Corp"}},
            project={"Title": "Test Project"},
            life_cycle={"Stage": "Qualified"},
        )

        assert result == expected_response


class TestUpdateOpportunity:
    """Test update_opportunity method."""

    def test_update_passes_last_modified_date(self, ace_client, mock_boto3_client):
        """Update should pass LastModifiedDate to boto3."""
        mock_boto3_client.update_opportunity.return_value = {"Id": "opp-12345"}

        ace_client.update_opportunity(
            opportunity_id="opp-12345",
            last_modified_date="2026-03-23T10:00:00Z",
            life_cycle={"Stage": "Technical Validation"},
        )

        call_kwargs = mock_boto3_client.update_opportunity.call_args.kwargs
        assert call_kwargs["Identifier"] == "opp-12345"
        assert call_kwargs["LastModifiedDate"] == "2026-03-23T10:00:00Z"

    def test_update_includes_all_optional_params(self, ace_client, mock_boto3_client):
        """Update should include all optional parameters when provided."""
        mock_boto3_client.update_opportunity.return_value = {"Id": "opp-12345"}

        ace_client.update_opportunity(
            opportunity_id="opp-12345",
            last_modified_date="2026-03-23T10:00:00Z",
            life_cycle={"Stage": "Technical Validation"},
            project={"Title": "Updated Project"},
            customer={"Account": {"CompanyName": "Acme Corp"}},
            marketing={"Source": "Direct"},
            primary_needs_from_aws=["Co-Sell"],
            opportunity_type="Expansion",
        )

        call_kwargs = mock_boto3_client.update_opportunity.call_args.kwargs
        assert "LifeCycle" in call_kwargs
        assert "Project" in call_kwargs
        assert "Customer" in call_kwargs
        assert "Marketing" in call_kwargs
        assert "PrimaryNeedsFromAws" in call_kwargs
        assert "OpportunityType" in call_kwargs

    def test_update_returns_response(self, ace_client, mock_boto3_client):
        """Update should return boto3 response."""
        expected_response = {"Id": "opp-12345"}
        mock_boto3_client.update_opportunity.return_value = expected_response

        result = ace_client.update_opportunity(
            opportunity_id="opp-12345",
            last_modified_date="2026-03-23T10:00:00Z",
            life_cycle={"Stage": "Technical Validation"},
        )

        assert result == expected_response


class TestWriteDelay:
    """Test write rate limiting."""

    def test_write_delay_enforced_on_create(self, ace_client, mock_boto3_client):
        """Multiple creates should be delayed by WRITE_DELAY_SECONDS."""
        mock_boto3_client.create_opportunity.return_value = {"Id": "opp-123"}

        import time

        ace_client.create_opportunity(
            client_token="token-1",
            customer={"Account": {"CompanyName": "Acme Corp"}},
            project={"Title": "Project 1"},
            life_cycle={"Stage": "Qualified"},
        )
        first_call = time.monotonic()

        ace_client.create_opportunity(
            client_token="token-2",
            customer={"Account": {"CompanyName": "Beta Corp"}},
            project={"Title": "Project 2"},
            life_cycle={"Stage": "Qualified"},
        )
        second_call = time.monotonic()

        # Second call should be delayed (at least ~1 second)
        elapsed = second_call - first_call
        # Allow some tolerance for timing variations
        assert elapsed >= 0.9

    def test_write_delay_enforced_on_update(self, ace_client, mock_boto3_client):
        """Multiple updates should be delayed by WRITE_DELAY_SECONDS."""
        mock_boto3_client.update_opportunity.return_value = {"Id": "opp-123"}

        import time

        ace_client.update_opportunity(
            opportunity_id="opp-1",
            last_modified_date="2026-03-23T10:00:00Z",
            life_cycle={"Stage": "Technical Validation"},
        )
        first_call = time.monotonic()

        ace_client.update_opportunity(
            opportunity_id="opp-2",
            last_modified_date="2026-03-23T11:00:00Z",
            life_cycle={"Stage": "Technical Validation"},
        )
        second_call = time.monotonic()

        elapsed = second_call - first_call
        assert elapsed >= 0.9


class TestReadOperations:
    """Test read-only operations (not rate-limited)."""

    def test_get_opportunity(self, ace_client, mock_boto3_client):
        """get_opportunity should call boto3 without rate limit."""
        expected_opp = {
            "Id": "opp-12345",
            "LifeCycle": {"Stage": "Qualified"},
            "LastModifiedDate": "2026-03-23T10:00:00Z",
        }
        mock_boto3_client.get_opportunity.return_value = expected_opp

        result = ace_client.get_opportunity("opp-12345")

        assert mock_boto3_client.get_opportunity.called
        assert result == expected_opp

    def test_list_opportunities(self, ace_client, mock_boto3_client):
        """list_opportunities should call boto3."""
        expected_opps = [
            {"Id": "opp-1", "LifeCycle": {"Stage": "Qualified"}},
            {"Id": "opp-2", "LifeCycle": {"Stage": "Technical Validation"}},
        ]
        mock_boto3_client.list_opportunities.return_value = {"OpportunitySummaries": expected_opps}

        result = ace_client.list_opportunities()

        assert mock_boto3_client.list_opportunities.called
        assert result == expected_opps

    def test_list_solutions(self, ace_client, mock_boto3_client):
        """list_solutions should call boto3."""
        expected_sols = [{"Id": "sol-123", "Name": "Test Solution"}]
        mock_boto3_client.list_solutions.return_value = {"SolutionSummaries": expected_sols}

        result = ace_client.list_solutions()

        assert mock_boto3_client.list_solutions.called
        assert result == expected_sols


class TestConnectionTest:
    """Test connection validation."""

    def test_connection_test_success(self, ace_client, mock_boto3_client):
        """test_connection should return True on success."""
        mock_boto3_client.list_solutions.return_value = {"SolutionSummaries": [{"Id": "sol-123"}]}

        result = ace_client.test_connection()

        assert result is True

    def test_connection_test_failure(self, ace_client, mock_boto3_client):
        """test_connection should return False on error."""
        from botocore.exceptions import ClientError

        mock_boto3_client.list_solutions.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "ListSolutions",
        )

        result = ace_client.test_connection()

        assert result is False


class TestAssociateOpportunity:
    """Test opportunity association with solution."""

    def test_associate_with_provided_solution_id(self, ace_client, mock_boto3_client):
        """associate_opportunity should use provided solution_id."""
        mock_boto3_client.associate_opportunity.return_value = {"Id": "assoc-123"}

        ace_client.associate_opportunity("opp-12345", solution_id="sol-999")

        call_kwargs = mock_boto3_client.associate_opportunity.call_args.kwargs
        assert call_kwargs["OpportunityIdentifier"] == "opp-12345"
        assert call_kwargs["RelatedEntityIdentifier"] == "sol-999"

    def test_associate_with_config_solution_id(self, ace_client, mock_boto3_client):
        """associate_opportunity should default to config solution_id."""
        mock_boto3_client.associate_opportunity.return_value = {"Id": "assoc-123"}

        ace_client.associate_opportunity("opp-12345")

        call_kwargs = mock_boto3_client.associate_opportunity.call_args.kwargs
        # Should use the one from ACEConfig (sol-12345)
        assert call_kwargs["RelatedEntityIdentifier"] == "sol-12345"


class TestStartEngagement:
    """Test engagement submission."""

    def test_start_engagement(self, ace_client, mock_boto3_client):
        """start_engagement should submit opportunity for AWS review."""
        mock_boto3_client.start_engagement_from_opportunity_task.return_value = {"Id": "eng-123"}

        ace_client.start_engagement("opp-12345")

        assert mock_boto3_client.start_engagement_from_opportunity_task.called
        call_kwargs = mock_boto3_client.start_engagement_from_opportunity_task.call_args.kwargs
        assert call_kwargs["Identifier"] == "opp-12345"
        assert "AwsSubmission" in call_kwargs
        assert call_kwargs["AwsSubmission"]["InvolvementType"] == "Co-Sell"
