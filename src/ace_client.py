"""AWS Partner Central Selling API client — boto3 wrapper with rate limiting."""

from __future__ import annotations

import time
import uuid
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from .config import (
    ACE_REGION,
    ACE_SERVICE,
    ACE_USER_AGENT,
    WRITE_DELAY_SECONDS,
    ACEConfig,
)
from .logger import get_logger

logger = get_logger(__name__)


class ACEClient:
    """Wrapper around boto3 partnercentral-selling API with rate limiting."""

    def __init__(self, config: ACEConfig) -> None:
        self.config = config
        self.catalog = config.catalog

        boto_config = BotoConfig(
            region_name=ACE_REGION,
            user_agent_extra=ACE_USER_AGENT,
            retries={"max_attempts": 4, "mode": "adaptive"},
        )

        self._client = boto3.client(
            ACE_SERVICE,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            config=boto_config,
        )

        self._last_write_time: float = 0.0

    def _enforce_write_delay(self) -> None:
        """Enforce 1-second delay between write operations."""
        elapsed = time.monotonic() - self._last_write_time
        if elapsed < WRITE_DELAY_SECONDS:
            time.sleep(WRITE_DELAY_SECONDS - elapsed)
        self._last_write_time = time.monotonic()

    # =========================================================================
    # Read Operations
    # =========================================================================

    def get_opportunity(self, opportunity_id: str) -> dict[str, Any]:
        """Fetch an opportunity by ID. Returns full opportunity data including LastModifiedDate."""
        response = self._client.get_opportunity(
            Catalog=self.catalog,
            Identifier=opportunity_id,
        )
        logger.info(f"Fetched ACE opportunity {opportunity_id}")
        return response

    def get_aws_opportunity_summary(self, opportunity_id: str) -> dict[str, Any]:
        """Fetch the AWS-side view of an opportunity.

        Returns AWS-assigned team members (Account Manager, Sales Rep, PSM, PDM),
        engagement score, and AWS-side stage/status. Only populated after AWS accepts
        the opportunity (typically 1-2 days after submission).
        """
        response = self._client.get_aws_opportunity_summary(
            Catalog=self.catalog,
            RelatedOpportunityIdentifier=opportunity_id,
        )
        logger.info(f"Fetched AWS summary for opportunity {opportunity_id}")
        return response

    def list_opportunities(self, max_results: int = 50) -> list[dict[str, Any]]:
        """List opportunities in the catalog."""
        response = self._client.list_opportunities(
            Catalog=self.catalog,
            MaxResults=min(max_results, 100),
        )
        return response.get("OpportunitySummaries", [])

    def list_solutions(self) -> list[dict[str, Any]]:
        """List partner solutions."""
        response = self._client.list_solutions(
            Catalog=self.catalog,
            MaxResults=10,
        )
        return response.get("SolutionSummaries", [])

    # =========================================================================
    # Write Operations (rate-limited to 1/sec)
    # =========================================================================

    def create_opportunity(
        self,
        *,
        client_token: str,
        customer: dict[str, Any],
        project: dict[str, Any],
        life_cycle: dict[str, Any],
        marketing: dict[str, Any] | None = None,
        primary_needs_from_aws: list[str] | None = None,
        opportunity_team: list[dict[str, Any]] | None = None,
        opportunity_type: str = "Net New Business",
    ) -> dict[str, Any]:
        """Create a new ACE opportunity. Returns response with Id (the ACE opportunity ID)."""
        self._enforce_write_delay()

        params: dict[str, Any] = {
            "Catalog": self.catalog,
            "ClientToken": client_token,
            "Customer": customer,
            "Project": project,
            "LifeCycle": life_cycle,
            "OpportunityType": opportunity_type,
            "Origin": "Partner Referral",
        }
        if marketing:
            params["Marketing"] = marketing
        if primary_needs_from_aws:
            params["PrimaryNeedsFromAws"] = primary_needs_from_aws
        if opportunity_team:
            params["OpportunityTeam"] = opportunity_team

        response = self._client.create_opportunity(**params)
        opp_id = response.get("Id", "unknown")
        logger.info(f"Created ACE opportunity {opp_id}")
        return response

    def associate_opportunity(self, opportunity_id: str, solution_id: str | None = None) -> dict[str, Any]:
        """Associate an opportunity with your partner solution. Required after create."""
        self._enforce_write_delay()

        response = self._client.associate_opportunity(
            Catalog=self.catalog,
            OpportunityIdentifier=opportunity_id,
            RelatedEntityType="Solutions",
            RelatedEntityIdentifier=solution_id or self.config.solution_id,
        )
        logger.info(
            f"Associated opportunity {opportunity_id} with solution {solution_id or self.config.solution_id}"
        )
        return response

    def start_engagement(self, opportunity_id: str) -> dict[str, Any]:
        """Submit opportunity for AWS review. Required after create."""
        self._enforce_write_delay()

        response = self._client.start_engagement_from_opportunity_task(
            Catalog=self.catalog,
            Identifier=opportunity_id,
            ClientToken=str(uuid.uuid4()),
            AwsSubmission={"InvolvementType": "Co-Sell", "Visibility": "Full"},
        )
        logger.info(f"Started engagement for opportunity {opportunity_id}")
        return response

    def update_opportunity(
        self,
        *,
        opportunity_id: str,
        last_modified_date: str,
        life_cycle: dict[str, Any] | None = None,
        project: dict[str, Any] | None = None,
        customer: dict[str, Any] | None = None,
        marketing: dict[str, Any] | None = None,
        primary_needs_from_aws: list[str] | None = None,
        opportunity_type: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing ACE opportunity. AWS requires ALL required fields on update.

        Note: OpportunityTeam is NOT updatable — it can only be set on create.
        """
        self._enforce_write_delay()

        params: dict[str, Any] = {
            "Catalog": self.catalog,
            "Identifier": opportunity_id,
            "LastModifiedDate": last_modified_date,
        }
        if life_cycle:
            params["LifeCycle"] = life_cycle
        if project:
            params["Project"] = project
        if customer:
            params["Customer"] = customer
        if marketing:
            params["Marketing"] = marketing
        if primary_needs_from_aws:
            params["PrimaryNeedsFromAws"] = primary_needs_from_aws
        if opportunity_type:
            params["OpportunityType"] = opportunity_type

        response = self._client.update_opportunity(**params)
        logger.info(f"Updated ACE opportunity {opportunity_id}")
        return response

    # =========================================================================
    # Connection Test
    # =========================================================================

    def test_connection(self) -> bool:
        """Test AWS Partner Central connectivity by listing solutions."""
        try:
            solutions = self.list_solutions()
            logger.info(f"ACE connection OK — {len(solutions)} solution(s) found")
            return True
        except ClientError as e:
            logger.error(f"ACE connection failed: {e}")
            return False
