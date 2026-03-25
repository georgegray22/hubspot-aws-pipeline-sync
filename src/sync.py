"""Core sync orchestration — create and update flows."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from .ace_client import ACEClient
from .config import (
    ACE_COMPANY_PROPERTIES,
    ACE_DEAL_PROPERTIES,
    ACE_ROLE_TO_HS_FIELDS,
    ACE_STATUS_NOT_SYNCED,
    ACE_STATUS_PENDING_REVIEW,
    ACE_STATUS_SYNC_ERROR,
    ACE_STATUS_SYNCED,
    DEFAULT_CLOSED_LOST_REASON,
    HS_ACE_LAST_SYNC,
    HS_ACE_OPPORTUNITY_ID,
    HS_ACE_PROJECT_DESCRIPTION,
    HS_ACE_SYNC_ERROR,
    HS_ACE_SYNC_STATUS,
    HS_DEALNAME,
    HS_DEALSTAGE,
    HS_OWNER_ID,
    HS_SUBMIT_TO_AWS,
    PIPELINE_ID,
    SKIP_STAGES,
    STAGE_DISPLAY_NAME,
    STAGE_TO_ACE,
    STAGE_TO_NEXT_STEPS,
    SYNC_ELIGIBLE_STAGES,
    ACEConfig,
)
from .hubspot_client import HubSpotClient, HubSpotConfig
from .logger import get_logger, print_status
from .mapping import (
    ValidationError,
    build_create_payload,
    build_update_payload,
    map_ace_stage,
    validate_deal_for_create,
)
from .slack_client import SlackClient, SlackConfig

logger = get_logger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
SLACK_CHANNEL = os.environ.get("ACE_SLACK_CHANNEL", "")
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "")
HUBSPOT_REGION = os.environ.get("HUBSPOT_REGION", "na1")  # na1, eu1


class SyncResult:
    """Tracks results across all deals in a sync run."""

    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.updated: list[dict[str, Any]] = []
        self.skipped: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []
        self.withdrawn: list[dict[str, Any]] = []

    @property
    def total(self) -> int:
        return len(self.created) + len(self.updated) + len(self.skipped) + len(self.errors)

    def summary(self) -> str:
        parts = [
            f"Created: {len(self.created)}, Updated: {len(self.updated)}, "
            f"Skipped: {len(self.skipped)}, Errors: {len(self.errors)}",
        ]
        if self.withdrawn:
            parts.append(f"Withdrawn: {len(self.withdrawn)}")
        return ", ".join(parts)


def _deal_link(deal_id: int) -> str:
    """HubSpot deal link (auto-detects region from HUBSPOT_REGION env var)."""
    if not HUBSPOT_PORTAL_ID:
        return ""
    return f"https://app-{HUBSPOT_REGION}.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-3/{deal_id}"


def fetch_eligible_deals(hubspot: HubSpotClient) -> list[dict[str, Any]]:
    """Fetch deals eligible for ACE sync from HubSpot.

    Criteria: submit_to_aws=true, pipeline matches configured pipeline, stage in eligible list.
    """
    endpoint = "/crm/v3/objects/deals/search"
    all_deals: list[dict[str, Any]] = []
    after: str | None = None

    while True:
        filter_groups = [
            {
                "filters": [
                    {"propertyName": HS_SUBMIT_TO_AWS, "operator": "EQ", "value": "true"},
                    {"propertyName": "pipeline", "operator": "EQ", "value": PIPELINE_ID},
                ]
            }
        ]

        payload: dict[str, Any] = {
            "filterGroups": filter_groups,
            "properties": ACE_DEAL_PROPERTIES,
            "limit": 100,
        }
        if after:
            payload["after"] = after

        response = hubspot.post(endpoint, json_data=payload)
        results = response.get("results", [])

        for deal_data in results:
            props = deal_data.get("properties", {})
            stage = props.get(HS_DEALSTAGE, "")

            if stage in SKIP_STAGES:
                logger.debug(f"Skipping deal {deal_data.get('id')} — stage {stage} is pre-eligible")
                continue

            if stage in SYNC_ELIGIBLE_STAGES:
                all_deals.append(deal_data)

        paging = response.get("paging") or {}
        next_page = paging.get("next", {})
        after = next_page.get("after")
        if not after:
            break

    logger.info(f"Found {len(all_deals)} eligible deals for ACE sync")
    return all_deals


def fetch_withdrawn_deals(hubspot: HubSpotClient) -> list[dict[str, Any]]:
    """Fetch deals where submit_to_aws=false but ace_opportunity_id is set.

    These are deals that were opted out after already being synced to ACE.
    """
    endpoint = "/crm/v3/objects/deals/search"
    all_deals: list[dict[str, Any]] = []
    after: str | None = None

    while True:
        filter_groups = [
            {
                "filters": [
                    {"propertyName": HS_SUBMIT_TO_AWS, "operator": "EQ", "value": "false"},
                    {"propertyName": HS_ACE_OPPORTUNITY_ID, "operator": "HAS_PROPERTY"},
                ]
            }
        ]

        payload: dict[str, Any] = {
            "filterGroups": filter_groups,
            "properties": ["dealname", HS_ACE_OPPORTUNITY_ID, HS_ACE_SYNC_STATUS],
            "limit": 100,
        }
        if after:
            payload["after"] = after

        response = hubspot.post(endpoint, json_data=payload)
        results = response.get("results", [])
        all_deals.extend(results)

        paging = response.get("paging") or {}
        next_page = paging.get("next", {})
        after = next_page.get("after")
        if not after:
            break

    logger.info(f"Found {len(all_deals)} withdrawn deal(s) to close in ACE")
    return all_deals


def withdraw_opportunity(
    deal_data: dict[str, Any],
    hubspot: HubSpotClient,
    ace: ACEClient,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Close an ACE opportunity for a deal that was opted out (submit_to_aws=false)."""
    deal_id = int(deal_data["id"])
    props = deal_data.get("properties", {})
    deal_name = props.get(HS_DEALNAME, f"Deal {deal_id}")
    opp_id = props.get(HS_ACE_OPPORTUNITY_ID, "")

    outcome = {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "action": "withdraw",
        "ace_opportunity_id": opp_id,
        "link": _deal_link(deal_id),
    }

    if dry_run:
        logger.info(f"Deal {deal_id} ({deal_name}): would withdraw ACE opportunity {opp_id}")
        return outcome

    ace_opp = ace.get_opportunity(opp_id)
    current_stage = ace_opp.get("LifeCycle", {}).get("Stage", "")
    last_modified = ace_opp.get("LastModifiedDate", "")

    if current_stage == "Closed Lost":
        logger.info(f"Deal {deal_id}: ACE opportunity {opp_id} already Closed Lost")
    else:
        ace.update_opportunity(
            opportunity_id=opp_id,
            last_modified_date=last_modified,
            life_cycle={
                "Stage": "Closed Lost",
                "ClosedLostReason": DEFAULT_CLOSED_LOST_REASON,
                "NextSteps": STAGE_TO_NEXT_STEPS["Closed Lost"],
            },
        )
        logger.info(f"Deal {deal_id}: closed ACE opportunity {opp_id} (withdrawn)")

    hubspot.update_deal(
        deal_id=deal_id,
        properties={
            HS_ACE_SYNC_STATUS: ACE_STATUS_NOT_SYNCED,
            HS_ACE_SYNC_ERROR: "",
            HS_ACE_OPPORTUNITY_ID: "",
            HS_ACE_LAST_SYNC: "",
        },
    )
    return outcome


def fetch_company_for_deal(hubspot: HubSpotClient, deal_id: int) -> dict[str, Any] | None:
    """Fetch the primary associated company for a deal."""
    associations = hubspot.get_deal_company_associations(deal_ids=[deal_id])
    company_ids = associations.get(deal_id, [])

    if not company_ids:
        return None

    company = hubspot.get_company(company_id=company_ids[0], properties=ACE_COMPANY_PROPERTIES)
    return {
        "id": company.id,
        "name": company.name,
        "domain": company.domain,
        **{k: v for k, v in (company.custom_properties or {}).items()},
    }


def fetch_contacts_for_deal(hubspot: HubSpotClient, deal_id: int) -> list[dict[str, Any]]:
    """Fetch associated contacts for a deal (name, title, email, phone)."""
    try:
        response = hubspot.get(f"/crm/v4/objects/deals/{deal_id}/associations/contacts")
        contacts = []
        for result in response.get("results", []):
            contact_id = result.get("toObjectId")
            if not contact_id:
                continue
            contact = hubspot.get(
                f"/crm/v3/objects/contacts/{contact_id}"
                "?properties=firstname,lastname,email,jobtitle,phone"
            )
            props = contact.get("properties", {})
            contacts.append(props)
        return contacts
    except Exception as e:
        logger.warning(f"Deal {deal_id}: failed to fetch contacts: {e}")
        return []


def fetch_deal_owner(hubspot: HubSpotClient, owner_id: str) -> dict[str, Any] | None:
    """Fetch HubSpot deal owner details."""
    if not owner_id:
        return None
    try:
        return hubspot.get(f"/crm/v3/owners/{owner_id}")
    except Exception as e:
        logger.warning(f"Failed to fetch owner {owner_id}: {e}")
        return None


def _update_deal_status(
    hubspot: HubSpotClient,
    deal_id: int,
    status: str,
    error_msg: str | None = None,
    opportunity_id: str | None = None,
    dry_run: bool = False,
) -> None:
    """Update ACE sync status fields on a HubSpot deal."""
    props: dict[str, Any] = {HS_ACE_SYNC_STATUS: status}

    if status in (ACE_STATUS_SYNCED, ACE_STATUS_PENDING_REVIEW):
        props[HS_ACE_LAST_SYNC] = datetime.now(timezone.utc).isoformat()
        props[HS_ACE_SYNC_ERROR] = ""

    if error_msg:
        props[HS_ACE_SYNC_ERROR] = error_msg[:2000]

    if opportunity_id:
        props[HS_ACE_OPPORTUNITY_ID] = opportunity_id

    if dry_run:
        logger.info(f"[DRY RUN] Would update deal {deal_id}: {props}")
        return

    hubspot.update_deal(deal_id, props)


def sync_deal_create(
    deal_data: dict[str, Any],
    hubspot: HubSpotClient,
    ace: ACEClient,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute create flow for a deal without an ACE opportunity ID."""
    deal_id = int(deal_data["id"])
    props = deal_data.get("properties", {})
    deal_name = props.get(HS_DEALNAME, f"Deal {deal_id}")

    company_props = fetch_company_for_deal(hubspot, deal_id)
    contacts = fetch_contacts_for_deal(hubspot, deal_id)
    owner = fetch_deal_owner(hubspot, props.get(HS_OWNER_ID, ""))

    errors = validate_deal_for_create(props, company_props)
    if errors:
        error_msg = "; ".join(errors)
        _update_deal_status(hubspot, deal_id, ACE_STATUS_SYNC_ERROR, error_msg, dry_run=dry_run)
        raise ValidationError(deal_id, deal_name, errors)

    payload = build_create_payload(deal_id, props, company_props, contacts=contacts, owner=owner)

    company_name = (company_props or {}).get("name", "Unknown")
    amount = props.get("amount", "")
    hubspot_stage_id = props.get(HS_DEALSTAGE, "")
    ace_stage = map_ace_stage(hubspot_stage_id) or "Qualified"
    hs_stage_name = STAGE_DISPLAY_NAME.get(hubspot_stage_id, hubspot_stage_id)

    if dry_run:
        logger.info(f"[DRY RUN] Would create ACE opportunity for deal {deal_id} ({deal_name})")
        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "action": "create",
            "dry_run": True,
            "company": company_name,
            "amount": amount,
            "stage": ace_stage,
            "hubspot_stage": hs_stage_name,
            "link": _deal_link(deal_id),
        }

    # Create → Associate → StartEngagement (3 writes, 1s apart)
    create_response = ace.create_opportunity(**payload)
    opp_id = create_response.get("Id", "")

    _update_deal_status(hubspot, deal_id, ACE_STATUS_PENDING_REVIEW, opportunity_id=opp_id)

    try:
        ace.associate_opportunity(opp_id)
    except ClientError as e:
        logger.warning(f"Deal {deal_id}: associate failed (will retry on next sync): {e}")

    try:
        ace.start_engagement(opp_id)
    except ClientError as e:
        logger.warning(f"Deal {deal_id}: start engagement failed (will retry on next sync): {e}")

    return {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "action": "create",
        "ace_opportunity_id": opp_id,
        "company": company_name,
        "amount": amount,
        "stage": ace_stage,
        "hubspot_stage": hs_stage_name,
        "link": _deal_link(deal_id),
    }


def _retry_post_create_steps(
    ace: ACEClient,
    opp_id: str,
    deal_id: int,
    ace_opp: dict[str, Any],
) -> None:
    """Retry associate/engage for deals where create succeeded but follow-up steps failed."""
    related = ace_opp.get("RelatedEntityIdentifiers", {})
    solutions = related.get("Solutions", [])

    if not solutions:
        try:
            ace.associate_opportunity(opp_id)
            logger.info(f"Deal {deal_id}: retried associate — success")
        except ClientError as e:
            logger.warning(f"Deal {deal_id}: associate retry failed: {e}")

    opp_stage = ace_opp.get("LifeCycle", {}).get("ReviewStatus", "")
    if not opp_stage:
        try:
            ace.start_engagement(opp_id)
            logger.info(f"Deal {deal_id}: retried start engagement — success")
        except ClientError as e:
            logger.warning(f"Deal {deal_id}: start engagement retry failed: {e}")


def sync_deal_update(
    deal_data: dict[str, Any],
    hubspot: HubSpotClient,
    ace: ACEClient,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute update flow for a deal that already has an ACE opportunity ID."""
    deal_id = int(deal_data["id"])
    props = deal_data.get("properties", {})
    deal_name = props.get(HS_DEALNAME, f"Deal {deal_id}")
    opp_id = props[HS_ACE_OPPORTUNITY_ID]

    ace_opp = ace.get_opportunity(opp_id)
    last_modified = ace_opp.get("LastModifiedDate", "")
    current_ace_stage = ace_opp.get("LifeCycle", {}).get("Stage", "")

    if not dry_run:
        _retry_post_create_steps(ace, opp_id, deal_id, ace_opp)

    review_status = ace_opp.get("LifeCycle", {}).get("ReviewStatus", "")
    non_updatable = {"Pending Submission", "Submitted", "In review", "Rejected"}
    if review_status in non_updatable:
        logger.info(f"Deal {deal_id}: opportunity {opp_id} is {review_status}, skipping update")
        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "action": "skip",
            "reason": review_status.lower(),
            "ace_opportunity_id": opp_id,
            "ace_stage": current_ace_stage,
            "link": _deal_link(deal_id),
        }

    hubspot_stage = props.get(HS_DEALSTAGE, "")
    hs_stage_name = STAGE_DISPLAY_NAME.get(hubspot_stage, hubspot_stage)
    new_ace_stage = map_ace_stage(hubspot_stage)

    if not new_ace_stage:
        logger.warning(f"Deal {deal_id}: stage {hubspot_stage} has no ACE mapping, updating non-stage fields only")
        new_ace_stage = current_ace_stage

    company_props = fetch_company_for_deal(hubspot, deal_id)
    contacts = fetch_contacts_for_deal(hubspot, deal_id)
    update_payload = build_update_payload(props, current_ace_stage, new_ace_stage, company_props, contacts=contacts)

    ace_stage_current = ace_opp.get("LifeCycle", {}).get("Stage", "")

    if ace_stage_current == "Launched" and new_ace_stage == "Launched":
        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "action": "skip",
            "reason": "post-launch (locked)",
            "ace_opportunity_id": opp_id,
            "ace_stage": current_ace_stage,
            "link": _deal_link(deal_id),
        }

    if ace_stage_current == "Closed Lost":
        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "action": "skip",
            "reason": "already closed in ACE",
            "ace_opportunity_id": opp_id,
            "ace_stage": current_ace_stage,
            "link": _deal_link(deal_id),
        }

    if review_status == "Approved":
        existing_account = ace_opp.get("Customer", {}).get("Account", {})
        existing_project = ace_opp.get("Project", {})

        if "customer" in update_payload:
            account = update_payload["customer"].get("Account", {})
            for locked_field in ("CompanyName", "WebsiteUrl", "Industry", "Address"):
                if locked_field in existing_account:
                    account[locked_field] = existing_account[locked_field]
                elif locked_field in account:
                    account.pop(locked_field)
            update_payload["customer"]["Account"] = account

        if "project" in update_payload:
            for locked_field in ("Title", "CustomerBusinessProblem"):
                if locked_field in existing_project:
                    update_payload["project"][locked_field] = existing_project[locked_field]
                elif locked_field in update_payload["project"]:
                    update_payload["project"].pop(locked_field)

    stage_changed = new_ace_stage != current_ace_stage

    if not update_payload:
        return {
            "deal_id": deal_id,
            "deal_name": deal_name,
            "action": "skip",
            "reason": "no changes",
            "ace_opportunity_id": opp_id,
            "ace_stage": current_ace_stage,
            "link": _deal_link(deal_id),
        }

    # Detect field-level changes for notifications
    field_changes: list[str] = []

    ace_spend_list = ace_opp.get("Project", {}).get("ExpectedCustomerSpend", [])
    ace_monthly = ace_spend_list[0].get("Amount", "") if ace_spend_list else ""
    new_spend_list = update_payload.get("project", {}).get("ExpectedCustomerSpend", [])
    new_monthly = new_spend_list[0].get("Amount", "") if new_spend_list else ""
    if new_monthly and ace_monthly:
        try:
            old_val, new_val = float(ace_monthly), float(new_monthly)
            if abs(old_val - new_val) > 0.01:
                field_changes.append(f"MRR ${old_val:,.0f} \u2192 ${new_val:,.0f}")
        except (ValueError, TypeError):
            pass

    ace_close = ace_opp.get("LifeCycle", {}).get("TargetCloseDate", "")
    new_close = update_payload.get("life_cycle", {}).get("TargetCloseDate", "")
    if ace_close and new_close and str(ace_close)[:10] != str(new_close)[:10]:
        field_changes.append(f"close {str(ace_close)[:10]} \u2192 {str(new_close)[:10]}")

    ace_type = ace_opp.get("OpportunityType", "")
    new_type = update_payload.get("opportunity_type", "")
    if ace_type and new_type and ace_type != new_type:
        field_changes.append(f"type {ace_type} \u2192 {new_type}")

    outcome = {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "action": "update",
        "ace_opportunity_id": opp_id,
        "from_stage": current_ace_stage,
        "to_stage": new_ace_stage,
        "stage_changed": stage_changed,
        "hubspot_stage": hs_stage_name,
        "link": _deal_link(deal_id),
        "field_changes": field_changes,
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would update ACE opportunity {opp_id} for deal {deal_id} ({deal_name})")
        outcome["dry_run"] = True
        return outcome

    ace.update_opportunity(
        opportunity_id=opp_id,
        last_modified_date=last_modified,
        **update_payload,
    )

    _update_deal_status(hubspot, deal_id, ACE_STATUS_SYNCED)
    return outcome


def run_sync(config: ACEConfig) -> SyncResult:
    """Main sync entry point. Fetches eligible deals and runs create/update flows."""
    result = SyncResult()

    hubspot = HubSpotClient(HubSpotConfig())
    ace = ACEClient(config)

    print_status(f"Starting ACE sync (catalog={config.catalog}, dry_run={config.dry_run})", "processing")

    # Withdraw opted-out deals
    withdrawn_deals = fetch_withdrawn_deals(hubspot)
    for wd in withdrawn_deals:
        wd_id = int(wd["id"])
        wd_name = wd.get("properties", {}).get(HS_DEALNAME, f"Deal {wd_id}")
        try:
            outcome = withdraw_opportunity(wd, hubspot, ace, dry_run=config.dry_run)
            result.withdrawn.append(outcome)
        except ClientError as e:
            logger.error(f"Failed to withdraw deal {wd_id} ({wd_name}): {e}")
            result.errors.append({"deal_id": wd_id, "deal_name": wd_name, "error": str(e), "link": _deal_link(wd_id)})
        except Exception as e:
            logger.error(f"Failed to withdraw deal {wd_id} ({wd_name}): {e}")
            result.errors.append({"deal_id": wd_id, "deal_name": wd_name, "error": str(e), "link": _deal_link(wd_id)})

    if result.withdrawn:
        print_status(f"Withdrawn {len(result.withdrawn)} deal(s) from ACE", "success")

    # Fetch eligible deals
    deals = fetch_eligible_deals(hubspot)

    if not deals:
        print_status("No eligible deals found", "info")
        return result

    print_status(f"Processing {len(deals)} eligible deal(s)", "processing")

    for deal_data in deals:
        deal_id = int(deal_data["id"])
        props = deal_data.get("properties", {})
        deal_name = props.get(HS_DEALNAME, f"Deal {deal_id}")
        has_opp_id = bool(props.get(HS_ACE_OPPORTUNITY_ID))

        try:
            if has_opp_id:
                outcome = sync_deal_update(deal_data, hubspot, ace, dry_run=config.dry_run)
            else:
                outcome = sync_deal_create(deal_data, hubspot, ace, dry_run=config.dry_run)

            action = outcome.get("action", "")
            if action == "create":
                result.created.append(outcome)
            elif action == "update":
                result.updated.append(outcome)
            else:
                result.skipped.append(outcome)

        except ValidationError as e:
            result.errors.append(
                {"deal_id": deal_id, "deal_name": deal_name, "error": str(e), "link": _deal_link(deal_id)}
            )
            logger.warning(f"Validation failed for deal {deal_id}: {e}")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = str(e)

            if error_code == "ConflictException":
                _update_deal_status(hubspot, deal_id, ACE_STATUS_PENDING_REVIEW, dry_run=config.dry_run)
                result.skipped.append(
                    {
                        "deal_id": deal_id,
                        "deal_name": deal_name,
                        "action": "skip",
                        "reason": "AWS review in progress (locked)",
                        "link": _deal_link(deal_id),
                    }
                )
            else:
                _update_deal_status(hubspot, deal_id, ACE_STATUS_SYNC_ERROR, error_msg, dry_run=config.dry_run)
                result.errors.append(
                    {"deal_id": deal_id, "deal_name": deal_name, "error": error_msg, "link": _deal_link(deal_id)}
                )

        except Exception as e:
            # Catch-all for safety: captures unexpected errors, validation errors, etc.
            # Specific exception types should be added above if they need custom handling.
            _update_deal_status(hubspot, deal_id, ACE_STATUS_SYNC_ERROR, str(e), dry_run=config.dry_run)
            result.errors.append(
                {"deal_id": deal_id, "deal_name": deal_name, "error": str(e), "link": _deal_link(deal_id)}
            )

    # Reverse sync: pull AWS team contacts back into HubSpot
    if not config.dry_run:
        _reverse_sync_aws_contacts(deals, hubspot, ace)

    _write_sync_log(result, config)
    _send_slack_summary(result, config)

    print_status(result.summary(), "success" if not result.errors else "warning")
    return result


def _reverse_sync_aws_contacts(
    deals: list[dict[str, Any]],
    hubspot: HubSpotClient,
    ace: ACEClient,
) -> None:
    """Pull AWS-assigned team members from ACE back into HubSpot deal properties."""
    synced_deals = [d for d in deals if d.get("properties", {}).get(HS_ACE_OPPORTUNITY_ID)]

    if not synced_deals:
        return

    print_status(f"Reverse sync: checking AWS contacts for {len(synced_deals)} deal(s)", "processing")
    updated_count = 0

    for deal_data in synced_deals:
        deal_id = int(deal_data["id"])
        props = deal_data.get("properties", {})
        opp_id = props[HS_ACE_OPPORTUNITY_ID]
        deal_name = props.get(HS_DEALNAME, f"Deal {deal_id}")

        try:
            aws_summary = ace.get_aws_opportunity_summary(opp_id)
        except Exception as e:
            logger.debug(f"Deal {deal_id}: no AWS summary available yet ({e})")
            continue

        aws_team = aws_summary.get("OpportunityTeam", [])
        if not aws_team:
            continue

        hs_updates: dict[str, str] = {}
        for member in aws_team:
            role = member.get("BusinessTitle", "")
            if role not in ACE_ROLE_TO_HS_FIELDS:
                continue

            first = member.get("FirstName", "")
            last = member.get("LastName", "")
            email = member.get("Email", "")
            display = f"{first} {last}".strip()
            if email:
                display += f" ({email})"

            name_field, email_field = ACE_ROLE_TO_HS_FIELDS[role]
            hs_updates[name_field] = display
            if email_field and email:
                hs_updates[email_field] = email

        if not hs_updates:
            continue

        changed: dict[str, str] = {}
        for field_name, new_value in hs_updates.items():
            current = props.get(field_name, "") or ""
            if current != new_value:
                changed[field_name] = new_value

        if not changed:
            continue

        try:
            hubspot.update_deal(deal_id=deal_id, properties=changed)
            updated_count += 1
            logger.info(f"Deal {deal_id} ({deal_name}): updated AWS contacts — {list(changed.keys())}")
        except Exception as e:
            logger.warning(f"Deal {deal_id}: failed to write AWS contacts back to HubSpot: {e}")

    if updated_count:
        print_status(f"Reverse sync: updated AWS contacts on {updated_count} deal(s)", "success")
    else:
        logger.info("Reverse sync: no AWS contact changes detected")


def _write_sync_log(result: SyncResult, config: ACEConfig) -> None:
    """Write sync results to JSON log file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log_path = DATA_DIR / "sync_log.json"

    log_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "catalog": config.catalog,
        "dry_run": config.dry_run,
        "summary": result.summary(),
        "created": result.created,
        "updated": result.updated,
        "skipped": result.skipped,
        "errors": result.errors,
        "withdrawn": result.withdrawn,
    }

    log_path.write_text(json.dumps(log_data, indent=2, default=str))
    logger.info(f"Sync log written to {log_path}")


def _fmt_amount(amount: str) -> str:
    """Format a deal amount as a currency string."""
    try:
        return f"${float(amount):,.0f}"
    except (ValueError, TypeError):
        return ""


def _send_slack_summary(result: SyncResult, config: ACEConfig) -> None:
    """Post a sync summary to Slack with per-deal detail in a thread."""
    if not SLACK_CHANNEL:
        logger.info("ACE_SLACK_CHANNEL not set, skipping Slack notification")
        return

    try:
        slack = SlackClient(SlackConfig())
    except Exception as e:
        logger.warning(f"Slack not configured, skipping notification: {e}")
        return

    has_activity = result.created or result.updated or result.errors or result.withdrawn
    if not has_activity:
        logger.info(f"No activity to report — {result.total} deal(s) checked, all up to date")
        return

    dry_label = "  :construction:  DRY RUN" if config.dry_run else ""
    blocks: list[dict] = []

    status_icon = ":large_green_circle:" if not result.errors else ":red_circle:"
    counts = []
    if result.created:
        counts.append(f"*{len(result.created)}* new")
    if result.updated:
        counts.append(f"*{len(result.updated)}* updated")
    if result.skipped:
        counts.append(f"{len(result.skipped)} skipped")
    if result.withdrawn:
        counts.append(f"*{len(result.withdrawn)}* withdrawn")
    if result.errors:
        counts.append(f"*{len(result.errors)}* failed")

    separator = "  \u2022  "
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"{status_icon}  *ACE Pipeline Sync*{dry_label}\n{separator.join(counts)}"},
    })

    if result.created:
        lines = []
        for d in result.created:
            name = f"<{d.get('link', '')}|{d['deal_name']}>" if d.get("link") else d["deal_name"]
            line = f":rocket:  {name} submitted to AWS"
            company = d.get("company", "")
            amount = _fmt_amount(d.get("amount", ""))
            if company:
                line += f" ({company}{', ' + amount if amount else ''})"
            lines.append(line)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    if result.errors:
        lines = []
        for e in result.errors:
            name = f"<{e.get('link', '')}|{e['deal_name']}>" if e.get("link") else e["deal_name"]
            error_text = e.get("error", "Unknown")[:100]
            lines.append(f":warning:  {name} \u2014 {error_text}")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    timestamp = datetime.now(timezone.utc).strftime("%b %-d, %H:%M UTC")
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"{config.catalog} catalog  \u00b7  {result.total} deals  \u00b7  {timestamp}"}],
    })

    fallback = f"ACE Sync: {result.summary()}"
    try:
        slack.send_message(channel=SLACK_CHANNEL, blocks=blocks, text=fallback)
        logger.info("Slack summary sent")
    except Exception as e:
        logger.warning(f"Failed to send Slack summary: {e}")


def validate_deals(config: ACEConfig) -> None:
    """Pre-flight validation — check which deals are ready for sync without writing anything."""
    hubspot = HubSpotClient(HubSpotConfig())

    print_status("Validating deals for ACE sync readiness", "processing")
    deals = fetch_eligible_deals(hubspot)

    if not deals:
        print_status("No eligible deals found", "info")
        return

    valid_count = 0
    error_count = 0

    for deal_data in deals:
        deal_id = int(deal_data["id"])
        props = deal_data.get("properties", {})
        deal_name = props.get(HS_DEALNAME, f"Deal {deal_id}")
        has_opp_id = bool(props.get(HS_ACE_OPPORTUNITY_ID))

        if has_opp_id:
            print_status(f"  {deal_name} — already synced (ID: {props[HS_ACE_OPPORTUNITY_ID]})", "info")
            valid_count += 1
            continue

        company_props = fetch_company_for_deal(hubspot, deal_id)
        errors = validate_deal_for_create(props, company_props)

        if errors:
            print_status(f"  {deal_name} — INVALID: {'; '.join(errors)}", "error")
            error_count += 1
        else:
            print_status(f"  {deal_name} — ready for sync", "success")
            valid_count += 1

    print_status(f"Valid: {valid_count}, Invalid: {error_count}", "success" if error_count == 0 else "warning")
