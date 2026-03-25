"""HubSpot deal/company → ACE payload transformation."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from .config import (
    ACE_STAGE_ORDER,
    DEALTYPE_TO_ACE_OPPORTUNITY_TYPE,
    DEFAULT_AWS_INDUSTRY,
    DEFAULT_CLOSED_LOST_REASON,
    DEFAULT_OPPORTUNITY_TYPE,
    HS_ACE_PROJECT_DESCRIPTION,
    HS_AMOUNT,
    HS_CITY,
    HS_CLOSEDATE,
    HS_CONTRACT_TERM,
    HS_COUNTRY_CODE,
    HS_DEALNAME,
    HS_DEALTYPE,
    HS_DESCRIPTION,
    HS_DOMAIN,
    HS_INDUSTRY,
    HS_LOSS_REASON,
    HS_STATE,
    HS_WEBSITE,
    HS_ZIP,
    INDUSTRY_TO_AWS,
    LOSS_REASON_TO_ACE,
    STAGE_TO_ACE,
    STAGE_TO_NEXT_STEPS,
    STAGE_TO_SALES_ACTIVITIES,
)
from .logger import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """Raised when a deal fails validation for ACE sync."""

    def __init__(self, deal_id: int, deal_name: str, errors: list[str]) -> None:
        self.deal_id = deal_id
        self.deal_name = deal_name
        self.errors = errors
        super().__init__(f"Deal {deal_id} ({deal_name}): {'; '.join(errors)}")


def validate_deal_for_create(deal_props: dict[str, Any], company_props: dict[str, Any] | None) -> list[str]:
    """Validate a deal has all required fields for ACE create. Returns list of error messages (empty = valid)."""
    errors: list[str] = []

    description = deal_props.get(HS_ACE_PROJECT_DESCRIPTION) or ""
    if len(description.strip()) < 20:
        errors.append(f"ace_project_description must be at least 20 characters (got {len(description.strip())})")

    if not deal_props.get(HS_CLOSEDATE):
        errors.append("closedate is required")

    if not deal_props.get(HS_AMOUNT):
        errors.append("amount is required")

    if not company_props:
        errors.append("No associated company found")
    else:
        if not company_props.get("name"):
            errors.append("Company name is required")
        if not company_props.get(HS_COUNTRY_CODE):
            errors.append("Company country code (hs_country_code) is required")

    return errors


PAYLOAD_VERSION = "v4"  # Bump when payload structure changes to avoid token conflicts


def generate_client_token(deal_id: int) -> str:
    """Generate a deterministic UUID from a HubSpot deal ID for idempotency."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"hubspot-deal-{deal_id}-{PAYLOAD_VERSION}"))


def map_opportunity_type(deal_props: dict[str, Any]) -> str:
    """Map HubSpot dealtype to ACE opportunity type."""
    dealtype = deal_props.get(HS_DEALTYPE) or ""
    return DEALTYPE_TO_ACE_OPPORTUNITY_TYPE.get(dealtype, DEFAULT_OPPORTUNITY_TYPE)


def map_ace_stage(hubspot_stage_id: str) -> str | None:
    """Map HubSpot stage ID to ACE stage name. Returns None if not mappable."""
    return STAGE_TO_ACE.get(hubspot_stage_id)


def is_stage_regression(current_ace_stage: str, new_ace_stage: str) -> bool:
    """Check if new stage would be a regression from current stage."""
    current_order = ACE_STAGE_ORDER.get(current_ace_stage, 0)
    new_order = ACE_STAGE_ORDER.get(new_ace_stage, 0)
    return new_order < current_order


def calculate_monthly_spend(deal_props: dict[str, Any]) -> str:
    """Calculate monthly expected customer spend from deal amount and contract term.

    Returns string with up to 2 decimal places (AWS format).
    """
    try:
        amount = float(deal_props.get(HS_AMOUNT) or 0)
    except (ValueError, TypeError):
        logger.warning(f"Invalid amount value: {deal_props.get(HS_AMOUNT)}, defaulting to 0")
        amount = 0.0
    try:
        contract_months = int(float(deal_props.get(HS_CONTRACT_TERM) or 12))
    except (ValueError, TypeError):
        contract_months = 12
    if contract_months <= 0:
        contract_months = 12

    monthly = amount / contract_months

    if monthly == int(monthly):
        return str(int(monthly))
    return f"{monthly:.2f}"


def map_industry(company_props: dict[str, Any]) -> str:
    """Map HubSpot industry to AWS industry picklist value."""
    industry = company_props.get(HS_INDUSTRY) or ""
    return INDUSTRY_TO_AWS.get(industry, DEFAULT_AWS_INDUSTRY)


def build_website_url(company_props: dict[str, Any]) -> str | None:
    """Get website URL with protocol. Falls back to domain with https://."""
    website = company_props.get(HS_WEBSITE)
    if website:
        return website if website.startswith(("http://", "https://")) else f"https://{website}"

    domain = company_props.get(HS_DOMAIN)
    if domain:
        return f"https://{domain}"

    return None


def _parse_close_date(deal_props: dict[str, Any]) -> str:
    """Parse closedate to YYYY-MM-DD format (AWS requirement)."""
    close_date_str = deal_props.get(HS_CLOSEDATE, "")
    if not close_date_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(close_date_str).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(close_date_str)[:10]


def _build_project_block(
    deal_props: dict[str, Any],
    company_props: dict[str, Any] | None,
    ace_stage: str,
) -> dict[str, Any]:
    """Build the Project block with stage-aware SalesActivities."""
    monthly_spend = calculate_monthly_spend(deal_props)
    target_company = company_props.get("name", "") if company_props else ""

    project: dict[str, Any] = {
        "CustomerBusinessProblem": deal_props.get(HS_ACE_PROJECT_DESCRIPTION, ""),
        "Title": deal_props.get(HS_DEALNAME, "Partner Opportunity"),
        "DeliveryModels": ["SaaS or PaaS"],
        "CustomerUseCase": "Other",
        "ExpectedCustomerSpend": [
            {
                "Amount": monthly_spend,
                "CurrencyCode": "USD",
                "Frequency": "Monthly",
                "TargetCompany": target_company,
            }
        ],
        "SalesActivities": STAGE_TO_SALES_ACTIVITIES.get(ace_stage, ["Customer has shown interest in solution"]),
    }

    description = deal_props.get(HS_DESCRIPTION) or ""
    if description.strip():
        project["AdditionalComments"] = description.strip()[:2000]

    return project


def _sanitize_phone(phone: str) -> str | None:
    """Sanitize phone to E.164 format (+[1-9]\\d{1,14}). Returns None if invalid."""
    digits = re.sub(r"[^\d+]", "", phone)
    if not digits.startswith("+"):
        digits = f"+{digits}"
    if re.match(r"^\+[1-9]\d{1,14}$", digits):
        return digits
    return None


def build_customer_contacts(contacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build ACE Customer.Contacts list from HubSpot contacts."""
    ace_contacts: list[dict[str, Any]] = []
    for contact in contacts:
        first = contact.get("firstname") or ""
        last = contact.get("lastname") or ""
        if not (first and last):
            continue
        ace_contact: dict[str, Any] = {
            "FirstName": first,
            "LastName": last,
        }
        title = contact.get("jobtitle")
        if title:
            ace_contact["BusinessTitle"] = title[:80]
        email = contact.get("email")
        if email:
            ace_contact["Email"] = email
        phone = _sanitize_phone(contact.get("phone") or "")
        if phone:
            ace_contact["Phone"] = phone
        ace_contacts.append(ace_contact)
    return ace_contacts


def build_opportunity_team(owner: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Build ACE OpportunityTeam from HubSpot deal owner."""
    if not owner:
        return []
    first = owner.get("firstName") or ""
    last = owner.get("lastName") or ""
    if not (first and last):
        return []
    member: dict[str, Any] = {
        "Email": owner.get("email", ""),
        "FirstName": first,
        "LastName": last,
        "BusinessTitle": "OpportunityOwner",
    }
    phone = _sanitize_phone(owner.get("phone") or "")
    if phone:
        member["Phone"] = phone
    return [member]


def _build_customer_block(company_props: dict[str, Any]) -> dict[str, Any]:
    """Build the Customer block (required on both create and update)."""
    address: dict[str, str] = {}
    country_code = company_props.get(HS_COUNTRY_CODE)
    if country_code:
        address["CountryCode"] = country_code
    city = company_props.get(HS_CITY)
    if city:
        address["City"] = city
    postal_code = company_props.get(HS_ZIP)
    if postal_code:
        address["PostalCode"] = postal_code
    state = company_props.get(HS_STATE)
    if state:
        address["StateOrRegion"] = state

    account: dict[str, Any] = {
        "CompanyName": company_props.get("name", ""),
        "Industry": map_industry(company_props),
    }
    if address:
        account["Address"] = address
    website = build_website_url(company_props)
    if website:
        account["WebsiteUrl"] = website

    return {"Account": account}


def build_create_payload(
    deal_id: int,
    deal_props: dict[str, Any],
    company_props: dict[str, Any],
    contacts: list[dict[str, Any]] | None = None,
    owner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the full CreateOpportunity payload from HubSpot deal + company data.

    Returns dict with keys: client_token, customer, project, life_cycle, etc.
    """
    customer = _build_customer_block(company_props)
    if contacts:
        ace_contacts = build_customer_contacts(contacts)
        if ace_contacts:
            customer["Contacts"] = ace_contacts

    ace_stage = "Qualified"  # New deals always start at Qualified
    project = _build_project_block(deal_props, company_props, ace_stage)

    target_close = _parse_close_date(deal_props)
    life_cycle: dict[str, Any] = {
        "Stage": ace_stage,
        "TargetCloseDate": target_close or None,
        "NextSteps": STAGE_TO_NEXT_STEPS.get(ace_stage, "Initial qualification"),
    }

    payload: dict[str, Any] = {
        "client_token": generate_client_token(deal_id),
        "customer": customer,
        "project": project,
        "life_cycle": life_cycle,
        "marketing": {"Source": "Marketing Activity", "AwsFundingUsed": "No"},
        "primary_needs_from_aws": ["Co-Sell - Architectural Validation"],
        "opportunity_type": map_opportunity_type(deal_props),
    }

    team = build_opportunity_team(owner)
    if team:
        payload["opportunity_team"] = team

    return payload


def build_update_payload(
    deal_props: dict[str, Any],
    current_ace_stage: str,
    new_ace_stage: str,
    company_props: dict[str, Any] | None = None,
    contacts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build UpdateOpportunity payload — AWS requires ALL required fields on update."""
    payload: dict[str, Any] = {}

    effective_stage = current_ace_stage
    if new_ace_stage != current_ace_stage:
        if is_stage_regression(current_ace_stage, new_ace_stage):
            logger.warning(
                f"Stage regression blocked: {current_ace_stage} → {new_ace_stage}. " "Updating non-stage fields only."
            )
        else:
            effective_stage = new_ace_stage

    target_close = _parse_close_date(deal_props)
    life_cycle: dict[str, Any] = {
        "Stage": effective_stage,
        "NextSteps": STAGE_TO_NEXT_STEPS.get(effective_stage, "In progress"),
    }
    if target_close:
        life_cycle["TargetCloseDate"] = target_close

    if new_ace_stage == "Closed Lost":
        hs_loss = deal_props.get(HS_LOSS_REASON, "") or ""
        life_cycle["ClosedLostReason"] = LOSS_REASON_TO_ACE.get(hs_loss, DEFAULT_CLOSED_LOST_REASON)

    payload["life_cycle"] = life_cycle

    if company_props:
        customer = _build_customer_block(company_props)
        if contacts:
            ace_contacts = build_customer_contacts(contacts)
            if ace_contacts:
                customer["Contacts"] = ace_contacts
        payload["customer"] = customer

    payload["project"] = _build_project_block(deal_props, company_props, effective_stage)
    payload["marketing"] = {"Source": "Marketing Activity", "AwsFundingUsed": "No"}
    payload["primary_needs_from_aws"] = ["Co-Sell - Architectural Validation"]
    payload["opportunity_type"] = map_opportunity_type(deal_props)

    return payload
