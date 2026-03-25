"""Configuration for AWS HubSpot Sync — stage mappings, industry mappings, and constants.

All company-specific values (stage IDs, pipeline IDs, user agent) are loaded from
environment variables or the YAML config file. See .env.example and README.md for setup.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# =============================================================================
# AWS Partner Central Constants
# =============================================================================

ACE_REGION = os.environ.get("ACE_REGION", "us-east-1")
ACE_SERVICE = "partnercentral-selling"

# User agent format: CompanyName|ConnectorName|CRM|Version
# Example: "Acme|AcmeACEConnector|HubSpot|1.0"
ACE_USER_AGENT = os.environ.get("ACE_USER_AGENT", "MyCompany|ACEConnector|HubSpot|1.0")

# Write rate limit — ACE API enforces 1 req/sec for write operations
WRITE_DELAY_SECONDS = 1.0

# =============================================================================
# HubSpot Custom Deal Properties for ACE Sync
#
# You MUST create these custom properties in HubSpot before running the sync.
# See README.md "HubSpot Setup" section for instructions.
# =============================================================================

# Boolean checkbox — reps toggle this to opt a deal into ACE sync
HS_SUBMIT_TO_AWS = os.environ.get("HS_SUBMIT_FIELD", "submit_to_aws")

# Text — stores the ACE opportunity ID after successful create
HS_ACE_OPPORTUNITY_ID = os.environ.get("HS_ACE_OPP_ID_FIELD", "ace_opportunity_id")

# Dropdown — tracks sync status (not_synced, pending_review, Synced, Sync Error, Rejected)
HS_ACE_SYNC_STATUS = os.environ.get("HS_ACE_STATUS_FIELD", "ace_sync_status")

# DateTime — last successful sync timestamp
HS_ACE_LAST_SYNC = os.environ.get("HS_ACE_LAST_SYNC_FIELD", "ace_last_sync")

# Text — error message from last failed sync attempt
HS_ACE_SYNC_ERROR = os.environ.get("HS_ACE_ERROR_FIELD", "ace_sync_error")

# Textarea — project description sent to AWS (min 20 chars)
HS_ACE_PROJECT_DESCRIPTION = os.environ.get("HS_ACE_DESCRIPTION_FIELD", "ace_project_description")

# AWS contact fields — written back from ACE → HubSpot (reverse sync)
HS_ACE_AWS_ACCOUNT_MANAGER = os.environ.get("HS_ACE_AM_FIELD", "ace_aws_account_manager")
HS_ACE_AWS_ACCOUNT_MANAGER_EMAIL = os.environ.get("HS_ACE_AM_EMAIL_FIELD", "ace_aws_account_manager_email")
HS_ACE_AWS_SALES_REP = os.environ.get("HS_ACE_SR_FIELD", "ace_aws_sales_rep")
HS_ACE_AWS_SALES_REP_EMAIL = os.environ.get("HS_ACE_SR_EMAIL_FIELD", "ace_aws_sales_rep_email")
HS_ACE_AWS_PSM = os.environ.get("HS_ACE_PSM_FIELD", "ace_aws_partner_sales_manager")
HS_ACE_AWS_PDM = os.environ.get("HS_ACE_PDM_FIELD", "ace_aws_partner_development_manager")

# ACE OpportunityTeam BusinessTitle → HubSpot field mapping
ACE_ROLE_TO_HS_FIELDS: dict[str, tuple[str, str | None]] = {
    "AWSAccountOwner": (HS_ACE_AWS_ACCOUNT_MANAGER, HS_ACE_AWS_ACCOUNT_MANAGER_EMAIL),
    "AWSSalesRep": (HS_ACE_AWS_SALES_REP, HS_ACE_AWS_SALES_REP_EMAIL),
    "PSM": (HS_ACE_AWS_PSM, None),
    "PDM": (HS_ACE_AWS_PDM, None),
}

# =============================================================================
# Standard HubSpot Deal & Company Fields
# =============================================================================

HS_AMOUNT = "amount"
HS_CLOSEDATE = "closedate"
HS_DEALTYPE = "dealtype"
HS_DEALSTAGE = "dealstage"
HS_PIPELINE = "pipeline"
HS_CONTRACT_TERM = os.environ.get("HS_CONTRACT_TERM_FIELD", "contract_term__months_")
HS_DESCRIPTION = "description"
HS_DEALNAME = "dealname"

HS_COMPANY_NAME = "name"
HS_WEBSITE = "website"
HS_DOMAIN = "domain"
HS_COUNTRY_CODE = "hs_country_code"
HS_COUNTRY = "country"
HS_CITY = "city"
HS_ZIP = "zip"
HS_STATE = "state"
HS_INDUSTRY = "industry"
HS_OWNER_ID = "hubspot_owner_id"

# Properties to request from HubSpot deal search
ACE_DEAL_PROPERTIES = [
    "dealname",
    "dealstage",
    "amount",
    "closedate",
    "pipeline",
    "dealtype",
    "description",
    HS_CONTRACT_TERM,
    "hubspot_owner_id",
    HS_SUBMIT_TO_AWS,
    HS_ACE_OPPORTUNITY_ID,
    HS_ACE_SYNC_STATUS,
    HS_ACE_SYNC_ERROR,
    HS_ACE_PROJECT_DESCRIPTION,
    "loss_reason",
    "closed_lost_reason",
    # AWS contact fields (reverse sync) — fetched to detect changes
    HS_ACE_AWS_ACCOUNT_MANAGER,
    HS_ACE_AWS_ACCOUNT_MANAGER_EMAIL,
    HS_ACE_AWS_SALES_REP,
    HS_ACE_AWS_SALES_REP_EMAIL,
    HS_ACE_AWS_PSM,
    HS_ACE_AWS_PDM,
]

# Properties to request from HubSpot company
ACE_COMPANY_PROPERTIES = [
    "name",
    "website",
    "domain",
    "hs_country_code",
    "country",
    "city",
    "zip",
    "state",
    "industry",
    "address",
]

# =============================================================================
# Pipeline & Stage Configuration
#
# IMPORTANT: You MUST configure these for YOUR HubSpot instance.
# HubSpot stage IDs are unique to each account. Go to Settings > Deals > Pipelines
# to find your stage IDs (they appear in the URL when editing a stage).
#
# Set PIPELINE_ID and STAGE_MAPPING in your .env file. See .env.example.
# =============================================================================

# The HubSpot pipeline ID to sync (only deals in this pipeline are processed)
PIPELINE_ID = os.environ.get("HUBSPOT_PIPELINE_ID", "default")

# ACE Sync Status enum values (stored in HubSpot dropdown)
ACE_STATUS_NOT_SYNCED = "not_synced"
ACE_STATUS_PENDING_REVIEW = "pending_review"
ACE_STATUS_SYNCED = "Synced"
ACE_STATUS_SYNC_ERROR = "Sync Error"
ACE_STATUS_REJECTED = "Rejected"


def _load_stage_mapping() -> dict[str, str]:
    """Load HubSpot stage ID → ACE stage mapping from environment.

    Set STAGE_MAPPING as a semicolon-separated list of stage_id=ACE Stage pairs.
    Example: STAGE_MAPPING="qualified=Qualified;pov=Technical Validation;closed_won=Launched"

    Valid ACE stages: Qualified, Technical Validation, Business Validation,
                      Committed, Launched, Closed Lost
    """
    raw = os.environ.get("STAGE_MAPPING", "")
    if not raw:
        return {}
    mapping = {}
    for pair in raw.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        hs_id, ace_stage = pair.split("=", 1)
        mapping[hs_id.strip()] = ace_stage.strip()
    return mapping


def _load_stage_display_names() -> dict[str, str]:
    """Load HubSpot stage ID → display name mapping from environment.

    Set STAGE_DISPLAY_NAMES as a semicolon-separated list of stage_id=Display Name pairs.
    Example: STAGE_DISPLAY_NAMES="qualified=Qualified;pov=Proof of Value;negotiation=Negotiation"
    """
    raw = os.environ.get("STAGE_DISPLAY_NAMES", "")
    if not raw:
        return {}
    mapping = {}
    for pair in raw.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        hs_id, name = pair.split("=", 1)
        mapping[hs_id.strip()] = name.strip()
    return mapping


def _load_skip_stages() -> list[str]:
    """Load stages to skip (too early for ACE) from environment.

    Set SKIP_STAGES as a comma-separated list of HubSpot stage IDs.
    Example: SKIP_STAGES="discovery,demo"
    """
    raw = os.environ.get("SKIP_STAGES", "")
    return [s.strip() for s in raw.split(",") if s.strip()] if raw else []


def _load_sync_eligible_stages() -> list[str]:
    """Load stages eligible for sync from environment.

    Set SYNC_ELIGIBLE_STAGES as a comma-separated list of HubSpot stage IDs.
    Example: SYNC_ELIGIBLE_STAGES="qualified,pov,business_validation,negotiation,closed_won,closedlost"
    """
    raw = os.environ.get("SYNC_ELIGIBLE_STAGES", "")
    return [s.strip() for s in raw.split(",") if s.strip()] if raw else []


# HubSpot stage ID → ACE stage name
STAGE_TO_ACE: dict[str, str] = _load_stage_mapping()

# HubSpot stage ID → human-readable display name
STAGE_DISPLAY_NAME: dict[str, str] = _load_stage_display_names()

# Stages eligible for sync
SYNC_ELIGIBLE_STAGES: list[str] = _load_sync_eligible_stages()

# Stages to skip (too early for ACE)
SKIP_STAGES: list[str] = _load_skip_stages()


# ACE stage order for regression detection (higher = further along)
ACE_STAGE_ORDER: dict[str, int] = {
    "Qualified": 1,
    "Technical Validation": 2,
    "Business Validation": 3,
    "Committed": 4,
    "Launched": 5,
    "Closed Lost": 6,
}

# SalesActivities progression — cumulative based on ACE stage
STAGE_TO_SALES_ACTIVITIES: dict[str, list[str]] = {
    "Qualified": [
        "Initialized discussions with customer",
        "Customer has shown interest in solution",
    ],
    "Technical Validation": [
        "Initialized discussions with customer",
        "Customer has shown interest in solution",
        "Conducted POC / Demo",
    ],
    "Business Validation": [
        "Initialized discussions with customer",
        "Customer has shown interest in solution",
        "Conducted POC / Demo",
        "In evaluation / planning stage",
    ],
    "Committed": [
        "Initialized discussions with customer",
        "Customer has shown interest in solution",
        "Conducted POC / Demo",
        "In evaluation / planning stage",
        "Agreed on solution to Business Problem",
    ],
    "Launched": [
        "Initialized discussions with customer",
        "Customer has shown interest in solution",
        "Conducted POC / Demo",
        "In evaluation / planning stage",
        "Agreed on solution to Business Problem",
    ],
}

# Stage-aware next steps for LifeCycle
STAGE_TO_NEXT_STEPS: dict[str, str] = {
    "Qualified": "Initial qualification — discovery and requirements gathering",
    "Technical Validation": "Technical proof of value in progress",
    "Business Validation": "Business case and commercial terms under review",
    "Committed": "Contract negotiations and final approvals",
    "Launched": "Customer onboarded — solution deployed",
    "Closed Lost": "Opportunity closed",
}

# =============================================================================
# HubSpot Loss Reason → ACE ClosedLostReason
# =============================================================================

HS_CLOSED_LOST_REASON = "closed_lost_reason"
HS_LOSS_REASON = "loss_reason"

LOSS_REASON_TO_ACE: dict[str, str] = {
    "Competitive Loss": "Lost to Competitor - Other",
    "Budget Constraints": "Financial/Commercial",
    "Technical Requirements": "Technical Limitations",
    "No Decision/Stalled": "Delay / Cancellation of Project",
    "Timing/Not Ready": "Delay / Cancellation of Project",
    "Strategic Mismatch": "Other",
    "Unresponsive": "People/Relationship/Governance",
}
DEFAULT_CLOSED_LOST_REASON = "Other"

# =============================================================================
# Deal Type → ACE Opportunity Type
# =============================================================================

DEALTYPE_TO_ACE_OPPORTUNITY_TYPE: dict[str, str] = {
    "newbusiness": "Net New Business",
    "existingbusiness": "Expansion",
    "Renewal": "Flat Renewal",
}
DEFAULT_OPPORTUNITY_TYPE = "Net New Business"

# =============================================================================
# HubSpot Industry → AWS Industry Mapping
# AWS has ~25 fixed picklist values. HubSpot has ~150.
# =============================================================================

INDUSTRY_TO_AWS: dict[str, str] = {
    # Technology
    "COMPUTER_SOFTWARE": "Software and Internet",
    "INFORMATION_TECHNOLOGY_AND_SERVICES": "Software and Internet",
    "COMPUTER_HARDWARE": "Computers and Electronics",
    "COMPUTER_NETWORKING": "Computers and Electronics",
    "INTERNET": "Software and Internet",
    "COMPUTER_AND_NETWORK_SECURITY": "Software and Internet",
    "SEMICONDUCTORS": "Computers and Electronics",
    # Financial
    "FINANCIAL_SERVICES": "Financial Services",
    "BANKING": "Financial Services",
    "INSURANCE": "Financial Services",
    "INVESTMENT_MANAGEMENT": "Financial Services",
    "CAPITAL_MARKETS": "Financial Services",
    "VENTURE_CAPITAL_AND_PRIVATE_EQUITY": "Financial Services",
    "ACCOUNTING": "Financial Services",
    # Healthcare
    "HOSPITAL_AND_HEALTH_CARE": "Healthcare",
    "HEALTH_WELLNESS_AND_FITNESS": "Healthcare",
    "MEDICAL_PRACTICE": "Healthcare",
    "MEDICAL_DEVICE": "Healthcare",
    "PHARMACEUTICALS": "Healthcare",
    "BIOTECHNOLOGY": "Healthcare",
    "MENTAL_HEALTH_CARE": "Healthcare",
    # Government
    "GOVERNMENT_ADMINISTRATION": "Government",
    "GOVERNMENT_RELATIONS": "Government",
    "MILITARY": "Government",
    "PUBLIC_SAFETY": "Government",
    "POLITICAL_ORGANIZATION": "Government",
    "JUDICIARY": "Government",
    "LEGISLATIVE_OFFICE": "Government",
    "INTERNATIONAL_AFFAIRS": "Government",
    # Education
    "EDUCATION_MANAGEMENT": "Education",
    "HIGHER_EDUCATION": "Education",
    "PRIMARY_SECONDARY_EDUCATION": "Education",
    "E_LEARNING": "Education",
    "RESEARCH": "Education",
    # Energy
    "OIL_AND_ENERGY": "Energy - Oil and Gas",
    "RENEWABLES_AND_ENVIRONMENT": "Energy - Power and Utilities",
    "UTILITIES": "Energy - Power and Utilities",
    "MINING_AND_METALS": "Mining",
    # Retail
    "RETAIL": "Retail",
    "CONSUMER_GOODS": "Consumer Goods",
    "CONSUMER_ELECTRONICS": "Consumer Goods",
    "FOOD_AND_BEVERAGES": "Consumer Goods",
    "WINE_AND_SPIRITS": "Consumer Goods",
    "FOOD_PRODUCTION": "Consumer Goods",
    "CONSUMER_SERVICES": "Consumer Goods",
    # Manufacturing
    "AUTOMOTIVE": "Automotive",
    "MECHANICAL_OR_INDUSTRIAL_ENGINEERING": "Manufacturing",
    "INDUSTRIAL_AUTOMATION": "Manufacturing",
    "ELECTRICAL_AND_ELECTRONIC_MANUFACTURING": "Computers and Electronics",
    "MACHINERY": "Manufacturing",
    "PLASTICS": "Manufacturing",
    "TEXTILES": "Manufacturing",
    "CHEMICALS": "Manufacturing",
    "BUILDING_MATERIALS": "Manufacturing",
    "PAPER_AND_FOREST_PRODUCTS": "Manufacturing",
    "GLASS_CERAMICS_AND_CONCRETE": "Manufacturing",
    "PACKAGING_AND_CONTAINERS": "Manufacturing",
    # Media & Entertainment
    "MEDIA_PRODUCTION": "Media and Entertainment",
    "ENTERTAINMENT": "Media and Entertainment",
    "MUSIC": "Media and Entertainment",
    "MOTION_PICTURES_AND_FILM": "Media and Entertainment",
    "BROADCAST_MEDIA": "Media and Entertainment",
    "PUBLISHING": "Media and Entertainment",
    "NEWSPAPERS": "Media and Entertainment",
    "ONLINE_MEDIA": "Media and Entertainment",
    "COMPUTER_GAMES": "Gaming",
    # Telecommunications
    "TELECOMMUNICATIONS": "Telecommunications",
    "WIRELESS": "Telecommunications",
    # Transportation & Logistics
    "LOGISTICS_AND_SUPPLY_CHAIN": "Transportation and Logistics",
    "TRANSPORTATION_TRUCKING_RAILROAD": "Transportation and Logistics",
    "AIRLINES_AVIATION": "Transportation and Logistics",
    "MARITIME": "Transportation and Logistics",
    "WAREHOUSING": "Transportation and Logistics",
    "PACKAGE_FREIGHT_DELIVERY": "Transportation and Logistics",
    "RAILROAD_MANUFACTURE": "Transportation and Logistics",
    # Real Estate & Construction
    "REAL_ESTATE": "Real Estate and Construction",
    "CONSTRUCTION": "Real Estate and Construction",
    "COMMERCIAL_REAL_ESTATE": "Real Estate and Construction",
    "ARCHITECTURE_AND_PLANNING": "Real Estate and Construction",
    "CIVIL_ENGINEERING": "Real Estate and Construction",
    # Professional Services
    "MANAGEMENT_CONSULTING": "Professional Services",
    "LEGAL_SERVICES": "Professional Services",
    "HUMAN_RESOURCES": "Professional Services",
    "STAFFING_AND_RECRUITING": "Professional Services",
    "MARKET_RESEARCH": "Professional Services",
    "PUBLIC_RELATIONS_AND_COMMUNICATIONS": "Professional Services",
    "DESIGN": "Professional Services",
    "GRAPHIC_DESIGN": "Professional Services",
    "MARKETING_AND_ADVERTISING": "Marketing and Advertising",
    "EVENTS_SERVICES": "Professional Services",
    "EXECUTIVE_OFFICE": "Professional Services",
    "OUTSOURCING_OFFSHORING": "Professional Services",
    "TRANSLATION_AND_LOCALIZATION": "Professional Services",
    "BUSINESS_SUPPLIES_AND_EQUIPMENT": "Professional Services",
    "FACILITIES_SERVICES": "Professional Services",
    "ENVIRONMENTAL_SERVICES": "Professional Services",
    "INFORMATION_SERVICES": "Professional Services",
    "PROGRAM_DEVELOPMENT": "Professional Services",
    "THINK_TANKS": "Professional Services",
    "SECURITY_AND_INVESTIGATIONS": "Professional Services",
    "LAW_ENFORCEMENT": "Professional Services",
    # Hospitality
    "HOSPITALITY": "Hospitality",
    "RESTAURANTS": "Hospitality",
    "LEISURE_TRAVEL_AND_TOURISM": "Travel",
    "GAMBLING_AND_CASINOS": "Hospitality",
    "RECREATIONAL_FACILITIES_AND_SERVICES": "Hospitality",
    "SPORTS": "Hospitality",
    # Non-Profit
    "NONPROFIT_ORGANIZATION_MANAGEMENT": "Non-Profit Organization",
    "PHILANTHROPY": "Non-Profit Organization",
    "CIVIC_AND_SOCIAL_ORGANIZATION": "Non-Profit Organization",
    "RELIGIOUS_INSTITUTIONS": "Non-Profit Organization",
    "FUND_RAISING": "Non-Profit Organization",
    # Agriculture
    "FARMING": "Agriculture",
    "RANCHING": "Agriculture",
    "DAIRY": "Agriculture",
    "FISHERY": "Agriculture",
    # Aerospace & Defense
    "AVIATION_AND_AEROSPACE": "Aerospace",
    "DEFENSE_AND_SPACE": "Aerospace",
    # Life Sciences
    "NANOTECHNOLOGY": "Life Sciences",
    "ALTERNATIVE_MEDICINE": "Life Sciences",
}

DEFAULT_AWS_INDUSTRY = "Software and Internet"


# =============================================================================
# Runtime Config
# =============================================================================


VALID_ACE_STAGES = {"Qualified", "Technical Validation", "Business Validation", "Committed", "Launched", "Closed Lost"}


def validate_config() -> list[str]:
    """Validate configuration at startup. Returns list of error messages (empty = valid).

    Call this before running the sync to catch configuration mistakes early.
    """
    errors: list[str] = []

    if not os.environ.get("AWS_ACE_ACCESS_KEY_ID"):
        errors.append("AWS_ACE_ACCESS_KEY_ID is not set. See .env.example for setup instructions.")
    if not os.environ.get("AWS_ACE_SECRET_ACCESS_KEY"):
        errors.append("AWS_ACE_SECRET_ACCESS_KEY is not set. See .env.example for setup instructions.")
    if not os.environ.get("HUBSPOT_API_KEY"):
        errors.append("HUBSPOT_API_KEY is not set. Create a HubSpot Private App — see README.md.")
    if not os.environ.get("ACE_SOLUTION_ID"):
        errors.append("ACE_SOLUTION_ID is not set. Find your solution ID in AWS Partner Central.")

    if not STAGE_TO_ACE:
        errors.append(
            "STAGE_MAPPING is not set. You must map your HubSpot stage IDs to ACE stages.\n"
            '  Example: STAGE_MAPPING="qualified=Qualified;eval=Technical Validation;closedlost=Closed Lost"\n'
            "  See .env.example for full instructions."
        )
    else:
        for hs_id, ace_stage in STAGE_TO_ACE.items():
            if ace_stage not in VALID_ACE_STAGES:
                errors.append(
                    f'STAGE_MAPPING: "{hs_id}={ace_stage}" — "{ace_stage}" is not a valid ACE stage.\n'
                    f"  Valid stages: {', '.join(sorted(VALID_ACE_STAGES))}"
                )

    if not SYNC_ELIGIBLE_STAGES:
        errors.append(
            "SYNC_ELIGIBLE_STAGES is not set. Specify which HubSpot stage IDs should be synced.\n"
            '  Example: SYNC_ELIGIBLE_STAGES="qualified,eval,negotiation,closed_won,closedlost"'
        )

    return errors


@dataclass
class ACEConfig:
    """Runtime configuration for ACE sync."""

    aws_access_key_id: str = field(default_factory=lambda: os.environ.get("AWS_ACE_ACCESS_KEY_ID", ""))
    aws_secret_access_key: str = field(default_factory=lambda: os.environ.get("AWS_ACE_SECRET_ACCESS_KEY", ""))
    catalog: str = field(default_factory=lambda: os.environ.get("ACE_CATALOG", "AWS"))
    solution_id: str = field(default_factory=lambda: os.environ.get("ACE_SOLUTION_ID", ""))
    dry_run: bool = False
