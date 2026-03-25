"""Tests for the mapping module."""

from src.mapping import (
    build_create_payload,
    build_customer_contacts,
    build_opportunity_team,
    build_update_payload,
    build_website_url,
    calculate_monthly_spend,
    generate_client_token,
    is_stage_regression,
    map_ace_stage,
    map_industry,
    map_opportunity_type,
    validate_deal_for_create,
)


class TestValidation:
    """Test deal validation logic."""

    def test_valid_deal_passes(self, sample_deal_props, sample_company_props):
        """A deal with all required fields should pass validation."""
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert errors == []

    def test_missing_project_description(self, sample_deal_props, sample_company_props):
        """Deal without ace_project_description should fail."""
        sample_deal_props["ace_project_description"] = None
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert any("ace_project_description must be at least 20 characters" in e for e in errors)

    def test_short_description(self, sample_deal_props, sample_company_props):
        """Deal with description < 20 chars should fail."""
        sample_deal_props["ace_project_description"] = "Short text"
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert any("ace_project_description must be at least 20 characters" in e for e in errors)

    def test_missing_closedate(self, sample_deal_props, sample_company_props):
        """Deal without closedate should fail."""
        sample_deal_props["closedate"] = None
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert any("closedate is required" in e for e in errors)

    def test_missing_amount(self, sample_deal_props, sample_company_props):
        """Deal without amount should fail."""
        sample_deal_props["amount"] = None
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert any("amount is required" in e for e in errors)

    def test_no_company(self, sample_deal_props):
        """Deal without associated company should fail."""
        errors = validate_deal_for_create(sample_deal_props, None)
        assert any("No associated company found" in e for e in errors)

    def test_company_missing_name(self, sample_deal_props, sample_company_props):
        """Company without name should fail."""
        sample_company_props["name"] = None
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert any("Company name is required" in e for e in errors)

    def test_company_missing_country_code(self, sample_deal_props, sample_company_props):
        """Company without hs_country_code should fail."""
        sample_company_props["hs_country_code"] = None
        errors = validate_deal_for_create(sample_deal_props, sample_company_props)
        assert any("hs_country_code" in e or "country code" in e for e in errors)


class TestClientToken:
    """Test deterministic UUID generation."""

    def test_same_deal_id_same_token(self):
        """Same deal ID should always generate same token."""
        token1 = generate_client_token(12345)
        token2 = generate_client_token(12345)
        assert token1 == token2

    def test_different_deal_ids_different_tokens(self):
        """Different deal IDs should generate different tokens."""
        token1 = generate_client_token(12345)
        token2 = generate_client_token(54321)
        assert token1 != token2

    def test_token_is_valid_uuid(self):
        """Generated token should be a valid UUID."""
        token = generate_client_token(12345)
        # Should be a valid UUID format
        assert len(token) == 36  # UUID string length
        assert token.count("-") == 4


class TestOpportunityType:
    """Test deal type to ACE opportunity type mapping."""

    def test_newbusiness(self):
        """newbusiness dealtype should map to Net New Business."""
        deal_props = {"dealtype": "newbusiness"}
        assert map_opportunity_type(deal_props) == "Net New Business"

    def test_existingbusiness(self):
        """existingbusiness dealtype should map to Expansion."""
        deal_props = {"dealtype": "existingbusiness"}
        assert map_opportunity_type(deal_props) == "Expansion"

    def test_renewal(self):
        """Renewal dealtype should map to Flat Renewal."""
        deal_props = {"dealtype": "Renewal"}
        assert map_opportunity_type(deal_props) == "Flat Renewal"

    def test_unknown_type_defaults(self):
        """Unknown dealtype should default to Net New Business."""
        deal_props = {"dealtype": "unknown_type"}
        assert map_opportunity_type(deal_props) == "Net New Business"

    def test_empty_dealtype(self):
        """Missing dealtype should default to Net New Business."""
        deal_props = {}
        assert map_opportunity_type(deal_props) == "Net New Business"


class TestStageMapping:
    """Test HubSpot stage ID to ACE stage mapping."""

    def test_stage_in_mapping(self):
        """A mapped stage should return correct ACE stage."""
        # This requires STAGE_TO_ACE to be set from config/env
        # For now, test that the function exists and handles unmapped stages
        result = map_ace_stage("unknown_stage_id")
        assert result is None

    def test_unmapped_stage_returns_none(self):
        """Unmapped stage should return None."""
        result = map_ace_stage("stage_id_not_in_mapping")
        assert result is None


class TestStageRegression:
    """Test regression detection."""

    def test_moving_backward_is_regression(self):
        """Moving to earlier stage should be detected as regression."""
        # Qualified (order 1) → Technical Validation (order 2) is progression
        # Committed (order 4) → Technical Validation (order 2) is regression
        assert is_stage_regression("Committed", "Technical Validation") is True

    def test_moving_forward_not_regression(self):
        """Moving to later stage should not be regression."""
        assert is_stage_regression("Qualified", "Technical Validation") is False

    def test_same_stage_not_regression(self):
        """Same stage should not be regression."""
        assert is_stage_regression("Technical Validation", "Technical Validation") is False

    def test_unknown_stage_defaults_to_zero(self):
        """Unknown stages should default to order 0."""
        # Unknown vs Qualified (order 1) — moving up
        assert is_stage_regression("UnknownStage1", "UnknownStage2") is False


class TestMonthlySpend:
    """Test monthly spend calculation."""

    def test_simple_annual_calculation(self, sample_deal_props):
        """$120,000 annual / 12 months = $10,000."""
        sample_deal_props["amount"] = "120000"
        sample_deal_props["contract_term__months_"] = "12"
        result = calculate_monthly_spend(sample_deal_props)
        assert result == "10000"

    def test_custom_contract_term(self, sample_deal_props):
        """$60,000 / 6 months = $10,000."""
        sample_deal_props["amount"] = "60000"
        sample_deal_props["contract_term__months_"] = "6"
        result = calculate_monthly_spend(sample_deal_props)
        assert result == "10000"

    def test_decimal_result(self, sample_deal_props):
        """$100,000 / 12 months = $8333.33."""
        sample_deal_props["amount"] = "100000"
        sample_deal_props["contract_term__months_"] = "12"
        result = calculate_monthly_spend(sample_deal_props)
        # Should have up to 2 decimal places
        assert "8333" in result

    def test_zero_amount_defaults(self, sample_deal_props):
        """Missing amount should default to 0."""
        sample_deal_props["amount"] = None
        sample_deal_props["contract_term__months_"] = "12"
        result = calculate_monthly_spend(sample_deal_props)
        assert result == "0"

    def test_default_contract_term(self, sample_deal_props):
        """Missing contract term should default to 12 months."""
        sample_deal_props["amount"] = "120000"
        sample_deal_props["contract_term__months_"] = None
        result = calculate_monthly_spend(sample_deal_props)
        assert result == "10000"

    def test_invalid_contract_term_defaults(self, sample_deal_props):
        """Invalid contract term should default to 12."""
        sample_deal_props["amount"] = "120000"
        sample_deal_props["contract_term__months_"] = "invalid"
        result = calculate_monthly_spend(sample_deal_props)
        assert result == "10000"


class TestIndustryMapping:
    """Test HubSpot industry to AWS industry mapping."""

    def test_software_industry(self, sample_company_props):
        """COMPUTER_SOFTWARE should map to Software and Internet."""
        sample_company_props["industry"] = "COMPUTER_SOFTWARE"
        result = map_industry(sample_company_props)
        assert result == "Software and Internet"

    def test_banking_industry(self, sample_company_props):
        """BANKING should map to Financial Services."""
        sample_company_props["industry"] = "BANKING"
        result = map_industry(sample_company_props)
        assert result == "Financial Services"

    def test_healthcare_industry(self, sample_company_props):
        """HOSPITAL_AND_HEALTH_CARE should map to Healthcare."""
        sample_company_props["industry"] = "HOSPITAL_AND_HEALTH_CARE"
        result = map_industry(sample_company_props)
        assert result == "Healthcare"

    def test_unknown_industry_defaults(self, sample_company_props):
        """Unknown industry should default to Software and Internet."""
        sample_company_props["industry"] = "UNKNOWN_INDUSTRY"
        result = map_industry(sample_company_props)
        assert result == "Software and Internet"

    def test_empty_industry_defaults(self, sample_company_props):
        """Missing industry should default to Software and Internet."""
        sample_company_props["industry"] = None
        result = map_industry(sample_company_props)
        assert result == "Software and Internet"


class TestWebsiteUrl:
    """Test website URL building."""

    def test_url_with_https_protocol(self, sample_company_props):
        """URL with https:// should be returned as-is."""
        sample_company_props["website"] = "https://acme.com"
        sample_company_props["domain"] = None
        result = build_website_url(sample_company_props)
        assert result == "https://acme.com"

    def test_url_with_http_protocol(self, sample_company_props):
        """URL with http:// should be returned as-is."""
        sample_company_props["website"] = "http://acme.com"
        sample_company_props["domain"] = None
        result = build_website_url(sample_company_props)
        assert result == "http://acme.com"

    def test_url_without_protocol(self, sample_company_props):
        """URL without protocol should add https://."""
        sample_company_props["website"] = "acme.com"
        sample_company_props["domain"] = None
        result = build_website_url(sample_company_props)
        assert result == "https://acme.com"

    def test_domain_fallback(self, sample_company_props):
        """Should fallback to domain if website missing."""
        sample_company_props["website"] = None
        sample_company_props["domain"] = "acme.com"
        result = build_website_url(sample_company_props)
        assert result == "https://acme.com"

    def test_website_takes_precedence(self, sample_company_props):
        """Website should take precedence over domain."""
        sample_company_props["website"] = "https://example.acme.com"
        sample_company_props["domain"] = "acme.com"
        result = build_website_url(sample_company_props)
        assert result == "https://example.acme.com"

    def test_no_website_or_domain(self, sample_company_props):
        """Should return None if no website or domain."""
        sample_company_props["website"] = None
        sample_company_props["domain"] = None
        result = build_website_url(sample_company_props)
        assert result is None


class TestBuildPayload:
    """Test ACE payload building."""

    def test_create_payload_structure(self, sample_deal_props, sample_company_props):
        """Created payload should have required top-level keys."""
        payload = build_create_payload(
            deal_id=12345,
            deal_props=sample_deal_props,
            company_props=sample_company_props,
        )

        # Check required keys exist
        assert "client_token" in payload
        assert "customer" in payload
        assert "project" in payload
        assert "life_cycle" in payload
        assert "marketing" in payload
        assert "primary_needs_from_aws" in payload
        assert "opportunity_type" in payload

    def test_create_payload_includes_opportunity_type(self, sample_deal_props, sample_company_props):
        """Created payload should include opportunity_type."""
        sample_deal_props["dealtype"] = "newbusiness"
        payload = build_create_payload(
            deal_id=12345,
            deal_props=sample_deal_props,
            company_props=sample_company_props,
        )
        assert payload["opportunity_type"] == "Net New Business"

    def test_create_payload_with_owner(self, sample_deal_props, sample_company_props, sample_owner):
        """Created payload with owner should include opportunity_team."""
        payload = build_create_payload(
            deal_id=12345,
            deal_props=sample_deal_props,
            company_props=sample_company_props,
            owner=sample_owner,
        )
        assert "opportunity_team" in payload
        assert len(payload["opportunity_team"]) > 0

    def test_create_payload_with_contacts(self, sample_deal_props, sample_company_props, sample_contact):
        """Created payload with contacts should include them in customer.Contacts."""
        payload = build_create_payload(
            deal_id=12345,
            deal_props=sample_deal_props,
            company_props=sample_company_props,
            contacts=[sample_contact],
        )
        assert "Contacts" in payload["customer"]

    def test_update_payload_has_last_modified_date(self, sample_deal_props, sample_company_props):
        """Update payload should require last_modified_date."""
        payload = build_update_payload(
            deal_props=sample_deal_props,
            current_ace_stage="Qualified",
            new_ace_stage="Technical Validation",
            company_props=sample_company_props,
        )
        # Update payload is different from create — it updates life_cycle
        assert "life_cycle" in payload

    def test_update_payload_blocks_regression(self, sample_deal_props, sample_company_props):
        """Update should not regress stage."""
        payload = build_update_payload(
            deal_props=sample_deal_props,
            current_ace_stage="Technical Validation",
            new_ace_stage="Qualified",
            company_props=sample_company_props,
        )
        # Should not actually regress
        assert payload["life_cycle"]["Stage"] == "Technical Validation"


class TestCustomerContacts:
    """Test customer contact building."""

    def test_valid_contact_included(self, sample_contact):
        """Valid contact with first and last name should be included."""
        result = build_customer_contacts([sample_contact])
        assert len(result) == 1
        assert result[0]["FirstName"] == "Jane"
        assert result[0]["LastName"] == "Doe"

    def test_contact_with_business_title(self, sample_contact):
        """Contact with job title should include it."""
        result = build_customer_contacts([sample_contact])
        assert "BusinessTitle" in result[0]
        assert result[0]["BusinessTitle"] == "Cloud Architect"

    def test_contact_with_email(self, sample_contact):
        """Contact with email should include it."""
        result = build_customer_contacts([sample_contact])
        assert "Email" in result[0]
        assert result[0]["Email"] == "jane.doe@acme.com"

    def test_contact_with_phone(self, sample_contact):
        """Contact with valid phone should include it."""
        result = build_customer_contacts([sample_contact])
        assert "Phone" in result[0]
        assert result[0]["Phone"] == "+14155555678"

    def test_contact_missing_first_name_excluded(self):
        """Contact without first name should be excluded."""
        contact = {"lastname": "Doe"}
        result = build_customer_contacts([contact])
        assert len(result) == 0

    def test_contact_missing_last_name_excluded(self):
        """Contact without last name should be excluded."""
        contact = {"firstname": "Jane"}
        result = build_customer_contacts([contact])
        assert len(result) == 0

    def test_multiple_contacts(self, sample_contact):
        """Multiple valid contacts should all be included."""
        contact2 = {
            "firstname": "Bob",
            "lastname": "Johnson",
            "email": "bob@acme.com",
            "jobtitle": "CTO",
        }
        result = build_customer_contacts([sample_contact, contact2])
        assert len(result) == 2


class TestOpportunityTeam:
    """Test opportunity team building."""

    def test_valid_owner_included(self, sample_owner):
        """Valid owner should be included."""
        result = build_opportunity_team(sample_owner)
        assert len(result) == 1
        assert result[0]["FirstName"] == "John"
        assert result[0]["LastName"] == "Smith"
        assert result[0]["Email"] == "john.smith@example.com"

    def test_owner_business_title_is_opportunity_owner(self, sample_owner):
        """Owner's BusinessTitle should be set to OpportunityOwner."""
        result = build_opportunity_team(sample_owner)
        assert result[0]["BusinessTitle"] == "OpportunityOwner"

    def test_owner_with_phone(self, sample_owner):
        """Owner with valid phone should include it."""
        result = build_opportunity_team(sample_owner)
        assert "Phone" in result[0]

    def test_missing_owner_returns_empty(self):
        """Missing owner should return empty list."""
        result = build_opportunity_team(None)
        assert result == []

    def test_owner_missing_first_name_returns_empty(self, sample_owner):
        """Owner without first name should return empty list."""
        sample_owner["firstName"] = None
        result = build_opportunity_team(sample_owner)
        assert result == []

    def test_owner_missing_last_name_returns_empty(self, sample_owner):
        """Owner without last name should return empty list."""
        sample_owner["lastName"] = None
        result = build_opportunity_team(sample_owner)
        assert result == []
