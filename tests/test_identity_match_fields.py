from sync_app.web.identity_match_fields import build_ad_match_field_options


def test_detected_readable_identity_field_does_not_require_verified_write_access():
    options = build_ad_match_field_options(
        [],
        [
            {
                "ldap_attribute": "employeeID",
                "display_label": "Employee ID",
                "capability_status": "unavailable_by_permission",
                "schema_detected": True,
                "is_read_only": False,
            }
        ],
    )

    employee_id = next(item for item in options if item["value"] == "employee_id")
    assert employee_id["is_primary_id_candidate"] is True
    assert employee_id["disabled"] is False
    assert employee_id["capability_status"] == "unavailable_by_permission"


def test_undetected_identity_field_remains_disabled():
    options = build_ad_match_field_options(
        [],
        [
            {
                "ldap_attribute": "employeeID",
                "display_label": "Employee ID",
                "capability_status": "not_detected",
                "schema_detected": False,
                "is_read_only": False,
            }
        ],
    )

    employee_id = next(item for item in options if item["value"] == "employee_id")
    assert employee_id["disabled"] is True
