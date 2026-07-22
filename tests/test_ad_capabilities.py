from sync_app.services.ad_capabilities import (
    AD_CAPABILITY_ORDER,
    build_ad_capability_report,
)
from sync_app.services.runtime_bootstrap import normalize_ad_directory_mode


def test_writable_mode_does_not_claim_unprobed_write_permissions():
    report = build_ad_capability_report(
        connected=True,
        user_read_succeeded=True,
        ou_read_succeeded=True,
        directory_mode="writable",
        use_ssl=True,
        validate_cert=True,
    )

    assert tuple(report["capabilities"]) == AD_CAPABILITY_ORDER
    assert report["capabilities"]["read_users"]["status"] == "success"
    assert report["capabilities"]["create_user"]["status"] == "not_tested"
    assert not report["capabilities"]["create_user"]["verified"]
    assert report["capabilities"]["ldaps_certificate"]["status"] == "success"


def test_read_only_mode_explicitly_blocks_every_write_capability():
    report = build_ad_capability_report(
        connected=True,
        user_read_succeeded=True,
        ou_read_succeeded=True,
        directory_mode="read_only",
        use_ssl=True,
        validate_cert=False,
    )

    for key in (
        "create_user",
        "update_user",
        "move_user",
        "disable_user",
        "password_operation",
    ):
        assert report["capabilities"][key]["status"] == "blocked"
    assert report["capabilities"]["ldaps_certificate"]["status"] == "warning"


def test_disposable_probe_results_are_reported_per_operation():
    report = build_ad_capability_report(
        connected=True,
        user_read_succeeded=True,
        ou_read_succeeded=True,
        directory_mode="writable",
        write_probe_results={"create_user": True, "move_user": False},
    )

    assert report["capabilities"]["create_user"]["status"] == "success"
    assert report["capabilities"]["move_user"]["status"] == "failed"
    assert report["capabilities"]["disable_user"]["status"] == "not_tested"


def test_invalid_directory_mode_fails_closed_to_read_only():
    assert normalize_ad_directory_mode("writable") == "writable"
    assert normalize_ad_directory_mode("read_only") == "read_only"
    assert normalize_ad_directory_mode("corrupted-setting") == "read_only"
    assert normalize_ad_directory_mode("") == "read_only"
