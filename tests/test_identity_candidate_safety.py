from __future__ import annotations

from sync_app.core.models import SourceDirectoryUser
from sync_app.core.sync_policies import build_identity_candidates


def test_only_employee_id_can_claim_an_existing_ad_username_automatically() -> None:
    candidates = build_identity_candidates(
        SourceDirectoryUser(
            userid="source-alice",
            name="Alice",
            email="alice@example.com",
            employee_id="E10018",
        ),
        username_strategy="userid",
    )

    claim_candidates = [
        candidate for candidate in candidates if candidate["allow_existing_match"]
    ]
    assert claim_candidates == [
        {
            "rule": "existing_ad_employee_id",
            "username": "E10018",
            "explanation": "Employee ID maps directly to an existing AD username",
            "allow_existing_match": True,
            "managed": False,
        }
    ]
    assert not any(
        candidate["username"].casefold() in {"source-alice", "alice"}
        and candidate["allow_existing_match"]
        for candidate in candidates
    )


def test_missing_employee_id_never_claims_by_source_id_or_email_localpart() -> None:
    candidates = build_identity_candidates(
        SourceDirectoryUser(
            userid="source-alice",
            name="Alice",
            email="alice@example.com",
        ),
        username_strategy="userid",
    )

    assert all(not candidate["allow_existing_match"] for candidate in candidates)
