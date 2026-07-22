import logging

from sync_app.core.models import DirectoryUserRecord
from sync_app.services.ad_sync import ADSyncLDAPS
from sync_app.services.runtime_user_phase import classify_user_operation


def test_new_ad_identity_is_planned_as_create():
    assert (
        classify_user_operation(
            None,
            target_ou_dn="OU=China,DC=example,DC=local",
            is_enabled=False,
            rehire_restore_enabled=True,
        )
        == "create_user"
    )


def test_existing_identity_in_different_ou_is_planned_as_move():
    existing = DirectoryUserRecord(
        username="alice",
        dn="CN=Alice,OU=Old,DC=example,DC=local",
    )

    assert (
        classify_user_operation(
            existing,
            target_ou_dn="OU=China,DC=example,DC=local",
            is_enabled=True,
            rehire_restore_enabled=True,
        )
        == "move_user"
    )


def test_existing_identity_in_same_ou_is_planned_as_update_case_insensitively():
    existing = DirectoryUserRecord(
        username="alice",
        dn="CN=Alice,OU=China,DC=example,DC=local",
    )

    assert (
        classify_user_operation(
            existing,
            target_ou_dn="ou=china,dc=EXAMPLE,dc=local",
            is_enabled=True,
            rehire_restore_enabled=True,
        )
        == "update_user"
    )


def test_rehire_reactivation_takes_precedence_over_ou_move():
    existing = {
        "distinguishedName": "CN=Alice,OU=Disabled,DC=example,DC=local"
    }

    assert (
        classify_user_operation(
            existing,
            target_ou_dn="OU=China,DC=example,DC=local",
            is_enabled=False,
            rehire_restore_enabled=True,
        )
        == "reactivate_user"
    )


def test_escaped_comma_in_common_name_does_not_break_ou_comparison():
    existing = DirectoryUserRecord(
        username="alice",
        dn=r"CN=Doe\, Alice,OU=China,DC=example,DC=local",
    )

    assert (
        classify_user_operation(
            existing,
            target_ou_dn="OU=China,DC=example,DC=local",
            is_enabled=True,
            rehire_restore_enabled=False,
        )
        == "update_user"
    )


def test_reactivate_enables_the_refreshed_dn_after_update_moves_the_user():
    class Connection:
        result = {}

        def __init__(self):
            self.modified_dns = []

        def modify(self, distinguished_name, _changes):
            self.modified_dns.append(distinguished_name)
            return True

    sync = ADSyncLDAPS.__new__(ADSyncLDAPS)
    sync.logger = logging.getLogger(__name__)
    sync.connection = Connection()
    sync._is_protected_account = lambda _username: False
    users = iter(
        [
            {"dn": "CN=Alice,OU=Disabled,DC=example,DC=local"},
            {"dn": "CN=Alice,OU=China,DC=example,DC=local"},
        ]
    )
    sync.get_user = lambda _username: next(users)
    sync.update_user = lambda *_args, **_kwargs: True

    assert sync.reactivate_user(
        "alice",
        "Alice",
        "alice@example.local",
        "OU=China,DC=example,DC=local",
    )
    assert sync.connection.modified_dns == [
        "CN=Alice,OU=China,DC=example,DC=local"
    ]
