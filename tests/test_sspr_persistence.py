from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from sync_app.modules.sspr import (
    SSPRVerifiedIdentity,
    SQLiteSSPROAuthTransactionStore,
    SQLiteSSPRRateLimitStore,
    SQLiteSSPRResetReceiptStore,
    SQLiteSSPRSessionStore,
)
from sync_app.modules.sspr.repositories import hash_capability
from sync_app.storage.local_db import DatabaseManager
from sync_app.storage.repositories import UserIdentityBindingRepository


class SSPRPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db = DatabaseManager(db_path=f"{self.temp_dir.name}/app.db")
        self.db.initialize(create_startup_snapshot=False)
        self.now = datetime(2026, 7, 13, 4, 0, tzinfo=timezone.utc)

    @staticmethod
    def _identity():
        return SSPRVerifiedIdentity(
            org_id="tenant-a",
            provider_id="dingtalk",
            connector_id="ad-primary",
            source_user_id="alice.dd",
            display_name="Alice",
        )

    def test_session_store_hashes_capabilities_validates_context_and_consumes_once(self):
        store = SQLiteSSPRSessionStore(
            self.db,
            now_factory=lambda: self.now,
            token_factory=lambda: "plain-session-capability",
            csrf_token_factory=lambda: "plain-csrf-capability",
        )

        session = store.create_session(
            self._identity(),
            request_ip="10.0.0.1",
            user_agent="DingTalk/7",
            ttl_seconds=600,
        )

        with self.db.connection() as connection:
            row = connection.execute("SELECT * FROM sspr_verification_sessions").fetchone()
        self.assertEqual(row["token_hash"], hash_capability("plain-session-capability"))
        self.assertEqual(row["csrf_token_hash"], hash_capability("plain-csrf-capability"))
        self.assertNotIn("plain-session-capability", dict(row).values())
        self.assertNotIn("plain-csrf-capability", dict(row).values())
        self.assertTrue(store.validate_csrf_token(session.session_id, session.csrf_token))
        self.assertIsNotNone(
            store.validate_session(
                session.session_id,
                org_id="tenant-a",
                provider_id="dingtalk",
                connector_id="ad-primary",
                source_user_id="alice.dd",
                request_ip="10.0.0.99",
                user_agent="DingTalk/7",
            )
        )
        self.assertIsNone(store.validate_session(session.session_id, provider_id="wecom", user_agent="DingTalk/7"))
        self.assertIsNone(store.validate_session(session.session_id, user_agent="Different-Agent"))
        self.assertIsNone(store.validate_session(session.session_id, user_agent=""))

        claimed = store.claim_session(
            session.session_id,
            org_id="tenant-a",
            provider_id="dingtalk",
            connector_id="ad-primary",
            source_user_id="alice.dd",
            user_agent="DingTalk/7",
        )
        self.assertIsNotNone(claimed)
        self.now += timedelta(seconds=120)
        self.assertIsNone(
            store.claim_session(
                session.session_id,
                org_id="tenant-a",
                provider_id="dingtalk",
                connector_id="ad-primary",
                source_user_id="alice.dd",
                user_agent="DingTalk/7",
            )
        )
        self.assertTrue(store.consume_claim(session.session_id, claimed[1]))
        self.assertIsNone(store.validate_session(session.session_id, user_agent="DingTalk/7"))
        self.assertFalse(store.consume_claim(session.session_id, claimed[1]))

    def test_session_expiration_and_revocation_are_enforced(self):
        tokens = iter(("expires", "revoked"))
        store = SQLiteSSPRSessionStore(
            self.db,
            now_factory=lambda: self.now,
            token_factory=lambda: next(tokens),
            csrf_token_factory=lambda: "csrf-" + str(self.now.timestamp()),
        )
        expires = store.create_session(self._identity(), user_agent="agent", ttl_seconds=60)
        self.now += timedelta(seconds=61)
        self.assertIsNone(store.validate_session(expires.session_id, user_agent="agent"))

        revoked = store.create_session(self._identity(), user_agent="agent", ttl_seconds=60)
        store.invalidate(revoked.session_id)
        self.assertIsNone(store.validate_session(revoked.session_id, user_agent="agent"))

    def test_oauth_state_and_reset_receipt_are_hashed_short_lived_and_one_time(self):
        oauth_store = SQLiteSSPROAuthTransactionStore(
            self.db,
            now_factory=lambda: self.now,
            token_factory=lambda: "plain-oauth-state",
        )
        transaction = oauth_store.create_transaction(
            org_id="tenant-a",
            provider_id="dingtalk",
            connector_id="ad-primary",
            corp_id="ding-corp",
            return_path="https://evil.example/steal",
            user_agent="agent",
        )
        with self.db.connection() as connection:
            row = connection.execute("SELECT * FROM sspr_oauth_transactions").fetchone()
        self.assertEqual(row["state_hash"], hash_capability("plain-oauth-state"))
        self.assertNotIn("plain-oauth-state", dict(row).values())
        self.assertEqual(transaction.return_path, "/sspr/account")
        self.assertIsNone(oauth_store.consume_transaction(transaction.state, user_agent="other"))
        self.assertIsNone(oauth_store.consume_transaction(transaction.state, user_agent=""))
        consumed = oauth_store.consume_transaction(transaction.state, user_agent="agent")
        self.assertIsNotNone(consumed)
        self.assertIsNone(oauth_store.consume_transaction(transaction.state, user_agent="agent"))

        receipt_store = SQLiteSSPRResetReceiptStore(
            self.db,
            now_factory=lambda: self.now,
            token_factory=lambda: "plain-result-receipt",
        )
        receipt = receipt_store.create_receipt(
            org_id="tenant-a",
            ad_username="alice.ad",
            unlock_requested=True,
            unlock_succeeded=False,
        )
        with self.db.connection() as connection:
            row = connection.execute("SELECT * FROM sspr_reset_receipts").fetchone()
        self.assertEqual(row["token_hash"], hash_capability("plain-result-receipt"))
        self.assertNotIn("plain-result-receipt", dict(row).values())
        self.assertIsNotNone(receipt_store.consume_receipt(receipt.token))
        self.assertIsNone(receipt_store.consume_receipt(receipt.token))

    def test_rate_limit_store_is_shared_and_persistent(self):
        first = SQLiteSSPRRateLimitStore(self.db)
        second = SQLiteSSPRRateLimitStore(self.db)

        self.assertEqual(
            first.evaluate(
                "bucket",
                now=self.now,
                max_attempts=2,
                window_seconds=60,
                lockout_seconds=120,
                record_failure=True,
            ),
            (False, 0),
        )
        limited, retry_after = second.evaluate(
            "bucket",
            now=self.now,
            max_attempts=2,
            window_seconds=60,
            lockout_seconds=120,
            record_failure=True,
        )
        self.assertTrue(limited)
        self.assertGreaterEqual(retry_after, 1)

    def test_identity_bindings_are_provider_and_connector_scoped(self):
        repository = UserIdentityBindingRepository(self.db)
        repository.upsert_binding(
            "same-user",
            "ding.primary",
            org_id="tenant-a",
            source_provider="dingtalk",
            connector_id="ad-primary",
            preserve_manual=False,
        )
        repository.upsert_binding(
            "same-user",
            "ding.asia",
            org_id="tenant-a",
            source_provider="dingtalk",
            connector_id="ad-asia",
            preserve_manual=False,
        )
        repository.upsert_binding(
            "same-user",
            "wecom.primary",
            org_id="tenant-a",
            source_provider="wecom",
            connector_id="ad-primary",
            preserve_manual=False,
        )

        dingtalk_primary = repository.list_binding_records_for_source_identity(
            "same-user",
            org_id="tenant-a",
            source_provider="dingtalk",
            connector_id="ad-primary",
            enabled_only=True,
        )
        dingtalk_all = repository.list_binding_records_for_source_identity(
            "same-user",
            org_id="tenant-a",
            source_provider="dingtalk",
            enabled_only=True,
        )

        self.assertEqual([item.ad_username for item in dingtalk_primary], ["ding.primary"])
        self.assertEqual(
            {item.ad_username for item in dingtalk_all},
            {"ding.primary", "ding.asia"},
        )


if __name__ == "__main__":
    unittest.main()
