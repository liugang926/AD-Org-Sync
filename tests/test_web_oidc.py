import base64
import hashlib
import json
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from sync_app.web.oidc import OIDCError, OIDCService, OIDCSettings


TEST_RSA_N = int(
    "20770158568359910009636774433702548126537931925357946314517347804165763110770527338768229636087999524893297610752796398228505921301825660518339974810008977112586437337654597565277347930596949550610397414770047796029759786885898430803767656534279644371723106025970120439036194612403679410511780842104360793918429192474522853865369794949039166278365041279079461766807741327201445424020426465538287467872198258085814236130899641539376061308206461479053915216221945190044886139325572172685780638019269622499574641596281686729762480967471758038495370787779694086852330903044337919467058713833854839079781997853608505244129"
)
TEST_RSA_E = 65537
TEST_RSA_D = int(
    "1721206817290426343322662342637571736198292693999099843357854584805899865031886323402814519334023916561568264095067476979096016884969027606925925861622575868569768850890369094969578659552192395278469694365261296370563580917303422153825505327336813534077363761341589088673658741473738237613706482650545241188480145135070869528046497597467962380816436659960427792419487699921626154195125126723349070481111354134613923236680193236789647027993784355241265736955232149514333382899262073975865203461022784378143848799004869503017289777379660686960779863324285293660094729280651701322254963363752999661977691951485451814353"
)


class _FakeResponse:
    def __init__(self, payload, *, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class _FakeHTTPSession:
    def __init__(self, metadata):
        self.metadata = metadata
        self.token_payload = {}
        self.userinfo_payload = {}
        self.jwks_payload = {
            "keys": [
                {
                    "kid": "test-key",
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "n": _int_base64url(TEST_RSA_N),
                    "e": _int_base64url(TEST_RSA_E),
                }
            ]
        }
        self.posts = []
        self.gets = []

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        if url.endswith("openid-configuration"):
            return _FakeResponse(self.metadata)
        if url == self.metadata["userinfo_endpoint"]:
            return _FakeResponse(self.userinfo_payload)
        if url == self.metadata["jwks_uri"]:
            return _FakeResponse(self.jwks_payload)
        return _FakeResponse({}, status_code=404)

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url == self.metadata["token_endpoint"]:
            return _FakeResponse(self.token_payload)
        return _FakeResponse({}, status_code=404)


def _base64url(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _int_base64url(value):
    return _base64url(value.to_bytes((value.bit_length() + 7) // 8, "big"))


def _signed_id_token(payload):
    header = _base64url(json.dumps({"alg": "RS256", "typ": "JWT", "kid": "test-key"}).encode("utf-8"))
    body = _base64url(json.dumps(payload).encode("utf-8"))
    signing_input = f"{header}.{body}".encode("ascii")
    digest_info = bytes.fromhex("3031300d060960864801650304020105000420") + hashlib.sha256(
        signing_input
    ).digest()
    encoded_length = (TEST_RSA_N.bit_length() + 7) // 8
    padding_length = encoded_length - len(digest_info) - 3
    encoded_message = b"\x00\x01" + (b"\xff" * padding_length) + b"\x00" + digest_info
    signature = pow(int.from_bytes(encoded_message, "big"), TEST_RSA_D, TEST_RSA_N).to_bytes(
        encoded_length,
        "big",
    )

    return f"{header}.{body}.{_base64url(signature)}"


class OIDCSettingsTests(unittest.TestCase):
    def test_environment_configuration_is_explicit_and_defaults_to_existing_user_mapping(self):
        values = {
            "AD_ORG_SYNC_OIDC_ENABLED": "true",
            "AD_ORG_SYNC_OIDC_DISCOVERY_URL": "https://id.example/.well-known/openid-configuration",
            "AD_ORG_SYNC_OIDC_CLIENT_ID": "console-client",
            "AD_ORG_SYNC_OIDC_CLIENT_SECRET": "secret",
            "AD_ORG_SYNC_OIDC_MFA_REQUIRED": "true",
            "AD_ORG_SYNC_OIDC_ACCEPTED_MFA_METHODS": "MFA, OTP",
            "AD_ORG_SYNC_ENVIRONMENT_LABEL": "Production / Shanghai",
        }
        with patch.dict("os.environ", values, clear=True):
            settings = OIDCSettings.from_environment()

        self.assertTrue(settings.configured)
        self.assertTrue(settings.mfa_required)
        self.assertEqual(settings.accepted_mfa_methods, ("mfa", "otp"))
        self.assertEqual(settings.username_claim, "preferred_username")
        self.assertEqual(settings.environment_label, "Production / Shanghai")


class OIDCServiceTests(unittest.TestCase):
    def setUp(self):
        self.metadata = {
            "issuer": "https://id.example",
            "authorization_endpoint": "https://id.example/authorize",
            "token_endpoint": "https://id.example/token",
            "userinfo_endpoint": "https://id.example/userinfo",
            "jwks_uri": "https://id.example/jwks",
        }
        self.http = _FakeHTTPSession(self.metadata)
        self.settings = OIDCSettings(
            enabled=True,
            discovery_url="https://id.example/.well-known/openid-configuration",
            client_id="console-client",
            client_secret="secret",
        )
        self.now = time.time()
        self.service = OIDCService(self.settings, http_session=self.http, clock=lambda: self.now)

    def test_begin_uses_state_nonce_and_pkce(self):
        authorization_url, transaction = self.service.begin(
            redirect_uri="https://console.example/auth/oidc/callback"
        )
        query = parse_qs(urlparse(authorization_url).query)

        self.assertEqual(query["client_id"], ["console-client"])
        self.assertEqual(query["response_type"], ["code"])
        self.assertEqual(query["code_challenge_method"], ["S256"])
        self.assertEqual(query["state"], [transaction["state"]])
        self.assertEqual(query["nonce"], [transaction["nonce"]])
        self.assertNotEqual(query["code_challenge"], [transaction["verifier"]])

    def test_finish_validates_transaction_and_maps_identity(self):
        _authorization_url, transaction = self.service.begin(
            redirect_uri="https://console.example/auth/oidc/callback"
        )
        claims = {
            "iss": self.metadata["issuer"],
            "aud": self.settings.client_id,
            "sub": "subject-001",
            "nonce": transaction["nonce"],
            "exp": self.now + 300,
            "amr": ["pwd", "mfa"],
        }
        self.http.token_payload = {
            "access_token": "access-token",
            "id_token": _signed_id_token(claims),
        }
        self.http.userinfo_payload = {
            "sub": "subject-001",
            "preferred_username": "admin@example.com",
        }

        identity = self.service.finish(
            query={"code": "code-001", "state": transaction["state"]},
            transaction=transaction,
        )

        self.assertEqual(identity.username, "admin@example.com")
        self.assertEqual(identity.mfa_methods, ("pwd", "mfa"))
        self.assertTrue(identity.mfa_satisfied)
        token_request = self.http.posts[0][1]["data"]
        self.assertEqual(token_request["code_verifier"], transaction["verifier"])
        self.assertEqual(token_request["client_secret"], "secret")

    def test_finish_rejects_state_mismatch_before_token_exchange(self):
        with self.assertRaises(OIDCError):
            self.service.finish(
                query={"code": "code-001", "state": "wrong"},
                transaction={"state": "expected"},
            )
        self.assertEqual(self.http.posts, [])

    def test_required_mfa_rejects_password_only_identity(self):
        settings = OIDCSettings(
            enabled=True,
            discovery_url=self.settings.discovery_url,
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            mfa_required=True,
        )
        service = OIDCService(settings, http_session=self.http, clock=lambda: self.now)
        _authorization_url, transaction = service.begin(
            redirect_uri="https://console.example/auth/oidc/callback"
        )
        self.http.token_payload = {
            "access_token": "access-token",
            "id_token": _signed_id_token(
                {
                    "iss": self.metadata["issuer"],
                    "aud": settings.client_id,
                    "sub": "subject-001",
                    "nonce": transaction["nonce"],
                    "exp": self.now + 300,
                    "amr": ["pwd"],
                }
            ),
        }
        self.http.userinfo_payload = {
            "sub": "subject-001",
            "preferred_username": "admin@example.com",
        }

        with self.assertRaisesRegex(OIDCError, "MFA"):
            service.finish(
                query={"code": "code-001", "state": transaction["state"]},
                transaction=transaction,
            )

    def test_invalid_id_token_signature_is_rejected(self):
        _authorization_url, transaction = self.service.begin(
            redirect_uri="https://console.example/auth/oidc/callback"
        )
        token = _signed_id_token(
            {
                "iss": self.metadata["issuer"],
                "aud": self.settings.client_id,
                "sub": "subject-001",
                "nonce": transaction["nonce"],
                "exp": self.now + 300,
            }
        )
        signature = token.rsplit(".", 1)[1]
        tampered_signature = ("A" if signature[0] != "A" else "B") + signature[1:]
        self.http.token_payload = {
            "access_token": "access-token",
            "id_token": f"{token.rsplit('.', 1)[0]}.{tampered_signature}",
        }

        with self.assertRaisesRegex(OIDCError, "signature"):
            self.service.finish(
                query={"code": "code-001", "state": transaction["state"]},
                transaction=transaction,
            )

    def test_non_local_http_endpoints_are_rejected(self):
        settings = OIDCSettings(
            enabled=True,
            discovery_url="http://id.example/.well-known/openid-configuration",
            client_id="console-client",
            client_secret="secret",
        )
        with self.assertRaisesRegex(OIDCError, "HTTPS"):
            OIDCService(settings, http_session=self.http).discovery()


if __name__ == "__main__":
    unittest.main()
