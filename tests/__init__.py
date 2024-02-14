"""The tests for statbank client should be run by pytest through nox."""

import os

os.environ["JUPYTERHUB_USER"] = "ssb@ssb.no"
os.environ["DAPLA_ENVIRONMENT"] = "TEST"

os.environ["STATBANK_BASE_URL"] = os.environ.get(
    "STATBANK_BASE_URL",
    "https://test_fake_url/",
)
os.environ["STATBANK_ENCRYPT_URL"] = os.environ.get(
    "STATBANK_ENCRYPT_URL",
    "https://fake_url2/",
)
os.environ["DAPLA_SERVICE"] = os.environ.get("DAPLA_SERVICE", "FAKE")
os.environ["DAPLA_REGION"] = os.environ.get("DAPLA_REGION", "PYTEST")
