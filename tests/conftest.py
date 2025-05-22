import pytest
from _pytest.fixtures import FixtureRequest
from _pytest.monkeypatch import MonkeyPatch

# The package uses mainly requests.get and .post, we dont want it to make ACTUAL requests during testing.
# This fixture blocks them in "the first layer", and therefore forces tests that have not patched these out correctly to fail.
# If running the jobs for "integration_dapla", this might need to be made conditional


@pytest.fixture(autouse=True)
def block_requests(request: FixtureRequest, monkeypatch: MonkeyPatch) -> None:
    if request.node.get_closest_marker("integration_dapla"):
        # Test is marked with @pytest.mark.integration_dapla, allow real HTTP when integration testing
        return

    def fake_request():
        err = "Attempted to make external HTTP request during testing!"
        raise RuntimeError(err)

    monkeypatch.setattr("requests.get", fake_request)
    monkeypatch.setattr("requests.post", fake_request)
