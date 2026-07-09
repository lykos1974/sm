import pytest

from research_core import Evidence, EvidenceSource, OpenInterestEvidenceSource, RequiredFieldError


class FakeOpenInterestProvider:
    def __init__(self, observations):
        self.observations = tuple(observations)
        self.calls = []

    def fetch_open_interest(self, context):
        self.calls.append(context)
        return self.observations


class MissingFetchProvider:
    pass


def test_open_interest_evidence_source_implements_evidence_source_protocol():
    provider = FakeOpenInterestProvider([])
    source = OpenInterestEvidenceSource("open_interest_source", provider)

    assert isinstance(source, EvidenceSource)
    assert source.source_id == "open_interest_source"


def test_open_interest_evidence_source_forwards_context_to_injected_provider():
    provider = FakeOpenInterestProvider([])
    source = OpenInterestEvidenceSource("open_interest_source", provider)
    context = {"symbol": "BTCUSDT", "timeframe": "1h"}

    produced = tuple(source.produce_evidence(context))

    assert produced == ()
    assert provider.calls == [context]


def test_open_interest_observations_are_converted_to_evidence_objects():
    provider = FakeOpenInterestProvider(
        [
            {
                "id": "obs_oi_btc_1",
                "symbol": "BTCUSDT",
                "timestamp": "2026-07-09T00:00:00Z",
                "open_interest_change": "0.05",
            },
            {
                "observation_id": "obs_oi_eth_1",
                "symbol": "ETHUSDT",
                "timestamp": "2026-07-09T00:00:00Z",
                "change": "-0.20",
                "source_quality": "exchange_adapter_snapshot",
                "reproducibility": "recorded_payload",
            },
        ]
    )
    source = OpenInterestEvidenceSource("open_interest_source", provider, extreme_change="0.10")

    produced = tuple(source.produce_evidence({"market": "perpetuals"}))

    assert len(produced) == 2
    assert all(isinstance(evidence, Evidence) for evidence in produced)
    assert produced[0].observation_ids == ("obs_oi_btc_1",)
    assert produced[0].confidence == 0.5
    assert produced[0].source_quality == "market_data_provider"
    assert produced[0].reproducibility == "provider_snapshot"
    assert produced[1].observation_ids == ("obs_oi_eth_1",)
    assert produced[1].confidence == 1.0
    assert produced[1].source_quality == "exchange_adapter_snapshot"
    assert produced[1].reproducibility == "recorded_payload"
    assert produced[0].id.startswith("ev_open_interest_")
    assert produced[1].id.startswith("ev_open_interest_")
    assert produced[0].id != produced[1].id


def test_open_interest_evidence_ids_are_deterministic_for_same_observation():
    observation = {"id": "obs_oi_btc_1", "open_interest_change": "0.05"}
    left = OpenInterestEvidenceSource("open_interest_source", FakeOpenInterestProvider([observation]))
    right = OpenInterestEvidenceSource("open_interest_source", FakeOpenInterestProvider([observation]))

    assert tuple(left.produce_evidence({})) == tuple(right.produce_evidence({}))


def test_open_interest_delta_alias_is_supported():
    provider = FakeOpenInterestProvider([{"id": "obs_oi_btc_1", "open_interest_delta": "0"}])
    source = OpenInterestEvidenceSource("open_interest_source", provider)

    produced = tuple(source.produce_evidence({}))

    assert produced[0].confidence == 0.0
    assert produced[0].id.startswith("ev_open_interest_")


def test_open_interest_evidence_source_rejects_invalid_configuration_and_observations():
    with pytest.raises(RequiredFieldError):
        OpenInterestEvidenceSource("", FakeOpenInterestProvider([]))
    with pytest.raises(RequiredFieldError):
        OpenInterestEvidenceSource("open_interest_source", MissingFetchProvider())
    with pytest.raises(RequiredFieldError):
        OpenInterestEvidenceSource("open_interest_source", FakeOpenInterestProvider([]), extreme_change="0")

    missing_id_source = OpenInterestEvidenceSource("open_interest_source", FakeOpenInterestProvider([{"open_interest_change": "0.01"}]))
    with pytest.raises(RequiredFieldError):
        tuple(missing_id_source.produce_evidence({}))

    missing_change_source = OpenInterestEvidenceSource("open_interest_source", FakeOpenInterestProvider([{"id": "obs_1"}]))
    with pytest.raises(RequiredFieldError):
        tuple(missing_change_source.produce_evidence({}))
