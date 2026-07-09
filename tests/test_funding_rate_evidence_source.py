import pytest

from research_core import Evidence, EvidenceSource, FundingRateEvidenceSource, RequiredFieldError


class FakeFundingRateProvider:
    def __init__(self, observations):
        self.observations = tuple(observations)
        self.calls = []

    def fetch_funding_rates(self, context):
        self.calls.append(context)
        return self.observations


class MissingFetchProvider:
    pass


def test_funding_rate_evidence_source_implements_evidence_source_protocol():
    provider = FakeFundingRateProvider([])
    source = FundingRateEvidenceSource("funding_rate_source", provider)

    assert isinstance(source, EvidenceSource)
    assert source.source_id == "funding_rate_source"


def test_funding_rate_evidence_source_forwards_context_to_injected_provider():
    provider = FakeFundingRateProvider([])
    source = FundingRateEvidenceSource("funding_rate_source", provider)
    context = {"symbol": "BTCUSDT", "timeframe": "8h"}

    produced = tuple(source.produce_evidence(context))

    assert produced == ()
    assert provider.calls == [context]


def test_funding_rate_observations_are_converted_to_evidence_objects():
    provider = FakeFundingRateProvider(
        [
            {
                "id": "obs_funding_btc_1",
                "symbol": "BTCUSDT",
                "timestamp": "2026-07-09T00:00:00Z",
                "funding_rate": "0.005",
            },
            {
                "observation_id": "obs_funding_eth_1",
                "symbol": "ETHUSDT",
                "timestamp": "2026-07-09T00:00:00Z",
                "rate": "-0.02",
                "source_quality": "exchange_adapter_snapshot",
                "reproducibility": "recorded_payload",
            },
        ]
    )
    source = FundingRateEvidenceSource("funding_rate_source", provider, extreme_rate="0.01")

    produced = tuple(source.produce_evidence({"market": "perpetuals"}))

    assert len(produced) == 2
    assert all(isinstance(evidence, Evidence) for evidence in produced)
    assert produced[0].observation_ids == ("obs_funding_btc_1",)
    assert produced[0].confidence == 0.5
    assert produced[0].source_quality == "market_data_provider"
    assert produced[0].reproducibility == "provider_snapshot"
    assert produced[1].observation_ids == ("obs_funding_eth_1",)
    assert produced[1].confidence == 1.0
    assert produced[1].source_quality == "exchange_adapter_snapshot"
    assert produced[1].reproducibility == "recorded_payload"
    assert produced[0].id.startswith("ev_funding_rate_")
    assert produced[1].id.startswith("ev_funding_rate_")
    assert produced[0].id != produced[1].id


def test_funding_rate_evidence_ids_are_deterministic_for_same_observation():
    observation = {"id": "obs_funding_btc_1", "funding_rate": "0.005"}
    left = FundingRateEvidenceSource("funding_rate_source", FakeFundingRateProvider([observation]))
    right = FundingRateEvidenceSource("funding_rate_source", FakeFundingRateProvider([observation]))

    assert tuple(left.produce_evidence({})) == tuple(right.produce_evidence({}))


def test_funding_rate_evidence_source_rejects_invalid_configuration_and_observations():
    with pytest.raises(RequiredFieldError):
        FundingRateEvidenceSource("", FakeFundingRateProvider([]))
    with pytest.raises(RequiredFieldError):
        FundingRateEvidenceSource("funding_rate_source", MissingFetchProvider())
    with pytest.raises(RequiredFieldError):
        FundingRateEvidenceSource("funding_rate_source", FakeFundingRateProvider([]), extreme_rate="0")

    missing_id_source = FundingRateEvidenceSource("funding_rate_source", FakeFundingRateProvider([{"funding_rate": "0.001"}]))
    with pytest.raises(RequiredFieldError):
        tuple(missing_id_source.produce_evidence({}))

    missing_rate_source = FundingRateEvidenceSource("funding_rate_source", FakeFundingRateProvider([{"id": "obs_1"}]))
    with pytest.raises(RequiredFieldError):
        tuple(missing_rate_source.produce_evidence({}))
