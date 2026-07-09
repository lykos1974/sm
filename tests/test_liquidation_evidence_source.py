import pytest

from research_core import Evidence, EvidenceSource, LiquidationEvidenceSource, RequiredFieldError


class FakeLiquidationProvider:
    def __init__(self, observations):
        self.observations = tuple(observations)
        self.calls = []

    def fetch_liquidations(self, context):
        self.calls.append(context)
        return self.observations


class MissingFetchProvider:
    pass


def test_liquidation_evidence_source_implements_evidence_source_protocol():
    provider = FakeLiquidationProvider([])
    source = LiquidationEvidenceSource("liquidation_source", provider)

    assert isinstance(source, EvidenceSource)
    assert source.source_id == "liquidation_source"


def test_liquidation_evidence_source_forwards_context_to_injected_provider():
    provider = FakeLiquidationProvider([])
    source = LiquidationEvidenceSource("liquidation_source", provider)
    context = {"symbol": "BTCUSDT", "timeframe": "1m"}

    produced = tuple(source.produce_evidence(context))

    assert produced == ()
    assert provider.calls == [context]


def test_liquidation_observations_are_converted_to_evidence_objects():
    provider = FakeLiquidationProvider(
        [
            {
                "id": "obs_liq_btc_1",
                "symbol": "BTCUSDT",
                "exchange": "binance",
                "timestamp": "2026-07-09T00:00:00Z",
                "side": "long",
                "size": "50000",
                "price": "60000.5",
            },
            {
                "observation_id": "obs_liq_eth_1",
                "symbol": "ETHUSDT",
                "exchange": "mexc",
                "timestamp": "2026-07-09T00:01:00Z",
                "side": "SHORT",
                "quantity": "250000",
                "price": "3500",
                "source_quality": "exchange_adapter_snapshot",
                "reproducibility": "recorded_payload",
            },
        ]
    )
    source = LiquidationEvidenceSource("liquidation_source", provider, large_size="100000")

    produced = tuple(source.produce_evidence({"market": "perpetuals"}))

    assert len(produced) == 2
    assert all(isinstance(evidence, Evidence) for evidence in produced)
    assert produced[0].observation_ids == ("obs_liq_btc_1",)
    assert produced[0].confidence == 0.5
    assert produced[0].source_quality == "market_data_provider"
    assert produced[0].reproducibility == "provider_snapshot"
    assert produced[1].observation_ids == ("obs_liq_eth_1",)
    assert produced[1].confidence == 1.0
    assert produced[1].source_quality == "exchange_adapter_snapshot"
    assert produced[1].reproducibility == "recorded_payload"
    assert produced[0].id.startswith("ev_liquidation_")
    assert produced[1].id.startswith("ev_liquidation_")
    assert produced[0].id != produced[1].id


def test_liquidation_evidence_ids_are_deterministic_and_preserve_metadata_in_identity():
    observation = {
        "id": "obs_liq_btc_1",
        "symbol": "BTCUSDT",
        "exchange": "binance",
        "timestamp": "2026-07-09T00:00:00Z",
        "side": "long",
        "size": "50000",
        "price": "60000.5",
    }
    left = LiquidationEvidenceSource("liquidation_source", FakeLiquidationProvider([observation]))
    right = LiquidationEvidenceSource("liquidation_source", FakeLiquidationProvider([dict(observation)]))
    changed_timestamp = dict(observation, timestamp="2026-07-09T00:01:00Z")
    changed_exchange = dict(observation, exchange="mexc")

    assert tuple(left.produce_evidence({})) == tuple(right.produce_evidence({}))
    assert tuple(left.produce_evidence({})) != tuple(
        LiquidationEvidenceSource("liquidation_source", FakeLiquidationProvider([changed_timestamp])).produce_evidence({})
    )
    assert tuple(left.produce_evidence({})) != tuple(
        LiquidationEvidenceSource("liquidation_source", FakeLiquidationProvider([changed_exchange])).produce_evidence({})
    )


def test_liquidation_quantity_alias_is_supported():
    provider = FakeLiquidationProvider(
        [
            {
                "id": "obs_liq_btc_1",
                "symbol": "BTCUSDT",
                "exchange": "binance",
                "timestamp": "2026-07-09T00:00:00Z",
                "side": "short",
                "qty": "0",
                "price": "60000",
            }
        ]
    )
    source = LiquidationEvidenceSource("liquidation_source", provider)

    produced = tuple(source.produce_evidence({}))

    assert produced[0].confidence == 0.0
    assert produced[0].id.startswith("ev_liquidation_")


def test_liquidation_evidence_source_rejects_invalid_configuration_and_observations():
    with pytest.raises(RequiredFieldError):
        LiquidationEvidenceSource("", FakeLiquidationProvider([]))
    with pytest.raises(RequiredFieldError):
        LiquidationEvidenceSource("liquidation_source", MissingFetchProvider())
    with pytest.raises(RequiredFieldError):
        LiquidationEvidenceSource("liquidation_source", FakeLiquidationProvider([]), large_size="0")

    required_observation = {
        "id": "obs_1",
        "symbol": "BTCUSDT",
        "exchange": "binance",
        "timestamp": "2026-07-09T00:00:00Z",
        "side": "long",
        "size": "50000",
        "price": "60000",
    }
    for missing_field in ("id", "symbol", "exchange", "timestamp", "side", "size", "price"):
        observation = dict(required_observation)
        observation.pop(missing_field)
        source = LiquidationEvidenceSource("liquidation_source", FakeLiquidationProvider([observation]))
        with pytest.raises(RequiredFieldError):
            tuple(source.produce_evidence({}))

    invalid_side_source = LiquidationEvidenceSource(
        "liquidation_source", FakeLiquidationProvider([dict(required_observation, side="buy")])
    )
    with pytest.raises(RequiredFieldError):
        tuple(invalid_side_source.produce_evidence({}))
