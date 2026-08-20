from pathlib import Path

from ware_ops_algos.algorithms.algorithm_cards import load_packaged_algo_cards
from ware_ops_algos.domain_models import load_and_flatten_data_card

from ware_ops_pipes.pipelines.io_helpers import (
    dump_json,
    dump_pickle,
    load_json,
    load_pickle,
)
from ware_ops_pipes.pipelines.subproblems.batching.generated.index import (
    CONFIGURED_COMPONENT_MODULES,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_data_cards_are_readable_without_instance_loaders():
    cards = sorted((PROJECT_ROOT / "data" / "data_cards").glob("*.yaml"))

    assert cards
    for card_path in cards:
        card = load_and_flatten_data_card(card_path)
        assert card.name
        assert card.problem_class
        assert card.layout["features"]
        assert card.orders["features"]


def test_algorithm_catalogue_and_configured_components_are_available():
    cards = load_packaged_algo_cards()

    assert len(cards) >= 10
    assert len({card.algo_name for card in cards}) == len(cards)
    assert "LSBatchingNNFiFo" in CONFIGURED_COMPONENT_MODULES


def test_pipeline_io_helpers_round_trip(tmp_path):
    payload = {"instance": "tiny", "distance": 18.0}
    json_path = tmp_path / "result.json"
    pickle_path = tmp_path / "result.pkl"

    dump_json(json_path, payload)
    dump_pickle(pickle_path, payload)

    assert load_json(json_path) == payload
    assert load_pickle(pickle_path) == payload
