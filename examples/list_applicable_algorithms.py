from pathlib import Path

from ware_ops_algos.algorithms.algorithm_cards import load_packaged_algo_cards
from ware_ops_algos.domain_algo_mapper.domain_algo_mapper import DomainAlgorithmMapper
from ware_ops_algos.domain_models import load_and_flatten_data_card
from ware_ops_algos.taxonomy.taxonomy import TAXONOMY


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    data_card = load_and_flatten_data_card(ROOT / "data" / "data_cards" / "foodmart.yaml")
    algorithm_cards = load_packaged_algo_cards()
    applicable = DomainAlgorithmMapper(TAXONOMY).filter(
        algorithms=algorithm_cards,
        instance=data_card,
    )

    print(f"{data_card.name}: {len(applicable)} applicable algorithm configurations")
    for card in sorted(applicable, key=lambda item: item.algo_name):
        print(f"- {card.algo_name} ({card.problem_type})")


if __name__ == "__main__":
    main()
