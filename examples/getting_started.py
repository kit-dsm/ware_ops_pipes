"""Inspect a tiny warehouse domain and compare two visit sequences.

The example deliberately avoids benchmark files. It shows the values that
flow from a warehouse description into algorithm-card filtering and then into
a concrete route-distance comparison.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

from ware_ops_algos.algorithms import GreedyItemAssignment
from ware_ops_algos.algorithms.algorithm_cards import load_packaged_algo_cards
from ware_ops_algos.domain_algo_mapper.domain_algo_mapper import DomainAlgorithmMapper
from ware_ops_algos.domain_models import (
    Article,
    ArticleType,
    Articles,
    BaseWarehouseDomain,
    LayoutData,
    LayoutNetwork,
    LayoutParameters,
    LayoutType,
    Location,
    Order,
    OrderPosition,
    OrderType,
    OrdersDomain,
    Resource,
    Resources,
    ResourceType,
    StorageLocations,
    StorageType,
    WarehouseInfo,
    WarehouseInfoType,
)
from ware_ops_algos.taxonomy.taxonomy import TAXONOMY


def manhattan(first: tuple[int, int], second: tuple[int, int]) -> int:
    return abs(first[0] - second[0]) + abs(first[1] - second[1])


def route_distance(route: list[tuple[int, int]]) -> int:
    return sum(manhattan(first, second) for first, second in zip(route, route[1:]))


def build_demo_domain() -> tuple[BaseWarehouseDomain, tuple[int, int]]:
    depot = (0, 0)
    locations = [
        Location(x=1, y=2, article_id=101, amount=4),
        Location(x=1, y=6, article_id=101, amount=3),
        Location(x=2, y=4, article_id=102, amount=2),
        Location(x=2, y=7, article_id=103, amount=5),
    ]
    storage = StorageLocations(StorageType.DEDICATED, locations=locations)
    storage.build_article_location_mapping()

    orders = OrdersDomain(
        OrderType.STANDARD,
        orders=[
            Order(
                order_id=1,
                order_positions=[
                    OrderPosition(order_number=1, article_id=103, amount=1),
                    OrderPosition(order_number=1, article_id=101, amount=2),
                    OrderPosition(order_number=1, article_id=102, amount=1),
                ],
            )
        ],
    )
    articles = Articles(
        ArticleType.STANDARD,
        articles=[
            Article(article_id=101, weight=1.2, volume=0.5),
            Article(article_id=102, weight=0.8, volume=0.3),
            Article(article_id=103, weight=2.0, volume=0.7),
        ],
    )
    resources = Resources(
        ResourceType.HUMAN,
        resources=[Resource(id=1, speed=1.0, time_per_pick=0.5)],
    )

    nodes = [depot, (1, 2), (1, 6), (2, 4), (2, 7)]
    graph = nx.Graph()
    graph.add_nodes_from(nodes)
    distance_matrix = pd.DataFrame(
        [[manhattan(first, second) for second in nodes] for first in nodes],
        index=nodes,
        columns=nodes,
    )
    layout = LayoutData(
        tpe=LayoutType.CONVENTIONAL,
        graph_data=LayoutParameters(
            n_aisles=2,
            n_pick_locations=7,
            n_blocks=1,
            dist_top_to_pick_location=1,
            dist_bottom_to_pick_location=1,
            dist_pick_locations=1,
            dist_aisle=1,
            dist_start=0,
            start_location=depot,
            end_location=depot,
            dist_end=0,
        ),
        layout_network=LayoutNetwork(
            graph=graph,
            distance_matrix=distance_matrix,
            start_node=depot,
            end_node=depot,
            closest_node_to_start=(1, 2),
            min_aisle_position=1,
            max_aisle_position=7,
            node_list=nodes,
        ),
    )
    domain = BaseWarehouseDomain(
        problem_class="OBRP",
        objective="distance",
        layout=layout,
        articles=articles,
        orders=orders,
        resources=resources,
        storage=storage,
        warehouse_info=WarehouseInfo(WarehouseInfoType.OFFLINE),
    )
    return domain, depot


def make_route_plot(
    input_route: list[tuple[int, int]],
    sorted_route: list[tuple[int, int]],
    input_distance: int,
    sorted_distance: int,
    output: Path,
) -> None:
    depot = input_route[0]
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharex=True, sharey=True)
    for axis, route, title, distance in [
        (axes[0], input_route, "Order as entered", input_distance),
        (axes[1], sorted_route, "Distance-sorted illustration", sorted_distance),
    ]:
        x_values, y_values = zip(*route)
        axis.plot(x_values, y_values, "-o", color="#007c91")
        axis.scatter(*depot, color="#d1495b", s=90, label="depot", zorder=3)
        for step, node in enumerate(route[1:-1], start=1):
            axis.annotate(str(step), node, xytext=(5, 5), textcoords="offset points")
        axis.set_title(f"{title}\n{distance} distance units")
        axis.set_xlabel("aisle")
        axis.grid(alpha=0.25)
    axes[0].set_ylabel("position")
    fig.suptitle("One order, two visit sequences")
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, format="svg")
    plt.close(fig)


def main(output: Path) -> None:
    warehouse, depot = build_demo_domain()
    orders = warehouse.orders.orders
    storage = warehouse.storage
    cards = load_packaged_algo_cards()
    applicable = DomainAlgorithmMapper(TAXONOMY).filter(cards, warehouse)

    print(
        f"{len(orders)} order, {len(orders[0].order_positions)} order lines, "
        f"{len(storage.locations)} storage locations"
    )
    print(
        "Order demand:",
        [(position.article_id, position.amount) for position in orders[0].order_positions],
    )
    print(f"Applicable algorithms: {len(applicable)} of {len(cards)}")

    assignment = GreedyItemAssignment(storage).solve(orders)
    pick_nodes = [position.pick_node for position in assignment.resolved_orders[0].pick_positions]
    input_route = [depot, *pick_nodes, depot]
    sorted_route = [depot, *sorted(pick_nodes, key=lambda node: manhattan(depot, node)), depot]
    input_distance = route_distance(input_route)
    sorted_distance = route_distance(sorted_route)
    print(f"Assigned pick nodes: {pick_nodes}")
    print(f"Input order distance: {input_distance}")
    print(f"Distance-sorted illustration: {sorted_distance}")
    print(f"Difference: {input_distance - sorted_distance}")

    make_route_plot(input_route, sorted_route, input_distance, sorted_distance, output)
    print(f"Route plot: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="SVG path for the route comparison")
    main(parser.parse_args().output)
