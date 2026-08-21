from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import networkx as nx
import pandas as pd
from scipy.sparse.csgraph import floyd_warshall

from ware_ops_algos.domain_models import (
    Article,
    ArticleType,
    Articles,
    DimensionType,
    LayoutData,
    LayoutNetwork,
    LayoutParameters,
    LayoutType,
    Location,
    Order,
    OrderPosition,
    OrderType,
    OrdersDomain,
    PickCart,
    Resource,
    Resources,
    ResourceType,
    StorageLocations,
    StorageType,
    WarehouseInfo,
    WarehouseInfoType,
)
from ware_ops_algos.domain_models.base_domain import BaseWarehouseDomain
from ware_ops_algos.domain_models.layout.graph_generators import ShelfStorageGraphGenerator
from ware_ops_pipes.data_loaders.base import DataLoader


class HesslerIrnichLoader(DataLoader):
    def __init__(
        self,
        instances_dir: str | Path,
        cache_dir: str | Path | None = None,
        mirror_top_depot: bool = True,
    ):
        super().__init__(instances_dir)

        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.mirror_top_depot = mirror_top_depot

    def pipeline_loader_kwargs(self) -> dict:
        return {
            "mirror_top_depot": self.mirror_top_depot,
        }

    def load(
        self,
        filepath: str | Path,
        use_cache: bool = False,
    ) -> BaseWarehouseDomain:
        filepath = self._resolve_path(filepath)
        parsed = self.parse_instance(filepath)
        layout = self.build_layout(parsed)

        return self.build_domain_with_layout(parsed, layout)

    def parse_instance(self, filepath: str | Path) -> Dict[str, Any]:
        filepath = self._resolve_path(filepath)
        return self._parse(str(filepath))

    def layout_signature(self, parsed: Dict[str, Any]) -> dict:
        ctx = self._layout_context(parsed)

        return {
            "n_aisles": ctx["n_aisles"],
            "n_cells": ctx["n_cells"],
            "depot_aisle": ctx["depot_aisle"],
            "depot_location": ctx["depot_location"],
            "mirror_top_depot": self.mirror_top_depot,
            "mirrored": ctx["mirrored"],
            "dist_aisle": ctx["dist_aisle"],
            "dist_cell": ctx["dist_cell"],
            "dist_top_stub": ctx["dist_top_stub"],
            "dist_bottom_stub": ctx["dist_bottom_stub"],
            "dist_to_depot": ctx["dist_to_depot"],
            "start_location": ctx["start_location"],
            "end_location": ctx["end_location"],
            "start_connection_point": ctx["start_connection_point"],
            "end_connection_point": ctx["end_connection_point"],
        }

    def build_layout(self, parsed: Dict[str, Any]) -> LayoutData:
        ctx = self._layout_context(parsed)

        layout_parameters = LayoutParameters(
            n_aisles=ctx["n_aisles"],
            n_pick_locations=ctx["n_cells"],
            dist_pick_locations=ctx["dist_cell"],
            dist_aisle=ctx["dist_aisle"],
            dist_top_to_pick_location=ctx["dist_top_stub"],
            dist_bottom_to_pick_location=ctx["dist_bottom_stub"],
            dist_start=ctx["dist_to_depot"],
            dist_end=ctx["dist_to_depot"],
            start_location=ctx["start_location"],
            end_location=ctx["end_location"],
            start_connection_point=ctx["start_connection_point"],
            end_connection_point=ctx["end_connection_point"],
            n_blocks=1,
            depot_location=ctx["depot_location"],
        )

        min_aisle_position = 0
        max_aisle_position = layout_parameters.n_pick_locations + 1

        graph_generator = ShelfStorageGraphGenerator(
            n_aisles=layout_parameters.n_aisles,
            n_pick_locations=layout_parameters.n_pick_locations,
            dist_aisle=layout_parameters.dist_aisle,
            dist_pick_locations=layout_parameters.dist_pick_locations,
            dist_aisle_location=layout_parameters.dist_bottom_to_pick_location,
            start_location=layout_parameters.start_location,
            end_location=layout_parameters.end_location,
            start_connection_point=layout_parameters.start_connection_point,
            end_connection_point=layout_parameters.end_connection_point,
            dist_start=layout_parameters.dist_start,
            dist_end=layout_parameters.dist_end,
        )
        graph_generator.populate_graph()
        graph = graph_generator.G

        nodes = list(graph.nodes())
        adjacency = nx.to_scipy_sparse_array(
            graph,
            nodelist=nodes,
            weight="weight",
            dtype=float,
        )

        distance_matrix_raw, predecessors = floyd_warshall(
            adjacency,
            directed=False,
            return_predecessors=True,
        )

        distance_matrix = pd.DataFrame(
            distance_matrix_raw,
            index=nodes,
            columns=nodes,
        )

        layout_network = LayoutNetwork(
            graph=graph,
            distance_matrix=distance_matrix,
            predecessor_matrix=predecessors,
            closest_node_to_start=ctx["closest_node_to_start"],
            min_aisle_position=min_aisle_position,
            max_aisle_position=max_aisle_position,
            start_node=ctx["start_location"],
            end_node=ctx["end_location"],
            node_list=nodes,
        )

        return LayoutData(
            tpe=LayoutType.CONVENTIONAL,
            graph_data=layout_parameters,
            layout_network=layout_network,
        )

    def build_domain_with_layout(
        self,
        parsed: Dict[str, Any],
        layout: LayoutData,
    ) -> BaseWarehouseDomain:
        header = parsed["header"]
        ctx = self._layout_context(parsed)

        if "weight" in parsed["articles"][0]:
            article_list = [
                Article(
                    article_id=article["article_id"],
                    weight=article["weight"],
                )
                for article in parsed["articles"]
            ]
        else:
            article_list = [
                Article(article_id=article["article_id"])
                for article in parsed["articles"]
            ]

        articles = Articles(
            tpe=ArticleType.STANDARD,
            articles=article_list,
        )

        storage_type = (
            StorageType.DEDICATED
            if len(parsed["articles"]) == len(parsed["skus"])
            else StorageType.SCATTERED
        )

        # JOBPRP benchmark pick positions are assumed to hold enough units to
        # fulfil any feasible selection of orders.  In some converted files
        # the SKU quantity is nevertheless 1 while an order requests 2 or 3
        # units.  Represent the published assumption explicitly instead of
        # exposing that format field as a false stock constraint.
        total_demand = Counter()
        if storage_type == StorageType.DEDICATED:
            for order_positions in parsed["orders"]:
                for position in order_positions:
                    total_demand[position["article_id"]] += position["amount"]

        storage_raw = StorageLocations(
            tpe=storage_type,
            locations=[
                Location(
                    x=sku["aisle"] + 1,
                    y=sku["cell"],
                    article_id=sku["article_id"],
                    amount=(
                        max(
                            sku["quantity"],
                            total_demand[sku["article_id"]],
                        )
                        if storage_type == StorageType.DEDICATED
                        else sku["quantity"]
                    ),
                )
                for sku in parsed["skus"]
            ],
        )
        storage_raw.build_article_location_mapping()

        storage = (
            self._mirror_storage_locations(storage_raw, ctx["n_cells"])
            if ctx["mirrored"]
            else storage_raw
        )

        order_list = [
            Order(
                order_id=order_id,
                order_positions=[
                    OrderPosition(
                        order_number=order_id,
                        article_id=position["article_id"],
                        amount=position["amount"],
                    )
                    for position in order_positions
                ],
            )
            for order_id, order_positions in enumerate(parsed["orders"])
        ]

        orders = OrdersDomain(
            tpe=OrderType.STANDARD,
            orders=order_list,
        )

        if "PICKER_CAPACITY" in header:
            pick_cart = PickCart(
                n_dimension=1,
                n_boxes=1,
                capacities=[int(header["PICKER_CAPACITY"])],
                dimensions=[DimensionType.WEIGHT],
                box_can_mix_orders=True,
            )

            resources_list = [
                Resource(
                    id=0,
                    capacity=int(header["PICKER_CAPACITY"]),
                    speed=1.0,
                    pick_cart=pick_cart,
                )
            ]
        else:
            resources_list = [Resource(id=0)]

        resources = Resources(
            tpe=ResourceType.HUMAN,
            resources=resources_list,
        )

        warehouse_info = WarehouseInfo(
            tpe=WarehouseInfoType.OFFLINE,
        )

        return BaseWarehouseDomain(
            problem_class=self._problem_class(parsed),
            objective="Distance",
            layout=layout,
            articles=articles,
            orders=orders,
            resources=resources,
            storage=storage,
            warehouse_info=warehouse_info,
        )

    def _build(self, parsed: Dict[str, Any]) -> BaseWarehouseDomain:
        layout = self.build_layout(parsed)
        return self.build_domain_with_layout(parsed, layout)

    def _resolve_path(self, filepath: str | Path) -> Path:
        filepath = Path(filepath)

        if not filepath.is_absolute():
            filepath = self.data_dir / filepath

        return filepath

    def _layout_context(self, parsed: Dict[str, Any]) -> dict:
        header = parsed["header"]

        n_aisles = int(header["NUM_AISLES"])
        n_cells = int(header["NUM_CELLS"])
        depot_location = header["DEPOT_LOCATION"].lower()

        if depot_location not in {"top", "bottom"}:
            raise ValueError(
                f"DEPOT_LOCATION must be 'top' or 'bottom', got {depot_location!r}"
            )

        depot_aisle = int(header["DEPOT_AISLE"]) + 1

        dist_aisle = float(header["DISTANCE_AISLE_TO_AISLE"])
        dist_cell = float(header["DISTANCE_CELL_TO_CELL"])
        dist_top_stub = float(header["DISTANCE_TOP_TO_CELL"])
        dist_bottom_stub = float(header["DISTANCE_BOTTOM_TO_CELL"])
        dist_to_depot = float(header["DISTANCE_TOP_OR_BOTTOM_TO_DEPOT"])

        if depot_location == "bottom":
            start_location = (depot_aisle, -1)
            end_location = (depot_aisle - 1, -1)
            start_connection_point = (depot_aisle, 0)
            end_connection_point = (depot_aisle, 0)
            closest_node_to_start = (depot_aisle, 0)
        else:
            start_location = (depot_aisle, n_cells + 1)
            end_location = (depot_aisle - 1, n_cells + 1)
            start_connection_point = (depot_aisle, n_cells)
            end_connection_point = (depot_aisle, n_cells)
            closest_node_to_start = (depot_aisle, n_cells)

        mirrored = depot_location == "top" and self.mirror_top_depot

        if mirrored:
            dist_top_stub, dist_bottom_stub = dist_bottom_stub, dist_top_stub

            depot_location = "bottom"
            start_location = (depot_aisle, -1)
            end_location = (depot_aisle - 1, -1)
            start_connection_point = (depot_aisle, 0)
            end_connection_point = (depot_aisle, 0)
            closest_node_to_start = (depot_aisle, 0)

        return {
            "n_aisles": n_aisles,
            "n_cells": n_cells,
            "depot_aisle": depot_aisle,
            "depot_location": depot_location,
            "dist_aisle": dist_aisle,
            "dist_cell": dist_cell,
            "dist_top_stub": dist_top_stub,
            "dist_bottom_stub": dist_bottom_stub,
            "dist_to_depot": dist_to_depot,
            "start_location": start_location,
            "end_location": end_location,
            "start_connection_point": start_connection_point,
            "end_connection_point": end_connection_point,
            "closest_node_to_start": closest_node_to_start,
            "mirrored": mirrored,
        }

    @staticmethod
    def _problem_class(parsed: Dict[str, Any]) -> str:
        instance_type = parsed["header"].get("TYPE")

        if instance_type in {
            "Single_picker_routing",
            "Single_picker_routing_with_scattered_storage",
        }:
            return "SPRP"

        return "OBRP"

    def _parse(self, filepath: str) -> Dict[str, Any]:
        lines = self._load_text(filepath, encoding="windows-1252")

        header: Dict[str, str] = {}
        articles: List[Dict[str, int]] = []
        sku_entries: List[Dict[str, Any]] = []
        order_entries: List[List[Dict[str, int]]] = []

        idx = 0

        while idx < len(lines) and not lines[idx].startswith("ARTICLE_SECTION"):
            if ":" in lines[idx]:
                key, value = lines[idx].split(":", 1)
                header[key.strip().upper()] = value.strip()
            idx += 1

        required = [
            "NUM_AISLES",
            "NUM_CELLS",
            "DEPOT_AISLE",
            "DEPOT_LOCATION",
            "DISTANCE_AISLE_TO_AISLE",
            "DISTANCE_CELL_TO_CELL",
            "DISTANCE_TOP_TO_CELL",
            "DISTANCE_BOTTOM_TO_CELL",
            "DISTANCE_TOP_OR_BOTTOM_TO_DEPOT",
        ]

        missing = [key for key in required if key not in header]

        if missing:
            raise ValueError(f"Missing required header keys: {missing}")

        idx += 1

        while idx < len(lines) and not lines[idx].startswith("SKU_SECTION"):
            if lines[idx].startswith("ID"):
                parts = lines[idx].split()

                if len(parts) == 2:
                    articles.append(
                        {
                            "article_id": int(parts[1]),
                        }
                    )
                else:
                    articles.append(
                        {
                            "article_id": int(parts[1]),
                            "weight": int(parts[3]),
                        }
                    )

            idx += 1

        idx += 1

        n_cells = int(header["NUM_CELLS"])

        while idx < len(lines) and not lines[idx].startswith("ORDER_SECTION"):
            if lines[idx].startswith("ID"):
                parts = lines[idx].split()

                sku_entries.append(
                    {
                        "article_id": int(parts[1]),
                        "aisle": int(parts[3]),
                        "cell": n_cells - int(parts[5]),
                        "quantity": int(parts[7]),
                        "side": parts[-1],
                    }
                )

            idx += 1

        idx += 1

        current_order: List[Dict[str, int]] = []

        while idx < len(lines):
            line = lines[idx]

            if line.startswith("NUM_ARTICLES_IN_ORDER"):
                if current_order:
                    order_entries.append(current_order)
                    current_order = []

            elif line.startswith("ID"):
                parts = line.split()

                current_order.append(
                    {
                        "article_id": int(parts[1]),
                        "amount": int(parts[3]),
                    }
                )

            idx += 1

        if current_order:
            order_entries.append(current_order)

        return {
            "header": header,
            "articles": articles,
            "skus": sku_entries,
            "orders": order_entries,
        }

    @staticmethod
    def _mirror_y(y: int, n_cells: int) -> int:
        if y == 0:
            return n_cells + 1

        if y == n_cells + 1:
            return 0

        return (n_cells + 1) - y

    def _mirror_storage_locations(
        self,
        storage: StorageLocations,
        n_cells: int,
    ) -> StorageLocations:
        mirrored = StorageLocations(
            tpe=storage.tpe,
            locations=[
                Location(
                    x=location.x,
                    y=self._mirror_y(location.y, n_cells),
                    article_id=location.article_id,
                    amount=location.amount,
                )
                for location in storage.locations
            ],
        )
        mirrored.build_article_location_mapping()

        return mirrored
