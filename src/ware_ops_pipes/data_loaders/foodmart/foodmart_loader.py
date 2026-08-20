from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import networkx as nx
from scipy.sparse.csgraph import floyd_warshall

from ware_ops_algos.domain_models.layout.graph_generators import (
    ExplicitGraphGenerator,
    distance_matrix_generator_from_shortest_paths,
)
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
from ware_ops_pipes.data_loaders.base import DataLoader


class FoodmartLoader(DataLoader):
    def __init__(
        self,
        instances_dir: str | Path,
        cache_dir: str | Path | None = None,
    ):
        super().__init__(instances_dir)

        # Kept for backwards-compatible construction. Caching is handled by Luigi.
        self.cache_dir = Path(cache_dir) if cache_dir else None

    def pipeline_loader_kwargs(self) -> dict:
        return {}

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
        header = parsed["header"]

        return {
            "departing_depot": header["DepartingDepot"],
            "arrival_depot": header["ArrivalDepot"],
            "nb_vertices_intersections": header["NbVerticesIntersections"],
            "arcs": sorted(
                (int(start), int(end), float(distance))
                for start, end, distance in parsed["arcs"]
            ),
            "shortest_paths": sorted(
                (int(start), int(end), float(distance))
                for (start, end), distance in parsed["shortest_paths"].items()
            ),
            "vertices_coords": sorted(
                (int(idx), float(x), float(y), str(node_type))
                for idx, (x, y, node_type) in parsed["vertices_coords"].items()
            ),
        }

    def build_layout(self, parsed: Dict[str, Any]) -> LayoutData:
        header = parsed["header"]
        arcs = parsed["arcs"]
        shortest_paths = parsed["shortest_paths"]
        vertices_coords = parsed["vertices_coords"]

        graph_generator = ExplicitGraphGenerator(vertices_coords, arcs)
        graph_generator.populate_graph()
        graph = graph_generator.G

        depot_idx = header["DepartingDepot"]
        end_idx = header["ArrivalDepot"]

        start_node = vertices_coords[depot_idx][:2]
        end_node = vertices_coords[end_idx][:2]

        shortest_paths_coords = {}
        for (start_idx, end_idx), distance in shortest_paths.items():
            x_start, y_start = vertices_coords[start_idx][:2]
            x_end, y_end = vertices_coords[end_idx][:2]

            shortest_paths_coords[
                ((x_start, y_start), (x_end, y_end))
            ] = distance

        distance_matrix = distance_matrix_generator_from_shortest_paths(
            graph,
            shortest_paths_coords,
        )

        nodes = list(graph.nodes())
        adjacency = nx.to_scipy_sparse_array(
            graph,
            nodelist=nodes,
            weight="weight",
            dtype=float,
        )

        _, predecessors = floyd_warshall(
            adjacency,
            directed=False,
            return_predecessors=True,
        )

        intersection_nodes = [
            (x, y)
            for x, y, node_type in vertices_coords.values()
            if node_type == "intersection"
        ]

        min_aisle_position = (
            min(y for _, y in intersection_nodes)
            if intersection_nodes
            else 0
        )
        max_aisle_position = max(y for _, y, _ in vertices_coords.values())
        n_aisles = int(max(x for x, _, _ in vertices_coords.values()))

        closest_node_to_start = (
            distance_matrix[start_node]
            .drop(labels=[start_node, end_node])
            .idxmin()
        )

        layout_parameters = LayoutParameters(
            n_aisles=n_aisles,
            n_pick_locations=max_aisle_position,
            dist_top_to_pick_location=0,
            dist_bottom_to_pick_location=0,
            dist_start=0,
            dist_end=0,
            dist_pick_locations=0,
            dist_aisle=0,
            n_blocks=2,
            start_location=start_node,
            end_location=end_node,
        )

        layout_network = LayoutNetwork(
            graph=graph,
            distance_matrix=distance_matrix,
            predecessor_matrix=predecessors,
            closest_node_to_start=closest_node_to_start,
            min_aisle_position=min_aisle_position,
            max_aisle_position=max_aisle_position,
            start_node=start_node,
            end_node=end_node,
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

        storage = StorageLocations(
            tpe=StorageType.DEDICATED,
            locations=parsed["locations"],
        )
        storage.build_article_location_mapping()

        articles = Articles(
            tpe=ArticleType.STANDARD,
            articles=parsed["articles"],
        )

        orders = OrdersDomain(
            tpe=OrderType.STANDARD,
            orders=parsed["orders"],
        )

        capacity = header["B_CapaBox"] * header["K_NbBoxesTrolley"]

        pick_cart = PickCart(
            n_dimension=1,
            capacities=[header["B_CapaBox"]],
            dimensions=[DimensionType.ITEMS],
            n_boxes=header["K_NbBoxesTrolley"],
            box_can_mix_orders=False,
        )

        resources = Resources(
            tpe=ResourceType.HUMAN,
            resources=[
                Resource(
                    id=0,
                    capacity=capacity,
                    speed=1.0,
                    pick_cart=pick_cart,
                )
            ],
        )

        warehouse_info = WarehouseInfo(
            tpe=WarehouseInfoType.OFFLINE,
        )

        return BaseWarehouseDomain(
            problem_class="OBRP",
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

    def _parse(self, filepath: str) -> Dict[str, Any]:
        lines = self._load_text(filepath, encoding="utf-8")

        line_idx = 0
        header = {}
        articles = []
        sku_entries = []
        order_entries = []
        arcs = []
        locations = []
        shortest_paths = {}
        vertices_coords = {}

        departing_depot = None
        arrival_depot = None

        def next_line():
            nonlocal line_idx

            if line_idx < len(lines):
                line = lines[line_idx]
                line_idx += 1
                return line

            return None

        def peek_line():
            return lines[line_idx] if line_idx < len(lines) else None

        def skip_to_prefix(prefix: str):
            nonlocal line_idx

            while line_idx < len(lines) and not lines[line_idx].startswith(prefix):
                line_idx += 1

            if line_idx < len(lines):
                return next_line()

            return None

        def skip_to_any_prefix(prefixes: list[str]):
            nonlocal line_idx

            while line_idx < len(lines) and not any(
                lines[line_idx].startswith(prefix)
                for prefix in prefixes
            ):
                line_idx += 1

            if line_idx < len(lines):
                return next_line()

            return None

        def next_data_line():
            line = next_line()

            while line is not None and line.startswith("//"):
                line = next_line()

            return line

        skip_to_prefix("//NbLocations")
        header["NbLocations"] = int(next_data_line())
        header["NbProducts"] = int(next_data_line())
        header["K_NbBoxesTrolley"] = int(next_data_line().split()[0])
        header["NbDimensionsCapacity"] = int(next_data_line().split()[0])
        header["B_CapaBox"] = int(next_data_line().split()[0])
        header["BoxCanMixOrders"] = int(next_data_line().split()[0])

        skip_to_prefix("//Products")

        for _ in range(header["NbProducts"]):
            parts = next_data_line().split()

            article_id = int(parts[0])
            location = int(parts[1])
            volume = float(parts[2])

            articles.append(
                Article(
                    article_id=article_id,
                    volume=volume,
                )
            )
            sku_entries.append((article_id, location))

        skip_to_prefix("//Orders")
        skip_to_any_prefix(["//Nb Orders", "//NbOrders"])

        header["NbOrders"] = int(next_data_line())

        for _ in range(header["NbOrders"]):
            parts = next_data_line().split()

            order_number = int(parts[0])
            nb_products_in_order = int(parts[2])

            idx = 3
            positions = []

            for _ in range(nb_products_in_order):
                article_id = int(parts[idx])
                amount = int(parts[idx + 1])

                positions.append(
                    {
                        "article_id": article_id,
                        "amount": amount,
                    }
                )
                idx += 2

            order_entries.append(
                Order.from_dict(
                    order_number,
                    {
                        "order_positions": positions,
                    },
                )
            )

        skip_to_prefix("//Graph")

        header["NbVerticesIntersections"] = int(next_data_line())
        header["DepartingDepot"] = int(next_data_line())
        header["ArrivalDepot"] = int(next_data_line())

        departing_depot = header["DepartingDepot"]
        arrival_depot = header["ArrivalDepot"]

        skip_to_prefix("//Arcs")
        next_line()

        while True:
            line = peek_line()

            if line is None or line.startswith("//LocStart"):
                break

            parts = next_line().split()

            if len(parts) >= 3:
                arcs.append(
                    (
                        int(parts[0]),
                        int(parts[1]),
                        float(parts[2]),
                    )
                )

        skip_to_prefix("//LocStart")

        while True:
            line = peek_line()

            if line is None or line.startswith("//Vertices"):
                break

            parts = next_line().split()

            if len(parts) >= 3:
                shortest_paths[(int(parts[0]), int(parts[1]))] = float(parts[2])

        skip_to_prefix("//Vertices")

        while (line := next_line()) is not None:
            if line.startswith("//"):
                continue

            parts = line.split()

            try:
                idx = int(parts[0])
                x = float(parts[1])
                y = float(parts[2])
                label = parts[3].strip('"')

                match label:
                    case "depot":
                        if idx == departing_depot:
                            node_type = "start_node"
                        elif idx == arrival_depot:
                            node_type = "end_node"
                            x += 1
                        else:
                            node_type = "depot_node"

                    case "product":
                        node_type = "pick_node"

                    case "intersection":
                        node_type = "intersection"

                    case _:
                        raise ValueError(f"Unknown node type: {label}")

                vertices_coords[idx] = (x, y, node_type)

            except (ValueError, IndexError) as exc:
                print(f"Warning: Error parsing vertex line: {line} — {exc}")

        for article_id, location_id in sku_entries:
            x, y, _ = vertices_coords.get(location_id, (0, 0, ""))

            locations.append(
                Location(
                    x=x,
                    y=y,
                    article_id=article_id,
                    amount=1000,
                )
            )

        return {
            "header": header,
            "articles": articles,
            "locations": locations,
            "orders": order_entries,
            "arcs": arcs,
            "shortest_paths": shortest_paths,
            "vertices_coords": vertices_coords,
        }
