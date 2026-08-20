from __future__ import annotations

from pathlib import Path
from typing import Any

import networkx as nx
from scipy.sparse.csgraph import floyd_warshall

from ware_ops_algos.domain_models import (
    Article,
    ArticleType,
    Articles,
    BaseWarehouseDomain,
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
from ware_ops_algos.domain_models.layout.graph_generators import (
    ExplicitGraphGenerator,
    distance_matrix_generator_from_shortest_paths,
)
from ware_ops_pipes.data_loaders.base import DataLoader


class IBRSPLoader(DataLoader):
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

    def parse_instance(self, filepath: str | Path) -> dict[str, Any]:
        filepath = self._resolve_path(filepath)
        return self._parse(str(filepath))

    def layout_signature(self, parsed: dict[str, Any]) -> dict:
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

    def build_layout(self, parsed: dict[str, Any]) -> LayoutData:
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

        pick_nodes = [
            (x, y)
            for x, y, node_type in vertices_coords.values()
            if node_type == "pick_node"
        ]

        min_aisle_position = (
            min(y for _, y in intersection_nodes)
            if intersection_nodes
            else 0
        )
        max_aisle_position = max(y for _, y, _ in vertices_coords.values())
        n_aisles = int(max(x for x, _, _ in vertices_coords.values()))

        excluded_nodes = [*pick_nodes, start_node, end_node]

        closest_node_to_start = (
            distance_matrix[start_node]
            .drop(labels=excluded_nodes)
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
        parsed: dict[str, Any],
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

        capacity = header["CapaPicker"]
        n_pickers = header["NbPickers"]
        picker_speed = 1 / header["TimeToTravelOneDistanceUnit"]
        time_per_pick = header["PickTime"]
        tour_setup_time = header["SetupTime"]

        pick_cart = PickCart(
            n_dimension=1,
            capacities=[capacity],
            dimensions=[DimensionType.ORDERLINES],
            n_boxes=1,
            box_can_mix_orders=True,
        )

        resources = Resources(
            tpe=ResourceType.HUMAN,
            resources=[
                Resource(
                    id=i,
                    capacity=capacity,
                    speed=picker_speed,
                    time_per_pick=time_per_pick,
                    pick_cart=pick_cart,
                    tour_setup_time=tour_setup_time,
                )
                for i in range(n_pickers)
            ],
        )

        warehouse_info = WarehouseInfo(
            tpe=WarehouseInfoType.OFFLINE,
        )

        return BaseWarehouseDomain(
            problem_class="OBSRP",
            objective="Distance",
            layout=layout,
            articles=articles,
            orders=orders,
            resources=resources,
            storage=storage,
            warehouse_info=warehouse_info,
        )

    def _build(self, parsed: dict[str, Any]) -> BaseWarehouseDomain:
        layout = self.build_layout(parsed)
        return self.build_domain_with_layout(parsed, layout)

    def _resolve_path(self, filepath: str | Path) -> Path:
        filepath = Path(filepath)

        if not filepath.is_absolute():
            filepath = self.data_dir / filepath

        return filepath

    def _parse(self, filepath: str) -> dict[str, Any]:
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

        def next_data_line():
            line = next_line()

            while line is not None and line.startswith("//"):
                line = next_line()

            return line

        skip_to_prefix("//NbLocations")

        header["NbLocations"] = int(next_data_line())
        header["NbProducts"] = int(next_data_line())
        header["NbPickers"] = int(next_data_line().split()[0])
        header["CapaPicker"] = int(next_data_line().split()[0])
        header["TimeToTravelOneDistanceUnit"] = int(next_data_line().split()[0])
        header["SetupTime"] = int(next_data_line().split()[0])
        header["PickTime"] = int(next_data_line().split()[0])

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
        skip_to_prefix("//NbOrders")

        header["NbOrders"] = int(next_data_line())

        for _ in range(header["NbOrders"]):
            parts = next_data_line().split()

            order_number = int(parts[0])
            due_date = int(parts[1])
            nb_products_in_order = int(parts[3])

            idx = 4
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
                        "due_date": due_date,
                        "order_date": 0,
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

            if line is None or line.startswith("//Location"):
                break

            parts = next_line().split()

            if len(parts) >= 3:
                shortest_paths[(int(parts[0]), int(parts[1]))] = float(parts[2])

        skip_to_prefix("//Location")

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

        aisle_x_positions = sorted(
            {
                x
                for x, _, node_type in vertices_coords.values()
                if node_type == "pick_node"
            }
        )

        def snap_to_nearest_aisle(x: float) -> float:
            return min(
                aisle_x_positions,
                key=lambda aisle_x: abs(aisle_x - x),
            )

        snapped_coords = {}

        for idx, (x, y, node_type) in vertices_coords.items():
            if node_type == "intersection":
                snapped_coords[idx] = (
                    snap_to_nearest_aisle(x),
                    y,
                    node_type,
                )
            else:
                snapped_coords[idx] = (
                    x,
                    y,
                    node_type,
                )

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
            "vertices_coords": snapped_coords,
        }
