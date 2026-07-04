import abc
from typing import List, Any

from dataclasses import dataclass
import networkx as nx
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import scipy


def distance_matrix_generator(G: nx.Graph) -> pd.DataFrame:
    """
    Creates a distance matrix of the pick nodes in the warehouse using the Floyd-Warshall algorithm.
    """
    # Compute the shortest path lengths between all pairs of nodes using Floyd-Warshall
    all_pairs_shortest_path_length = dict(nx.floyd_warshall_predecessor_and_distance(G)[1])

    # Convert the numpy matrix to a pandas DataFrame
    nodes = list(G.nodes())
    matrix = pd.DataFrame(all_pairs_shortest_path_length, index=nodes, columns=nodes)
    matrix.index.name = "index"
    return matrix


def distance_matrix_generator_scipy(G: nx.Graph):
    #distance_mat = nx.floyd_warshall_numpy(self.graph, self.nodes_list)
    nodes = list(G.nodes())  # Extract node labels
    A = nx.adjacency_matrix(G, nodelist=nodes).tolil()  # Ensure node order is consistent
    dist_mat = scipy.sparse.csgraph.floyd_warshall(A, directed=False, unweighted=False)

    # Convert NumPy array to Pandas DataFrame
    df = pd.DataFrame(dist_mat, index=nodes, columns=nodes)
    return df


def distance_matrix_generator_from_shortest_paths(G: nx.Graph, shortest_paths: dict):
    """Generates the distance matrix for a graph based on pre-calculated shortest-paths"""
    nodes = sorted(list(G.nodes))
    n_nodes = len(nodes)

    # Initialize distance matrix
    dist_mat = np.zeros((n_nodes, n_nodes))

    # Fill with shortest-path distances
    for i, node_i in enumerate(nodes):
        for j, node_j in enumerate(nodes):
            if i == j:
                dist_mat[i, j] = 0
            else:
                # Use the pre-computed shortest paths if available
                if (node_i, node_j) in shortest_paths:
                    dist_mat[i, j] = shortest_paths[(node_i, node_j)]

    df = pd.DataFrame(dist_mat, index=nodes, columns=nodes)
    return df


def tour_matrix_generator(G: nx.Graph) -> pd.DataFrame:
    """
    Creates a tour matrix of the pick nodes in the warehouse using the Floyd-Warshall algorithm.
    """
    # Compute the shortest path lengths between all pairs of nodes using Floyd-Warshall
    all_pairs_shortest_paths = dict(nx.floyd_warshall_predecessor_and_distance(G)[0])

    nodes = list(G.nodes())
    matrix = pd.DataFrame(index=nodes, columns=nodes)

    for i in nodes:
        for j in nodes:
            # Reconstruct the path from i to j using the predecessor information
            path = []
            current = j
            while current != i:
                path.insert(0, current)
                current = all_pairs_shortest_paths[i][current]
            path.insert(0, i)

            matrix.at[i, j] = path
    matrix.index.name = "index"
    return matrix


def predecessor_matrix_generator(G: nx.Graph) -> dict[Any, Any] | dict[str, Any] | dict[str, str] | dict[bytes, bytes]:
    """
    Creates a predecessor matrix of the pick nodes in the warehouse using the Floyd-Warshall algorithm.
    """
    # Compute the shortest path lengths between all pairs of nodes using Floyd-Warshall
    all_pairs_shortest_paths = dict(nx.floyd_warshall_predecessor_and_distance(G)[0])

    return all_pairs_shortest_paths


@dataclass
class GraphParameters:
    n_aisles: int
    n_pick_locations: int
    dist_change_aisle: float
    dist_aisle_to_pick_location: float
    dist_pick_locations: float
    dist_start: float
    dist_end: float
    start_location: tuple[int, int]
    end_location: tuple[int, int]
    start_connection_point: tuple[int, int]
    end_connection_point: tuple[int, int]


@dataclass
class GraphExplicitRepresentation:
    vertices: List
    arcs: List


class GraphGeneratorBase(abc.ABC):
    def __init__(self,
                 G: nx.Graph | None = None,
                 **kwargs):

        if G is not None:
            self.G = G
        else:
            self.G = nx.Graph()

    def populate_graph(self):
        ...

    def render(self, plot: bool = True):
        ...


class ExplicitGraphGenerator(GraphGeneratorBase):
    def __init__(self,
                 vertices_coords: dict,
                 arcs: List,
                 G: nx.Graph | None = None,
                 **kwargs):
        super().__init__(G, **kwargs)
        self.vertices_coords = vertices_coords
        self.arcs = arcs

    def _add_nodes(self):
        # Add all vertices
        for vertex_idx in self.vertices_coords:
            x_pos, y_pos, name = self.vertices_coords[vertex_idx]
            # self.G.add_node(vertex_idx, pos=(x_pos, y_pos), type=name)
            self.G.add_node((x_pos, y_pos), pos=(x_pos, y_pos), type=name)

    def _add_edges(self):
        # Add all edges with their distances
        for start, end, distance in self.arcs:
            # self.G.add_edge(start, end, weight=distance)
            self.G.add_edge(
                (self.vertices_coords[start][0],
                 self.vertices_coords[start][1]),
                (self.vertices_coords[end][0],
                 self.vertices_coords[end][1]), weight=distance)

    def populate_graph(self):
        self._add_nodes()
        self._add_edges()

    def render(self, plot: bool = True, out_name=False, dpi=700, font_size=5, node_size=50, node_color='lightblue') -> None:
        pos = nx.get_node_attributes(self.G, 'pos')
        nx.draw(self.G, pos=pos, with_labels=True, node_color=node_color, font_size=font_size, node_size=node_size)
        weight = nx.get_edge_attributes(self.G, 'weight')
        nx.draw_networkx_edge_labels(self.G, pos, edge_labels=weight, font_size=font_size)

        if plot:
            plt.show()

        if out_name:
            plt.savefig(out_name, dpi=dpi)


class ShelfStorageGraphGenerator(GraphGeneratorBase):
    def __init__(
            self,
            n_aisles: int,
            n_pick_locations: int,
            dist_aisle: float,
            dist_pick_locations: float,
            dist_aisle_location: float,
            dist_start: float,  # from start to graph
            dist_end: float,  # from end to graph
            start_location: tuple[int, int] = (0, 0),
            end_location: tuple[int, int] = (-1, 0),
            start_connection_point: tuple[int, int] = (1, 0),
            end_connection_point: tuple[int, int] = (1, 0),
            G: nx.Graph | None = None,
            reverse_pick_nodes: bool = None,
            **kwargs):
        super().__init__(G, **kwargs)

        self.n_aisles = n_aisles
        self.n_pick_locations = n_pick_locations
        self.dist_aisle = dist_aisle
        self.dist_pick_locations = dist_pick_locations
        self.dist_aisle_location = dist_aisle_location
        self.dist_start = dist_start
        self.dist_end = dist_end
        self.end_location = end_location
        self.start_location = start_location
        self.start_connection_point = start_connection_point
        self.end_connection_point = end_connection_point
        self.reverse_pick_nodes = reverse_pick_nodes

    def _add_pick_nodes(self) -> None:

        for i in range(1, self.n_aisles + 1):
            for j in range(1, self.n_pick_locations + 1):
                if self.reverse_pick_nodes:
                    self.G.add_node((i, j),
                                    pos=(i, self.n_pick_locations - j + 1), type='pick_node')
                else:
                    self.G.add_node((i, j), pos=(i, j), type='pick_node')

    def _add_start_and_end_nodes(self) -> None:
        self.G.add_node(self.start_location, pos=self.start_location, type='start_node')
        self.G.add_node(self.end_location, pos=self.end_location, type='end_node')

    def _add_change_aisle_nodes(self) -> None:
        # add nodes for changing aisles bottom
        for i in range(1, self.n_aisles + 1):
            self.G.add_node((i, 0), pos=(i, 0), type='change_aisle_node')
        # add nodes for changing aisles top
        for i in range(1, self.n_aisles + 1):
            self.G.add_node((i, self.n_pick_locations + 1), pos=(i, self.n_pick_locations + 1), type='change_aisle_node')

    def _add_edges(self) -> None:

        for aisle in range(1, self.n_aisles + 1):

            # add edges between pick locations (vertical
            for location in range(1, self.n_pick_locations):
                self.G.add_edge((aisle, location),
                                (aisle, location + 1),
                                weight=self.dist_pick_locations)

            # add edges between pick locations and change aisle nodes (bottom)
            self.G.add_edge((aisle, 0),
                            (aisle, 1),
                            weight=self.dist_aisle_location)

            # add edges between pick locations and change aisle nodes (top)
            self.G.add_edge((aisle, self.n_pick_locations ),
                            (aisle, self.n_pick_locations + 1),
                            weight=self.dist_aisle_location)

            if aisle < self.n_aisles:
                # add edges between aisles (horizontal) (bottom)
                self.G.add_edge((aisle, 0),
                                (aisle + 1, 0),
                                weight=self.dist_aisle)

                # add edges between aisles (horizontal) (top)
                self.G.add_edge((aisle, self.n_pick_locations + 1),
                                (aisle + 1, self.n_pick_locations + 1),
                                weight=self.dist_aisle)

        # add edges between start and first aisle
        self.G.add_edge(self.start_location,
                        self.start_connection_point,
                        weight=self.dist_start)

        # add edges between end and last aisle
        self.G.add_edge(self.end_location,
                        self.end_connection_point,
                        weight=self.dist_end)

    def populate_graph(self):
        self._add_pick_nodes()
        self._add_change_aisle_nodes()
        self._add_start_and_end_nodes()
        self._add_edges()

    def render(self, plot: bool = True, out_name=False, dpi=700, font_size=5, node_size=50, node_color='lightblue') -> None:
        pos = nx.get_node_attributes(self.G, 'pos')
        nx.draw(self.G, pos=pos, with_labels=True, node_color=node_color, font_size=font_size, node_size=node_size)
        weight = nx.get_edge_attributes(self.G, 'weight')
        nx.draw_networkx_edge_labels(self.G, pos, edge_labels=weight, font_size=font_size)

        if plot:
            plt.show()

        if out_name:
            plt.savefig(out_name, dpi=dpi)


class MultiBlockShelfStorageGraphGenerator(GraphGeneratorBase):
    def __init__(
        self,
        n_aisles: int,
        n_pick_locations: int,
        dist_aisle: float,
        dist_pick_locations: float,
        dist_aisle_location: float,
        dist_start: float,
        dist_end: float,
        start_location: tuple[int, int] = (0, 0),
        end_location: tuple[int, int] = (-1, 0),
        start_connection_point: tuple[int, int] = (1, 0),
        end_connection_point: tuple[int, int] = (1, 0),
        G: nx.Graph | None = None,
        reverse_pick_nodes: bool = None,
        n_blocks: int = 1,                     # number of vertically stacked blocks
        dist_between_blocks: float | None = None,  # edge weight from top of block b to bottom of b+1
        block_gap_rows: int = 0,               # visual spacing only (no effect on distances)
        **kwargs
    ):
        super().__init__(G, **kwargs)
        self.n_aisles = n_aisles
        self.n_pick_locations = n_pick_locations
        self.dist_aisle = dist_aisle
        self.dist_pick_locations = dist_pick_locations
        self.dist_aisle_location = dist_aisle_location
        self.dist_start = dist_start
        self.dist_end = dist_end
        self.end_location = end_location
        self.start_location = start_location
        self.start_connection_point = start_connection_point
        self.end_connection_point = end_connection_point
        self.reverse_pick_nodes = reverse_pick_nodes

        self.n_blocks = max(1, int(n_blocks))
        self.dist_between_blocks = dist_between_blocks if dist_between_blocks is not None else dist_aisle_location
        self.block_gap_rows = int(block_gap_rows)

        self._block_span_rows = self.n_pick_locations + 2 + self.block_gap_rows
        self._offsets = [b * self._block_span_rows for b in range(self.n_blocks)]

    def _p(self, aisle: int, y: int, b: int) -> tuple[int, int]:
        return (aisle, y + self._offsets[b])

    def _add_pick_nodes(self) -> None:
        for b in range(self.n_blocks):
            for i in range(1, self.n_aisles + 1):
                for j in range(1, self.n_pick_locations + 1):
                    y_draw = self.n_pick_locations - j + 1 if self.reverse_pick_nodes else j
                    node = self._p(i, j, b)
                    self.G.add_node(node, pos=(i, y_draw + self._offsets[b]), type='pick_node')

    def _add_start_and_end_nodes(self) -> None:
        # unchanged
        self.G.add_node(self.start_location, pos=self.start_location, type='start_node')
        self.G.add_node(self.end_location, pos=self.end_location, type='end_node')

    def _add_change_aisle_nodes(self) -> None:
        for b in range(self.n_blocks):
            for i in range(1, self.n_aisles + 1):
                self.G.add_node(self._p(i, 0, b), pos=(i, 0 + self._offsets[b]), type='change_aisle_node')
                self.G.add_node(self._p(i, self.n_pick_locations + 1, b),
                                pos=(i, self.n_pick_locations + 1 + self._offsets[b]), type='change_aisle_node')

    def _add_edges(self) -> None:
        for b in range(self.n_blocks):
            for aisle in range(1, self.n_aisles + 1):
                # vertical edges along an aisle
                for location in range(1, self.n_pick_locations):
                    self.G.add_edge(self._p(aisle, location, b),
                                    self._p(aisle, location + 1, b),
                                    weight=self.dist_pick_locations)

                # connect to bottom/top change-aisle nodes
                self.G.add_edge(self._p(aisle, 0, b), self._p(aisle, 1, b), weight=self.dist_aisle_location)
                self.G.add_edge(self._p(aisle, self.n_pick_locations, b),
                                self._p(aisle, self.n_pick_locations + 1, b),
                                weight=self.dist_aisle_location)

                # horizontal edges between aisles (bottom/top rows)
                if aisle < self.n_aisles:
                    self.G.add_edge(self._p(aisle, 0, b), self._p(aisle + 1, 0, b), weight=self.dist_aisle)
                    self.G.add_edge(self._p(aisle, self.n_pick_locations + 1, b),
                                    self._p(aisle + 1, self.n_pick_locations + 1, b),
                                    weight=self.dist_aisle)

        for b in range(self.n_blocks - 1):
            top_y = self.n_pick_locations + 1
            for aisle in range(1, self.n_aisles + 1):
                self.G.add_edge(self._p(aisle, top_y, b),
                                self._p(aisle, 0, b + 1),
                                weight=self.dist_between_blocks)

        self.G.add_edge(self.start_location,
                        self.start_connection_point,
                        weight=self.dist_start)

        # add edges between end and last aisle
        self.G.add_edge(self.end_location,
                        self.end_connection_point,
                        weight=self.dist_end)

    def populate_graph(self):
        self._add_pick_nodes()
        self._add_change_aisle_nodes()
        self._add_start_and_end_nodes()
        self._add_edges()

    def render(self, plot: bool = True, out_name=False, dpi=700, font_size=5, node_size=50, node_color='lightblue') -> None:
        pos = nx.get_node_attributes(self.G, 'pos')
        nx.draw(self.G, pos=pos, with_labels=True, node_color=node_color, font_size=font_size, node_size=node_size)
        weight = nx.get_edge_attributes(self.G, 'weight')
        nx.draw_networkx_edge_labels(self.G, pos, edge_labels=weight, font_size=font_size)

        if plot:
            plt.show()

        if out_name:
            plt.savefig(out_name, dpi=dpi)


