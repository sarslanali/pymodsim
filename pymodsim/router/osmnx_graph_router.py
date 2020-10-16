from pymodsim.router.osm_fixed_nodes_router import FixedNodesRouter
from pandas import DataFrame, read_pickle
from networkx import Graph, get_edge_attributes, get_node_attributes, to_numpy_array, read_gpickle, write_gpickle
from networkx.relabel import convert_node_labels_to_integers
import igraph as ig
from osmnx import graph_to_gdfs
from typing import Optional, Union
from numpy import empty, full, nan, uint16
from numba import njit
from tqdm.auto import tqdm
import osmnx as ox
from pyproj import Transformer
from pymodsim.router import RouteElement
from pathlib import Path


@njit
def sum_costs(costs, indices, counts):
    cost_sum = empty((indices.shape[0], indices.shape[1]))
    for i in range(indices.shape[0]):
        for j in range(indices.shape[1]):
            cost_sum[i,j] = 0
            path = indices[i,j]
            for k in range(counts[i,j]-1):
                cost_sum[i,j] += costs[path[k], path[k+1]]
    return cost_sum


def remove_not_reachable_nodes(nx_graph):
    # Remove all nodes that have "in but no out" or "out but no in"

    total_removal_nodes = []
    graph = nx_graph.copy()
    removal_nodes = set([node for node in graph.nodes if graph.in_degree(node) < 1])
    removal_nodes.update([node for node in graph.nodes if graph.out_degree(node) < 1])
    print("Removing not reachable nodes from osmnx graph")
    while len(removal_nodes) != 0:
        removal_nodes = set([node for node in graph.nodes if graph.in_degree(node) < 1])
        removal_nodes.update([node for node in graph.nodes if graph.out_degree(node) < 1])
        graph.remove_nodes_from(removal_nodes)
        total_removal_nodes.extend(removal_nodes)
    print("Total removed: {} out of {}".format(len(total_removal_nodes), len(nx_graph.nodes)))
    print("New Number of nodes:", len(graph.nodes))
    return graph


def convert_to_igraph(graph: Graph):
    graph_nx = convert_node_labels_to_integers(graph)
    graph_ig = ig.Graph(directed=True)
    graph_ig.add_vertices(list(graph_nx.nodes()))
    graph_ig.add_edges(list(graph_nx.edges()))
    graph_ig.vs['osmid'] = list(get_node_attributes(graph_nx, 'osmid').values())
    graph_ig.es["length"] = list(get_edge_attributes(graph_nx, "length").values())
    graph_ig.es["travel_time"] = list(get_edge_attributes(graph_nx, "travel_time").values())
    return graph_ig


class OSMNxGraphRouter(FixedNodesRouter):

    def __init__(self, osmnx_graph: Optional[Graph] = None,
                 missing_speed: Optional[float] = None,
                 constant_speed: Optional[float] = None,
                 path_type:str = "shortest",
                 straight_line_routes: bool = False,
                 save_folder: Union[Path, str] = "processed_osmnx_router_data",
                 load_processed: bool = False):
        """ A Router class based on the osmnx networkx graph

        :param osmnx_graph:     osmnx networkx graph
        :param missing_speed:   default speed (kph) in case some link has not specified "maxspeed"
        :param constant_speed:  a constant speed (kph) for all links
        :param path_type:       the type of path to use: "quickest" or "shortest
        :param straight_line_routes:    simulate on straight lines with osmnx based travel times.
        :param save_folder:     folder for saving processed osmnx router data
        :param load_processed:  Load processed graph data directly from save_folder
        """

        self.path_type: str = path_type
        osmnx_graph, self.osmnx_graph_utm = self._process_graph(osmnx_graph, missing_speed, constant_speed,
                                                                save_folder, load_processed)
        self.graph_ig: ig.Graph = convert_to_igraph(osmnx_graph)
        self.osmid_to_inx: dict = {node: inx for inx, node in enumerate(osmnx_graph.nodes)}
        self.inx_to_osmid_dict: dict = {inx: node for inx, node in enumerate(osmnx_graph.nodes)}
        edges_df = graph_to_gdfs(osmnx_graph, nodes=False).set_index(['u', 'v'])
        self.edge_data_dict = edges_df[["geometry", "travel_time", "length"]].to_dict()
        time_df, distance_df = self._calculate_time_distance_df(osmnx_graph, save_folder)
        self.straight_line_routes = straight_line_routes
        super().__init__(time_df, distance_df)

    def _process_graph(self, osmnx_graph, missing_speed, constant_speed, save_folder, load_processed):
        if load_processed is False:
            osmnx_graph = osmnx_graph.copy()
            osmnx_graph.missing_speed = missing_speed
            osmnx_graph.constant_speed = constant_speed
            osmnx_graph = remove_not_reachable_nodes(osmnx_graph)
            osmnx_graph = self._remove_multiple_edges(osmnx_graph)
            self._assign_missing_speeds_in_graph(osmnx_graph, missing_speed, constant_speed)
            if not Path(save_folder).exists():
                Path(save_folder).mkdir()
            osmnx_graph_utm = osmnx_graph
            if not ox.is_crs_utm(osmnx_graph.graph["crs"]):
                osmnx_graph_utm = ox.projection.project_graph(osmnx_graph)
                write_gpickle(osmnx_graph_utm, Path(save_folder, "utm_graph.pickle"))
                write_gpickle(osmnx_graph, Path(save_folder, "latlon_graph.pickle"))
        else:
            print("loading processed graphs from folder {}".format(save_folder))
            osmnx_graph = read_gpickle(Path(save_folder, "latlon_graph.pickle"))
            osmnx_graph_utm = read_gpickle(Path(save_folder, "utm_graph.pickle"))
        return osmnx_graph, osmnx_graph_utm

    def _calculate_time_distance_df(self, nx_graph: Graph, save_folder: Union[Path, str]):

        assert self.path_type in {"shortest", "quickest"}, "path type can only be shortest or quickest"
        distance_path = Path(save_folder,"{}_distance_df.pickle".format(self.path_type))
        time_path = Path(save_folder, "{}_time_df.pickle".format(self.path_type))
        node_names = list(nx_graph.nodes)
        try:
            distance_df = read_pickle(distance_path)
            time_df = read_pickle(time_path)
            assert len(set(distance_df.index).union(time_df.index)) == len(time_df.index), \
                "nodes mismatch between the read time and distance matrix"
            assert len(set(distance_df.index).union(node_names)) == len(node_names), \
                "nodes mismatch between the read matrices and provided graph"
            print("loaded time and distance matrices from available files")
            return time_df, distance_df
        except AssertionError as e:
            print(e)
        except FileNotFoundError:
            print("could not find files for precalculated matrices for OSMNxGraphRouter")

        print("calculating the time and distance matrices")
        primary_weight, secondary_weight = ("length", "travel_time") if self.path_type == "shortest" \
            else ("travel_time", "length")
        distance = self.graph_ig.shortest_paths(weights=primary_weight)
        time = self._calculate_secondary_cost_matrix(nx_graph, primary_weight, secondary_weight)
        distance_df = DataFrame(distance, columns=node_names, index=node_names)
        time_df = DataFrame(time, columns=node_names, index=node_names)
        print("writing the calculated matrices to files")
        distance_df.to_pickle(distance_path)
        time_df.to_pickle(time_path)
        return time_df, distance_df

    @staticmethod
    def _remove_multiple_edges(osmnx_graph):
        """ Remove all the multi edges from the osmnx graph """

        g = osmnx_graph.copy()
        g.remove_edges_from(osmnx_graph.edges)
        total_removed = 0
        print("Removing multiple edges between nodes from the osmnx graph.")
        for u, v, data in osmnx_graph.edges(data=True):
            if g.has_edge(u, v):
                total_removed += 1
                if g[u][v][0]['length'] > data["length"]:
                    g.add_edge(u, v, key=0, **data)
            else:
                g.add_edge(u, v, key=0, **data)
        print("Total edges removed={}".format(total_removed))
        return g

    @staticmethod
    def _assign_missing_speeds_in_graph(osmnx_graph, missing_speed, constant_speed):
        # Fix speeds in the graph
        for node1, node2, data in osmnx_graph.edges(data=True):
            if constant_speed is not None:
                data.update({"maxspeed": constant_speed})
            elif "maxspeed" not in data or data["maxspeed"] is None:
                data["maxspeed"] = missing_speed
            else:
                if isinstance(data["maxspeed"], list):
                    speed_with_unit = data["maxspeed"][0].split()
                else:
                    speed_with_unit = data["maxspeed"].split()
                speed = float(speed_with_unit[0])
                # If the speed is given in miles per hour, convert to kph
                if len(speed_with_unit) > 1 and speed_with_unit[1] == "mph":
                    speed = speed * 1.60934
                data.update({"maxspeed": speed})
            speed_meter_per_second = data["maxspeed"] / 3.6
            data.update({"travel_time": data["length"] / speed_meter_per_second})

    def _calculate_secondary_cost_matrix(self, nx_graph, primary_weight, secondary_weight, chunk_size=200):
        graph_ig = self.graph_ig
        costs = full((len(graph_ig.vs), len(graph_ig.vs)), nan)
        nodes = list(range(len(graph_ig.vs)))
        edge_costs = to_numpy_array(nx_graph, weight=secondary_weight, multigraph_weight=min)
        for i in tqdm(range(0, len(graph_ig.vs), chunk_size)):
            chunk = nodes[i: i + chunk_size]
            inx = empty((len(chunk), len(nodes), len(nodes)), dtype=uint16)
            counts = empty((len(chunk), len(nodes)), dtype=uint16)
            for j in tqdm(range(len(chunk))):
                source_node = chunk[j]
                paths = graph_ig.get_shortest_paths(v=source_node, weights=primary_weight)
                for k, path in enumerate(paths):
                    inx[j, k, :len(path)] = path
                    counts[j, k] = len(path)
            costs[chunk, :] = sum_costs(edge_costs, inx, counts)
        return costs

    def calculate_nearest_nodes(self, points=None, lats=None, lons=None):
        proj_transformer = Transformer.from_proj('epsg:4326', self.osmnx_graph_utm.graph["crs"])
        if points is not None:
            lats, lons = zip(*[pt.latlon for pt in points])
        # project to utm
        x, y = proj_transformer.transform(lats, lons)
        return ox.geo_utils.get_nearest_nodes(self.osmnx_graph_utm, x, y, method="kdtree")

    def route_steps(self, origin_destination_tuples):
        if self.straight_line_routes is True:
            return super().route_steps(origin_destination_tuples)
        sources, destinations = list(zip(*origin_destination_tuples))
        source_nodes = [self.osmid_to_inx[pt.osm_id] for pt in sources]
        dest_nodes = [self.osmid_to_inx[pt.osm_id] for pt in destinations]
        weight = "length" if self.path_type == "shortest" else "travel_time"
        paths = [self.graph_ig.get_shortest_paths(src, dest, weights=weight)[0]
                 for src, dest in zip(source_nodes, dest_nodes)]
        paths = [[self.inx_to_osmid_dict[i] for i in path] for path in paths]
        steps = []
        for i, path in enumerate(paths):
            if len(path) == 1:
                steps.append([RouteElement([sources[i].lonlat, destinations[i].lonlat], 0.0, 0.0)])
            else:
                route_pairwise = list(zip(path[:-1], path[1:]))
                lines = [self.edge_data_dict["geometry"][uv] for uv in route_pairwise]
                travel_times = [self.edge_data_dict["travel_time"][uv] for uv in route_pairwise]
                lengths = [self.edge_data_dict["length"][uv] for uv in route_pairwise]
                steps.append([RouteElement(line, distance, time)
                              for line, distance, time in zip(lines, lengths, travel_times)])
        return steps
