from pandas import read_pickle
from numpy import array
from simulator.router import AbstractRouter, RouteElement
from pandas import DataFrame
from typing import Union


class FixedNodesRouter(AbstractRouter):

    def __init__(self, time_matrix: Union[str, DataFrame], distance_matrix: Union[str, DataFrame]):
        """ A Router class based on a fixed number open street maps (osm) nodes within a city

        :param time_matrix_path: path of pandas dataframe for travelling time with osm ids as indices and columns
        :param distance_matrix_path: path pandas dataframe for travel distance with osm ids as indices and columns
        """

        time_matrix: DataFrame = time_matrix if isinstance(time_matrix, DataFrame) else read_pickle(time_matrix)
        distance_matrix: DataFrame = distance_matrix if isinstance(distance_matrix, DataFrame) \
            else read_pickle(time_matrix)
        self.time_matrix: array() = time_matrix.values
        self.distance_matrix: array() = distance_matrix.values
        self.node_inx_dict: dict = {inx: i for i, inx in enumerate(time_matrix.index.values)}
        super().__init__()

    def route_steps(self, origin_destination_tuples):
        sources, destinations = list(zip(*origin_destination_tuples))
        time, distance = self.calculate_from_points(sources, destinations)
        steps = []
        for src, dest in origin_destination_tuples:
            steps.append([RouteElement([src.lonlat, dest.lonlat], distance[src.key, dest.key],
                                       time[src.key, dest.key])])
        return steps

    def calculate_dict(self, moving_objects, stationary_objects, factored_time=False):
        time_dict, distance_dict = {}, {}
        mob_origins = [moving_object.orig for moving_object in moving_objects]
        visit_locations = set().union(*[stationary_object.locations for stationary_object in stationary_objects])
        # First Calculate distance with car origins as sources only
        time, distance = self.calculate_from_points(mob_origins, visit_locations, factored_time=factored_time)
        time_dict.update(time)
        distance_dict.update(distance)
        # Now calculate a whole matrix
        time, distance = self.calculate_from_points(visit_locations, factored_time=factored_time)
        time_dict.update(time)
        distance_dict.update(distance)
        return time_dict, distance_dict

    def calculate_from_points(self, sources, destinations = None, factored_time = False):
        source_ids = [pt.osm_id for pt in sources]
        source_keys = [pt.key for pt in sources]
        destinations = sources if destinations is None else destinations
        destination_ids = [pt.osm_id for pt in destinations]
        destination_keys = [pt.key for pt in destinations]

        time, distance = self.calculate_from_osm_ids(source_ids, destination_ids, factored_time)
        time = time.flatten().tolist()
        distance = distance.flatten().tolist()
        orig_dest_tuples = [(orig, dest) for orig in source_keys for dest in destination_keys]
        time_dict = dict(zip(orig_dest_tuples, time))
        distance_dict = dict(zip(orig_dest_tuples, distance))
        return time_dict, distance_dict

    def calculate_from_osm_ids(self, source_ids, destination_ids = None, factored_time = False):
        src_np_inx = array([self.node_inx_dict[x] for x in source_ids])
        if destination_ids is None:
            dst_np_inx = src_np_inx
        else:
            dst_np_inx = array([self.node_inx_dict[x] for x in destination_ids])
        time = self.time_matrix[src_np_inx[:, None], dst_np_inx]
        distance = self.distance_matrix[src_np_inx[:, None], dst_np_inx]
        factor = self.time_factor if factored_time is True else 1.0
        return time * factor, distance

    def calculate_from_osm_ids_pairs(self, source_ids, destination_ids, factored_time = False):
        factor = self.time_factor if factored_time is True else 1.0
        src_np_inx = array([self.node_inx_dict[x] for x in source_ids])
        dst_np_inx = array([self.node_inx_dict[x] for x in destination_ids])
        time = self.time_matrix[src_np_inx, dst_np_inx]
        distance = self.distance_matrix[src_np_inx, dst_np_inx]
        return time * factor, distance
