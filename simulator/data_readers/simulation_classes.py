from copy import copy
from itertools import chain
from abc import abstractmethod, ABC
from typing import Optional, Set, Tuple, Dict, List, Union
from simulator import router
from simulator import config


class SimulationObject:
    __slots__ = ["ID"]

    def __init__(self, ID: str):
        self.ID: str = ID

    def __hash__(self):
        return hash(self.ID)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.ID == other.ID
        else:
            return False

    def __repr__(self):
        return self.ID

    def __getstate__(self):
        return {name: getattr(self, name) for name in self.get_all_slots()}

    def __setstate__(self, state):
        [setattr(self, name, value) for name, value in state.items()]

    def get_all_slots(self):
        return chain.from_iterable(getattr(cls, '__slots__', []) for cls in type(self).__mro__)


class Point:
    __slots__ = ["lat", "lon", "key", "associated_object_id"]

    def __init__(self, lat: float, lon: float, key: str, simulation_object_id: Optional[str] = None):
        """ Class for a geographical location

        :param lat:                     Latitude
        :param lon:                     Longitude
        :param key:                     ID for the Point
        :param simulation_object_id:    The ID of the associated SimulationObject. If None then value of key is used
        """

        self.lat: float = lat
        self.lon: float = lon
        self.key: str = key
        self.associated_object_id: str = key if simulation_object_id is None else simulation_object_id

    def __repr__(self):
        return "Point: {}".format(self.key)

    def __hash__(self):
        return hash(self.key)

    @property
    def latlon(self):
        return self.lat, self.lon

    @property
    def lonlat(self):
        return self.lon, self.lat


class FixedNodesPoint(Point):
    __slots__ = ["osm_id", "zone"]

    def __init__(self, lat, lon, key, simulation_object_id, zone: int, osm_id: int):
        self.osm_id: int = osm_id
        self.zone: int = zone
        super().__init__(lat, lon, key, simulation_object_id)


class PathNode:
    __slots__ = ["stationary_object", "reach_time", "geographical_point"]

    def __init__(self, stationary_object: "StationaryObject", reach_time: float, geographical_point: Point):
        self.stationary_object: "StationaryObject" = stationary_object
        self.reach_time: float = reach_time
        self.geographical_point: Point = geographical_point

    def __repr__(self):
        return "PathNode {} reach_time: {:0.2f}".format(self.geographical_point.key, self.reach_time)

    def __hash__(self):
        return hash(self.__repr__())

    def __eq__(self, other):
        # Compare if the keys of geographical_point and reach_time is same
        if self.geographical_point.key == other.geographical_point.key and self.reach_time == other.reach_time:
            return True
        else:
            return False


class MovingObject(ABC, SimulationObject):
    __slots__ = ["orig", "treq", "path", "serving_node", "nr_current_onboard", "on_board_paired_objects"]

    def __init__(self, orig: Point, treq: float, key: str):
        self.orig: Point = orig
        self.treq: float = treq
        self.serving_node: Optional[PathNode] = None
        self.nr_current_onboard: int = 0
        self.path: List[PathNode] = []
        self.on_board_paired_objects: Set[Servable] = set()
        super().__init__(key)

    def __deepcopy__(self, memodict={}):
        new_object = copy(self)
        new_object.path = self.path.copy()
        new_object.on_board_paired_objects = self.on_board_paired_objects.copy()
        return new_object

    def add_to_path(self, stationary_object: "StationaryObject",
                    estimated_reach_time: float, location: Point):
        self.path.append(PathNode(stationary_object, estimated_reach_time, location))

    @abstractmethod
    def get_optimization_copy(self, current_time: float,
                              optimization_counts: Dict[str, "StationaryObjectTW"],
                              settings: config.Settings) \
            -> Union[Tuple["MovingObject", List[PathNode]], Tuple[None, None]]:
        """ Returns a copy of the moving object along with a set of removed nodes the path of copy"""
        pass

    def consume_energy(self, covered_distance: float, settings: config.Settings):
        pass

    def reached_serving_node(self, current_time: float, time_of_arrival: float, covered_distance: float,
                             settings: config.Settings):
        last_node: PathNode = self.serving_node
        next_point, move_time = None, None
        self.consume_energy(covered_distance, settings)

        if last_node is not None:
            earliest_leave_time = last_node.stationary_object.reached(self, current_time, time_of_arrival,
                                                                      last_node.geographical_point, settings)
            if current_time >= earliest_leave_time:
                move_time = earliest_leave_time
        else:
            move_time = current_time

        if move_time is not None:
            # Means the moving object is free to move from the current node
            if len(self.path) > 0:
                # Move to the next node in path
                self.serving_node = self.path.pop(0)
                next_point = self.serving_node.geographical_point
                self.orig = next_point
            else:
                # set the vehicle idle
                self.serving_node = None
        assert self.nr_current_onboard >= 0, "nr_current_onboard cannot be negative."
        return next_point, move_time

    def recalculate_path_reach_times(self, current_time: float, settings: config.Settings,
                                     router: Optional["router.AbstractRouter"] = None,
                                     time_distance_dict_tuple: Optional[Tuple[dict, dict]] = None):
        node_visit_objects = {node.stationary_object for node in self.path}
        if len(node_visit_objects) > 0:
            if time_distance_dict_tuple is None:
                time_dict, distance_dict = router.calculate_dict([self], node_visit_objects, factored_time=True)
            else:
                time_dict, distance_dict = time_distance_dict_tuple
            move_object_copy = copy(self)
            if self.serving_node is None:
                leave_time = current_time
                last_node_point = self.orig
            else:
                leave_time = self.serving_node.stationary_object.estimate_leave_time(move_object_copy,
                                                                                     self.serving_node.reach_time,
                                                                                     self.serving_node.reach_time,
                                                                                     self.serving_node.geographical_point,
                                                                                     settings)
                last_node_point = self.serving_node.geographical_point
            for node in self.path:
                move_object_copy.consume_energy(distance_dict[last_node_point.key, node.geographical_point.key],
                                                settings)
                node.reach_time = leave_time + time_dict[last_node_point.key, node.geographical_point.key]
                leave_time = node.stationary_object.estimate_leave_time(move_object_copy, node.reach_time, node.reach_time,
                                                                        node.geographical_point, settings)
                last_node_point = node.geographical_point

    def remove_object_from_path(self, current_time: float, objects: Set["StationaryObject"],
                                settings: config.Settings, router: Optional["router.AbstractRouter"] = None,
                                time_distance_dict_tuple: Optional[Tuple[dict, dict]] = None):
        """ Removes a stationary object from the path """

        if self.serving_node is not None:
            assert self.serving_node.stationary_object not in objects, \
                "Trying to remove serving node object {}".format(self.serving_node.stationary_object)
        self.path = [node for node in self.path if node.stationary_object not in objects]
        if router is not None or time_distance_dict_tuple is not None:
            self.recalculate_path_reach_times(current_time, settings, router, time_distance_dict_tuple)


class StationaryObject(SimulationObject):
    __slots__ = ["locations"]

    def __init__(self, locations: List[Point], key: str):
        self.locations: List[Point] = locations
        super().__init__(key)

    @abstractmethod
    def reached(self, moving_object: MovingObject, current_time: float, time_of_arrival: float,
                point_reached: Point, settings: config.Settings) -> float:
        pass

    def estimate_leave_time(self, moving_object: MovingObject, current_time: float, time_of_arrival: float,
                            point_reached: Point, settings: config.Settings) -> float:
        return self.reached(moving_object, current_time, time_of_arrival, point_reached, settings)


class StationaryObjectTW(StationaryObject):
    __slots__ = ["time_windows"]

    def __init__(self, time_windows: List[Tuple[float, float]], **kwargs):
        self.time_windows: List[Tuple[float, float]] = time_windows
        super().__init__(**kwargs)


class Visitable(StationaryObject):
    pass


class VisitableTW(StationaryObjectTW):
    pass


class Servable(StationaryObject):
    __slots__ = ["serving_moving_object"]

    def __init__(self, **kwargs):
        self.serving_moving_object = None
        super().__init__(**kwargs)


class ServableTW(StationaryObjectTW):
    __slots__ = ["serving_moving_object"]

    def __init__(self, **kwargs):
        self.serving_moving_object = None
        super().__init__(**kwargs)


class _SinglePoint(StationaryObject):

    def __init__(self, location: Point, key: str, **kwargs):
        super().__init__([location], key, **kwargs)

    def reached(self, moving_object: MovingObject, current_time: float, time_of_arrival: float,
                point_reached: Point, settings: config.Settings) -> float:
        assert point_reached == self.location, "reached location {} is not equal to " \
                                               "location {} of {}".format(point_reached, self.location, self)
        return self.reached_location(moving_object, current_time, time_of_arrival, settings)

    @abstractmethod
    def reached_location(self, moving_object: MovingObject, current_time: float, time_of_arrival: float,
                         settings: config.Settings) -> float:
        pass

    @property
    def location(self):
        return self.locations[0]

    @location.setter
    def location(self, value: Point):
        self.locations[0] = value


class _SinglePointTW(_SinglePoint, StationaryObjectTW):

    def __init__(self, location: Point, time_window: Tuple[float, float], key: str, **kwargs):
        super().__init__(location=location, time_windows=[time_window], key=key, **kwargs)

    @property
    def time_window(self):
        return self.time_windows[0]

    @time_window.setter
    def time_window(self, value: float):
        self.time_windows[0] = value


class _PairedPoints(StationaryObject):

    def __init__(self, orig: Point, dest: Point, key: str, **kwargs):
        super().__init__(locations=[orig, dest], key=key, **kwargs)

    def reached(self, moving_object, current_time, time_of_arrival, point_reached, settings):
        if point_reached == self.orig:
            return self.reached_orig(moving_object, current_time, time_of_arrival, settings)
        elif point_reached == self.dest:
            return self.reached_dest(moving_object, current_time, time_of_arrival, settings)
        else:
            raise ValueError("The reached PathNode {} does not belong to {}".format(moving_object.serving_node,
                                                                                    str(self)))

    @abstractmethod
    def reached_orig(self, moving_object: MovingObject, current_time: float, time_of_arrival: float,
                     settings: config.Settings):
        pass

    @abstractmethod
    def reached_dest(self, moving_object: MovingObject, current_time: float, time_of_arrival: float,
                     settings: config.Settings):
        pass

    @property
    def orig(self):
        return self.locations[0]

    @orig.setter
    def orig(self, value: Point):
        self.locations[0] = value

    @property
    def dest(self):
        return self.locations[1]

    @dest.setter
    def dest(self, value: Point):
        self.locations[1] = value


class _PairedPointsTW(_PairedPoints, StationaryObjectTW):

    def __init__(self, orig: Point, dest: Point, key: str, orig_window: tuple, dest_window: tuple, **kwargs):
        super().__init__(orig=orig, dest=dest, time_windows=[orig_window, dest_window], key=key, **kwargs)

    @property
    def orig_window(self) -> Tuple[float, float]:
        return self.time_windows[0]

    @orig_window.setter
    def orig_window(self, value: Tuple[float, float]):
        self.time_windows[0] = value

    @property
    def dest_window(self) -> Tuple[float, float]:
        return self.time_windows[1]

    @dest_window.setter
    def dest_window(self, value: Tuple[float, float]):
        self.time_windows[1] = value


class SinglePointVisitable(_SinglePoint, Visitable):
    pass


class PairedPointVisitable(_PairedPoints, Visitable):
    pass


class SinglePointVisitableTW(_SinglePointTW, VisitableTW):
    pass


class PairedPointVisitableTW(_PairedPointsTW, VisitableTW):
    pass


class SinglePointServable(_SinglePoint, Servable):
    pass


class PairedPointsServable(_PairedPoints, Servable):

    def reached(self, moving_object, current_time, time_of_arrival, point_reached, settings):
        assert self.serving_moving_object == moving_object, "The serving_moving_object {} of {} does not " \
                                                            "match with the given " \
                                                            "moving object {}".format(self.serving_moving_object,
                                                                                      self, moving_object)
        return super().reached(moving_object, current_time, time_of_arrival, point_reached, settings)


class SinglePointServableTW(_SinglePointTW, ServableTW):
    pass


class PairedPointsServableTW(_PairedPointsTW, ServableTW):

    def reached(self, moving_object, current_time, time_of_arrival, point_reached, settings):
        assert self.serving_moving_object == moving_object, "The serving_moving_object {} of {} does not " \
                                                            "match with the given " \
                                                            "moving object {}".format(self.serving_moving_object,
                                                                                      self, moving_object)
        return super().reached(moving_object, current_time, time_of_arrival, point_reached, settings)
