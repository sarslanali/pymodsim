import polyline
from abc import abstractmethod, ABC
from typing import Set, Optional, List, Tuple, Dict, Union
from simulator.data_readers import simulation_classes as sc
from json import dumps
from logging import getLogger
from shapely.geometry import LineString


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class RouteElement:
    __slots__ = ["geometry", "distance", "duration"]

    def __init__(self, geometry: Union[List[Tuple[float, float]], LineString], distance: float, duration: float):
        """ A class for single element of a route

        :param geometry: List of (lon, lats) or (x,y). A shapely LineString for the element is also acceptable.
        :param distance: distance in meters for the route element
        :param duration: durations in seconds for the route element
        """
        self.geometry: LineString = geometry if isinstance(geometry, LineString) else LineString(geometry)
        self.distance: float = distance
        self.duration: float = duration

    @property
    def speed(self):
        if self.duration == 0:
            return 0
        else:
            return self.distance / self.duration

    @property
    def speed_kmph(self):
        return self.speed * 3.6

    def __repr__(self):
        return "RouteElement: " + dumps({"speed": self.speed, "distance": self.distance, "duration": self.duration,
                                         "geometry": list(self.geometry.coords)})


class AbstractRouter(ABC):

    def __init__(self):
        self.time_factor: float = 1.0
        super().__init__()

    def reset_time_factor(self):
        self.time_factor = 1.0

    @staticmethod
    def points_to_string(points: List["sc.Point"]):
        """convert from list of points to semi-colon seperated string of coordinates    """
        comma_coordinates = [','.join(map(str, (x.lon, x.lat))) for x in points]
        return ';'.join(map(str, [x for x in comma_coordinates]))

    @staticmethod
    def points_to_polyline(points: List["sc.Point"]):
        """convert from list of points to polyline """
        return polyline.encode([(x.lat, x.lon) for x in points])

    @abstractmethod
    def calculate_dict(self, moving_objects: Union[List["sc.MovingObject"], Set["sc.MovingObject"]],
                       stationary_objects: Union[List["sc.StationaryObject"], Set["sc.StationaryObject"]],
                       factored_time: bool = False):
        pass

    @abstractmethod
    def calculate_from_points(self, sources: Union[List["sc.Point"], Set["sc.Point"]],
                              destinations: Union[List["sc.Point"], Set["sc.Point"], None] = None,
                              factored_time: bool = False):
        pass

    @abstractmethod
    def route_steps(self, origin_destination_tuples: List[Tuple["sc.Point", "sc.Point"]]) -> List[List[RouteElement]]:
        pass


class _OSRMRouter(AbstractRouter):

    def __init__(self, url_osrm: Optional[str] = None,
                 time_matrix: Optional[Dict[Tuple[str, str], float]] = None,
                 distance_matrix: Optional[Dict[Tuple[str, str], float]] = None,
                 use_polyline: bool = True, incremental_matrices: bool = False):
        self.url_osrm: str = "http://127.0.0.1:4000" if url_osrm is None else url_osrm
        self.time_matrix:  Dict[Tuple[str, str], float] = {} if time_matrix is None else time_matrix
        self.distance_matrix: Dict[Tuple[str, str], float] = {} if distance_matrix is None else distance_matrix
        self.use_polyline: bool = use_polyline
        self.incremental_matrices: bool = incremental_matrices
        super().__init__()

    def __repr__(self):
        return dumps({key: getattr(self, key) for key in ["usr_osrm", "use_polyline", "incremental_matrices"]})

    def __add_missing_points(self, moving_objects: Union[List["sc.MovingObject"], Set["sc.MovingObject"]],
                             stationary_objects: Union[List["sc.StationaryObject"],
                                         Set["sc.StationaryObject"], None], bsize: int = 100,
                             factored_time: bool = False):

        all_destinations = set().union(*[stationary_object.locations for stationary_object in stationary_objects])
        all_sources = set([c.orig for c in moving_objects])
        all_sources.union(all_destinations)
        point_tuples = [(p1, p2) for p1 in all_sources for p2 in all_destinations
                        if (p1.key, p2.key) not in self.time_matrix]

        if point_tuples:
            sources, destinations = list(zip(*point_tuples))
            sources, destinations = set(sources), set(destinations)
            len_car_sources = len([pt for pt in sources if pt.key[0] == "v"])
            getLogger().info("OSRM called for %i sources and %i destinations while simulating "
                             "pickled scenario. Cars Sources =%i Reqs Sources =%i" % (len(sources), len(destinations),
                                                                                      len_car_sources,
                                                                                      len(sources) - len_car_sources))
            time, distance = self.calculate_from_points(list(sources), list(destinations), bsize=bsize,
                                                        factored_time = factored_time)
            self.time_matrix.update(time)
            self.distance_matrix.update(distance)
        time_factor = self.time_factor if factored_time is True else 1.0
        time_matrix = {(p1.key, p2.key): self.time_matrix[p1.key, p2.key] * time_factor
                       for p1 in all_sources for p2 in all_destinations}
        distance_matrix = {(p1.key, p2.key): self.distance_matrix[p1.key, p2.key]
                           for p1 in all_sources for p2 in all_destinations}
        return time_matrix, distance_matrix

    def remove_keys(self, keys: List[Tuple[str, str]]):
        for key in keys:
            self.time_matrix.pop(key)
            self.distance_matrix.pop(key)
