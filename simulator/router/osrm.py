from simulator.router import _OSRMRouter, chunks
import requests
from json import loads
from urllib.parse import urljoin
from polyline import decode as polyline_decode
from typing import List, Tuple, Dict, Optional
from simulator.data_readers.implemented_simulation_classes import Point
from simulator.router import RouteElement


class OSRMRouter(_OSRMRouter):

    def __repr__(self):
        return "OSRM Router API: " + super().__repr__()

    def route(self, points_list: List[Point], param_dict: Optional[Dict[str, str]] = None) -> dict:
        """ Calls the route service of osrm

        :param points_list: list of Points for the route service
        :param param_dict: parameters dictionary for the "route" service... see OSRM api docs for parameters
        """
        url = urljoin(self.url_osrm, "route/v1/driving/")
        # Add the coordinates
        url = url + self.points_to_string(points_list)
        r = requests.get(url, params=param_dict)
        assert r.status_code == 200, "Some problem in calling OSRM router"
        json_dict = loads(r.text)
        return json_dict

    def table(self, list_pts: List[Point], param_dict: Optional[Dict[str, str]] = None) -> dict:
        """ Calls the table service of osrm

        :param list_pts: list of pts, with each element having named element "lat" and "lon"
        :param param_dict: parameters dictionary for the "table" service... see OSRM api docs for parameters
        """
        if param_dict is None:
            param_dict = {}
        url = urljoin(self.url_osrm, "table/v1/driving/")
        if self.use_polyline is True:
            url = url + "polyline(" + self.points_to_polyline(list_pts) + ")"
        else:
            url = url + self.points_to_string(list_pts)
        payload_str = "&".join("%s=%s" % (k, v) for k, v in list(param_dict.items()))
        if payload_str: url = url + "?" + payload_str
        r = requests.get(url)
        assert r.status_code==200, "Some problem in calling OSRM router"
        jsondict = loads(r.text)
        return jsondict

    def route_steps(self, origin_destination_tuples: List[Tuple[Point, Point]]) -> List[List[RouteElement]]:
        """ Calculates the steps between origin and destinations

        The distance and duration are in meters and seconds respectively

        :param origin_destination_tuples: list of origin and destinations point tuples
        :return: list of RouteElement
        """
        param_dict = {"overview": "full", "geometries": "polyline", "steps": "true"}
        results = []
        for start_point, end_point in origin_destination_tuples:
            route_dict = self.route([start_point, end_point], param_dict)
            steps = route_dict["routes"][0]["legs"][0]["steps"]
            results.append([RouteElement([(lon, lat) for (lat, lon) in polyline_decode(step["geometry"])],
                                         step["distance"],
                                         step["duration"]) for step in steps[:-1]])
        return results

    def __duration_matrix(self, srcpts: List[Point], destpts: List[Point], factored_time: bool = False):
        param = {"sources": ";".join(map(str, range(len(srcpts)))),
                 "destinations": ";".join(map(str, range(len(srcpts), len(srcpts) + len(destpts))))}
        # The new version of OSRM sometimes return "null" if there is only one destination
        # Therefore, we add one more destination and later remove it
        if len(destpts) == 1:
            param["destinations"] += ";0"
        if len(srcpts) == 1:
            param["sources"] += ";0"
        param.update({"annotations": "duration,distance"})
        jsondict = self.table(srcpts + destpts, param_dict=param)
        time_factor = self.time_factor if factored_time is True else 1.0
        duration_dict = {(src.key, dest.key): jsondict["durations"][n][m] * time_factor
                         for n, src in enumerate(srcpts) for m, dest in enumerate(destpts)}
        distance_dict = {}
        if "distances" in jsondict:
            distance_dict = {(src.key, dest.key): jsondict["distances"][n][m] for n, src in enumerate(srcpts)
                             for m, dest in enumerate(destpts)}
        return duration_dict, distance_dict

    def calculate_from_points(self, sources, destinations=None, factored_time: bool = False, bsize: int = 100):

        if isinstance(sources, set):
            sources = list(sources)
        if destinations is None:
            destinations = sources
        elif isinstance(destinations, set):
            destinations = list(destinations)

        def inner_loop(xpts):
            j = 0
            points_list = []
            while j < len(destinations) // bsize:
                points_list.append(destinations[j * bsize: j * bsize + bsize])
                j += 1
            if len(destinations) % bsize > 0:
                points_list.append(destinations[j * bsize:])
            return points_list

        pts_groups = []
        i = 0
        while i < len(sources) // bsize:
            xpts = sources[i * bsize: i * bsize + bsize]
            ypts_list = inner_loop(xpts)
            for pts in ypts_list:
                pts_groups.append((xpts, pts))
            i += 1
        xpts = sources[i * bsize:]
        if xpts:
            ypts_list = inner_loop(xpts)
            for pts in ypts_list:
                pts_groups.append((xpts, pts))

        results = [self.__duration_matrix(xpts, ypts, factored_time) for xpts, ypts in pts_groups]
        time_dict, distance_dict = {}, {}
        [time_dict.update(r[0]) for r in results]
        [distance_dict.update(r[1]) for r in results]
        if self.incremental_matrices is True:
            self.time_matrix.update(time_dict)
            self.distance_matrix.update(distance_dict)
        return time_dict, distance_dict

    def calculate_dict(self, moving_objects, stationary_objects, factored_time: bool = False, bsize: int = 100):
        """  Calculates Time and Distance Matrix directly from car, customer_request and service points

        :param moving_objects:      set or list of MovingObjects
        :param stationary_objects:  set or list of StationaryObject
        :param factored_time:       whether to scale the travel times using self.time_factor
        :param bsize:               max size of single osrm api matrix request
        :return: dictionaries of travel times(seconds) and distances (meters)
        """

        if self.incremental_matrices is True:
            return self.__add_missing_points(moving_objects, stationary_objects, bsize=bsize,
                                             factored_time=factored_time)
        time_dict, distance_dict = {}, {}
        mob_origins = [c.orig for c in moving_objects]
        visit_locations = set().union(*[stationary_object.locations for stationary_object in stationary_objects])
        # First Calculate distance with moving objects origins as sources only
        time, distance = self.calculate_from_points(mob_origins, visit_locations, bsize=bsize,
                                                    factored_time=factored_time)
        time_dict.update(time)
        distance_dict.update(distance)
        # Now calculate a whole matrix
        time, distance = self.calculate_from_points(visit_locations, bsize=bsize, factored_time=factored_time)
        time_dict.update(time)
        distance_dict.update(distance)
        return time_dict, distance_dict



