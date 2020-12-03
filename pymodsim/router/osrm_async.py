
__license__ = u"""
Copyright (C), BMW Group
Author: Arslan Ali Syed (arslan-ali.syed@bmw.de, arslan.syed@tum.de)]

This program is free software: you can redistribute it
and/or modify it under the terms of the GNU Lesser
General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be
useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License
for more details.

You should have received a copy of the GNU Lesser General
Public License along with this program. If not, see
<http://www.gnu.org/licenses/>.
"""

from pymodsim.router import _OSRMRouter, chunks
from json import loads
from urllib.parse import urljoin
from polyline import decode as polyline_decode
from typing import List, Tuple, Dict, Optional
from pymodsim.data_readers.implemented_simulation_classes import Point
from pymodsim.router import RouteElement
import aiohttp
import asyncio


def close_session(session):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(session.close())


class OSRMRouter(_OSRMRouter):

    def __repr__(self):
        return "OSRM Async Router API: " + super().__repr__()

    async def route(self, points_list: List[Point], session: aiohttp.ClientSession,
                    param_dict: Optional[Dict[str, str]] = None) -> dict:
        """ async version of route for calling the route service of OSRM

        :param points_list: list of Points for the route service
        :param session:     ClientSession for calling the route service
        :param param_dict: parameters dictionary for the "route" service... see OSRM api docs for parameters
        """
        url = urljoin(self.url_osrm, "route/v1/driving/")
        # Add the coordinates
        url = url + self.points_to_string(points_list)
        async with session.get(url, params=param_dict) as r:
            assert r.status == 200, "Some problem in calling OSRM router"
            jsondict = loads(await r.text())
        return jsondict

    async def table(self, list_pts: List[Point], session: aiohttp.ClientSession,
                    param_dict: Optional[Dict[str, str]] = None) -> dict:
        """ async function for calling the table service of OSRM

        :param list_pts:        list of pts, with each element having named element "lat" and "lon"
        :param session:         ClientSession for calling the route service
        :param param_dict:      parameters dictionary for the "table" service... see OSRM api docs for parameters
        """
        url = urljoin(self.url_osrm, "table/v1/driving/")
        if self.use_polyline is True:
            url = url + "polyline(" + self.points_to_polyline(list_pts) + ")"
        else:
            url = url + self.points_to_string(list_pts)
        payload_str = "&".join("%s=%s" % (k, v) for k, v in list(param_dict.items()))
        if payload_str: url = url + "?" + payload_str
        async with session.get(url) as r:
            assert r.status == 200, "Some problem in calling OSRM router"
            jsondict = loads(await r.text())
        return jsondict

    def route_steps(self, origin_destination_tuples: List[Tuple[Point, Point]]) -> List[List[RouteElement]]:
        """ Calculates the steps between origin and destinations

        The distance and duration are in meters and seconds respectively

        :param origin_destination_tuples: list of origin and destinations point tuples
        :return: list of RouteElement
        """

        async def single_pair_step(points_tuple: Tuple[Point, Point]) -> List[RouteElement]:
            route_dict = await self.route(list(points_tuple), session, param_dict)
            steps = route_dict["routes"][0]["legs"][0]["steps"]
            return [RouteElement([(lon, lat) for (lat, lon) in polyline_decode(step["geometry"])],
                                 step["distance"], step["duration"]) for step in steps[:-1]]

        param_dict = {"overview": "full", "geometries": "polyline", "steps": "true"}
        tasks = []
        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession(loop=loop)
        for orig_dest_tuple in origin_destination_tuples:
            tasks.append(asyncio.ensure_future(single_pair_step(orig_dest_tuple)))
        results = loop.run_until_complete(asyncio.gather(*tasks))
        close_session(session)
        return results

    async def __duration_matrix(self, srcpts: List[Point], destpts: List[Point], session: aiohttp.ClientSession,
                                factored_time: bool = False):
        param = {"sources": ";".join(map(str, range(len(srcpts)))),
                 "destinations": ";".join(map(str, range(len(srcpts), len(srcpts) + len(destpts))))}
        # The new version of OSRM sometimes return "null" if there is only one destination
        # Therefore, we add one more destination and later remove it
        if len(destpts) == 1:
            param["destinations"] += ";0"
        if len(srcpts) == 1:
            param["sources"] += ";0"
        param.update({"annotations": "duration,distance"})
        jsondict = await self.table(srcpts + destpts, session, param_dict=param)
        time_factor = self.time_factor if factored_time is True else 1.0
        duration_dict = {(src.key, dest.key): jsondict["durations"][n][m] * time_factor
                         for n, src in enumerate(srcpts) for m, dest in enumerate(destpts)}
        distance_dict = {}
        if "distances" in jsondict:
            distance_dict = {(src.key, dest.key): jsondict["distances"][n][m] for n, src in enumerate(srcpts)
                             for m, dest in enumerate(destpts)}
        return duration_dict, distance_dict

    def calculate_from_points(self, sources, destinations=None,
                              session: Optional[aiohttp.ClientSession] = None,
                              factored_time=False, bsize=100, nr_parallel_osrm_reqs=4):

        if isinstance(sources, set):
            sources = list(sources)
        if destinations is None:
            destinations = sources
        elif isinstance(destinations, set):
            destinations = list(destinations)

        def innerloop(xpts):
            j = 0
            points_list = []
            while j < len(destinations) // bsize:
                points_list.append(destinations[j * bsize: j * bsize + bsize])
                j += 1
            if len(destinations) % bsize > 0:
                points_list.append(destinations[j * bsize:])
            return points_list

        tasks = []
        pts_groups = []
        i = 0
        while i < len(sources) // bsize:
            xpts = sources[i * bsize: i * bsize + bsize]
            ypts_list = innerloop(xpts)
            for pts in ypts_list:
                pts_groups.append((xpts, pts))
            i += 1
        xpts = sources[i * bsize:]
        if xpts:
            ypts_list = innerloop(xpts)
            for pts in ypts_list:
                pts_groups.append((xpts, pts))

        close_later = False
        if session is None:
            close_later = True
            session = aiohttp.ClientSession(loop=asyncio.get_event_loop())

        loop = asyncio.get_event_loop()
        time_dict, distance_dict = {}, {}
        for i, chunk in enumerate(chunks(pts_groups, nr_parallel_osrm_reqs)):
            tasks = [asyncio.ensure_future(self.__duration_matrix(xpts, ypts, session, factored_time))
                     for xpts, ypts in chunk]
            result = loop.run_until_complete(asyncio.gather(*tasks))
            [time_dict.update(r[0]) for r in result]
            [distance_dict.update(r[1]) for r in result]
        if close_later is True: close_session(session)
        if self.incremental_matrices is True:
            self.time_matrix.update(time_dict)
            self.distance_matrix.update(distance_dict)
        return time_dict, distance_dict

    def calculate_dict(self, moving_objects, stationary_objects, factored_time: bool = False, bsize: int = 100,
                       nr_parallel_osrm_reqs: int = 4):
        """  Calculates Time and Distance Matrix directly from car, customer_request and service points

        :param moving_objects:          set or list of MovingObjects
        :param stationary_objects:      set or list of StationaryObject
        :param factored_time:           whether to scale the travel times using self.time_factor
        :param bsize:                   max size of single osrm api matrix request
        :param nr_parallel_osrm_reqs:   number of maximum async calls to osrm api. Default is 4.
        :return: dictionaries of travel times(seconds) and distances (meters)
        """

        if self.incremental_matrices is True:
            return self.__add_missing_points(moving_objects, stationary_objects, bsize=bsize,
                                             factored_time=factored_time)

        results = []
        session = aiohttp.ClientSession(loop=asyncio.get_event_loop())
        mob_origins = [c.orig for c in moving_objects]
        visit_locations = set().union(*[stationary_object.locations for stationary_object in stationary_objects])

        # First Calculate distance with car origins as sources only
        results.append(self.calculate_from_points(mob_origins, visit_locations, session, factored_time, bsize,
                                                  nr_parallel_osrm_reqs))
        # Now calculate a whole matrix
        results.append(self.calculate_from_points(visit_locations, factored_time=factored_time, bsize=bsize,
                                                  session=session, nr_parallel_osrm_reqs=nr_parallel_osrm_reqs))
        close_session(session)
        time_dict, distance_dict = {}, {}
        [time_dict.update(r[0]) for r in results]
        [distance_dict.update(r[1]) for r in results]
        return time_dict, distance_dict




