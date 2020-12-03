
""" Nearest neighbor assignment heuristic for PyMoDSim. """

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

from pymodsim.data_readers import implemented_simulation_classes as isc
import typing as tp


def nearest_neighbour(cars: tp.List[isc.Car], reqs: tp.List[isc.CustomerRequest], time_dict, settings) \
        -> tp.Dict[isc.MovingObject, tp.List[isc.Point]]:
    dropoff_time = {}
    result_dict = {}
    if reqs:
        dict_picktime = {}
        unscheduled_reqs = reqs.copy()

        chkCars = cars
        while unscheduled_reqs:
            for newreq in unscheduled_reqs:
                for car in chkCars:
                    if car in result_dict:
                        last_dropoff = result_dict[car][-1]
                        dist = time_dict[last_dropoff.key, newreq.orig.key]
                        if dropoff_time[car, last_dropoff] + dist < newreq.orig_window[1]:
                            dict_picktime.update({(car, newreq): dropoff_time[car, last_dropoff] + dist})
                    else:
                        dist = time_dict[car.orig.key, newreq.orig.key]
                        picktime = car.treq + dist
                        if picktime < newreq.orig_window[1]:
                            dict_picktime.update({(car, newreq): picktime})

            # if dict_picktime is empty it means no further requests can be scheduled to a car
            if not dict_picktime:
                break
            mincar, minreq = min(dict_picktime, key=dict_picktime.get)
            picktime = max(dict_picktime[(mincar, minreq)], minreq.orig_window[0]) + settings.boarding_time
            droptime = picktime + time_dict[minreq.orig.key, minreq.dest.key] + settings.disembarking_time
            if mincar not in result_dict:
                result_dict.update({mincar: [minreq.orig, minreq.dest]})
            else:
                result_dict[mincar].extend([minreq.orig, minreq.dest])
            dropoff_time.update({(mincar, minreq.dest): droptime})
            unscheduled_reqs.remove(minreq)
            for car in cars:
                dict_picktime.pop((car, minreq), None)
            for req in unscheduled_reqs:
                dict_picktime.pop((mincar, req), None)
            # Only check for mincar in next iteration
            chkCars = [mincar]
    return result_dict

