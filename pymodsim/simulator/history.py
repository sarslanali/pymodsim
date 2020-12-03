
""" Class for recording history of PyMoDSim simulations. """

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

from pandas import DataFrame, to_datetime, HDFStore
from collections import defaultdict
import os
from numpy import array
from pickle import dump
from pymodsim.data_readers import implemented_simulation_classes as isc
import typing as tp


class History:

    def __init__(self, settings_dict=None):
        """
        Class for recoding the data of the dynamic simulation
        """
        self.trip_info = defaultdict(list)
        self.realtime_info = defaultdict(list)
        self.matching_stats = defaultdict(list)
        self.ALNSOprStatsList = []
        self.time_factor = defaultdict(list)
        self.vehicle_positions = None
        self.fixed_visitables: tp.Dict[str, tp.List[isc.Visitable]] = {}
        self.stop_animation = False
        self.recent_expired_requests = None
        self.vehicle_miles = None
        self.simulation_settings = settings_dict
        self.summary = None

    def get_attrb_by_name(self, name):
        return getattr(self, name)

    def set_attrb_by_name(self, name, value):
        setattr(self, name, value)

    def get_fixed_visitable_locations(self):
        loc_dict ={}
        for name, visitable_list in self.fixed_visitables.items():
            loc_dict[name] = array([x.latlon for v in visitable_list for x in v.locations])
        return loc_dict

    def add_info(self, info_type_string, values_dict, append_to_not_present=True):
        info = getattr(self, info_type_string)
        for key, value in values_dict.items():
            info[key].append(value)
        if append_to_not_present and len(info) > 0:
            for key in set(info.keys()).difference(values_dict.keys()):
                info[key].append(None)

    def addALNSOprStats(self, extradict):
        self.ALNSOprStatsList.append(extradict)

    def get_realtime_info_df(self):
        realtime_dataframe = DataFrame(self.realtime_info)
        realtime_dataframe["ActualTime"] = to_datetime(realtime_dataframe["ActualTime"])
        realtime_dataframe.index = realtime_dataframe["ActualTime"]
        return realtime_dataframe

    def get_trip_info_df(self):
        return DataFrame(self.trip_info)

    def get_time_factor_df(self):
        df = DataFrame(self.time_factor)
        if len(df) > 0:
            df.index = to_datetime(df["ActualTime"])
        return df

    def get_matching_stats_df(self):
        return DataFrame(self.matching_stats)

    def get_vehicle_mile_df(self):
        return DataFrame.from_dict(self.vehicle_miles)

    def get_alns_batch_stats(self):
        TupleList = []
        for i, listitem in enumerate(self.ALNSOprStatsList):
            TupleList.extend([(i, key, item[0], item[1], item[2]) for key, item in listitem.items()])
        return DataFrame.from_records(TupleList, columns=["BatchCallNr", "Operator", "Count", "Final Weight", "Type"])

    def write_to_file(self, path, csv_or_pickle="csv", as_single_dict=False):
        extension = ".csv" if csv_or_pickle=="csv" else ".p"
        func_name = "to_csv" if csv_or_pickle=="csv" else "to_pickle"
        dataframes = [self.get_trip_info_df(), self.get_realtime_info_df(), self.get_matching_stats_df(),
                      self.get_alns_batch_stats(), self.get_vehicle_mile_df()]
        dataframes.append(DataFrame({"value name": list(self.summary.keys()),
                                     "value": list(self.summary.values())}))
        filenames = ["tripinfo", "realtimeinfo", "matching_stats", "alns_operator_stats", "vehicle_miles",
                     "summary"]
        if as_single_dict is True:
            result_dict = {name: df for name, df in zip(filenames, dataframes)}
            result_dict.update({"settings": self.simulation_settings})
            with open(os.path.join(path, "simulation_results.pkl"), "wb") as file:
                dump(result_dict, file)
        else:
            for df, filename in zip (dataframes, filenames):
                file_path = os.path.join(path, filename + extension)
                if len(df) > 0:
                    getattr(df, func_name)(file_path)






