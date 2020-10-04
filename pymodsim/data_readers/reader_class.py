'''
Created on Apr 21, 2019

@author: Arslan Ali Syed
'''

from datetime import timedelta, datetime
import os
from pickle import dump, load
from pymodsim.data_readers import implemented_simulation_classes as isc
from pandas import read_pickle, read_csv, DataFrame
from tempfile import NamedTemporaryFile
from os import remove
from pymodsim.router import AbstractRouter
from pymodsim.router.osm_fixed_nodes_router import FixedNodesRouter
from pymodsim.router.osmnx_graph_router import OSMNxGraphRouter
from pymodsim.router.osrm_async import OSRMRouter
from typing import Optional, List
from pymodsim import config
from dynaconf import Dynaconf


class DataReader:

    def __init__(self, data_file_path: str, router: AbstractRouter, startdt: str, enddt: Optional[str] = None,
                 settings: Optional[Dynaconf] = None):

        """ A class for generating dynamic and static examples from given data file.

        :param data_file_path:      path of the given data file. The accepted formats are ".csv" or the pickled
                                    pandas dataframe file with ".pkl" extension
        :param router               router class for calculating travel time and distances
        :param startdt:             the starting date and time from where to read the given file in
                                    format: "year-month-day hour:min"
        :param enddt:               optional endtime for the reading the give datafile
        :param settings_object:     Settings object to be used for the simulation. This object can be generated from
                                    simulator.config file. If None then config.global_settings is used.
        """

        self.data_file_path: str = data_file_path
        self.settings: Dynaconf = config.global_settings.copy() if settings is None else settings
        # The new requests start to arrive after one minute of the start of simulation
        self.start_offset: timedelta = timedelta(seconds = self.settings.start_offset)
        self.startdt: datetime = datetime.strptime(startdt, '%Y-%m-%d %H:%M:%S')
        self.enddt: Optional[str] = None
        if enddt is not None:
            self.enddt = datetime.strptime(enddt, '%Y-%m-%d %H:%M:%S')
        self.actual_time: datetime = self.startdt - self.start_offset
        self.t_anticipate: timedelta = timedelta(seconds = self.settings.time_anticipate)
        self.router: AbstractRouter = router
        self.max_wait_time: float = self.settings.MaxWait

    def update_time(self, delta_time: timedelta):
        self.actual_time = self.startdt + delta_time - self.start_offset

    def _read_data_file(self, startdt=None, enddt=None) -> DataFrame:
        _, file_ext = os.path.splitext(self.data_file_path)
        if file_ext == ".csv":
            read_data = read_csv(self.data_file_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        elif file_ext == ".pkl":
            read_data = read_pickle(self.data_file_path)
        else:
            raise ValueError(
                "Wrong formatted data file given. Provide .csv or pickled pandas dataframe having .pkl extension")
        startdt = self.startdt if startdt is None else startdt
        if startdt is not None:
            read_data = read_data[read_data["tpep_pickup_datetime"] > startdt]
        enddt = self.enddt if enddt is None else enddt
        if enddt is not None:
            read_data = read_data[read_data["tpep_pickup_datetime"] < enddt]
        if isinstance(self.router, OSMNxGraphRouter):
            read_data["pickup_node"] = self.router.calculate_nearest_nodes(lats=read_data["pickup_latitude"].values,
                                                                           lons=read_data["pickup_longitude"].values)
            read_data["dropoff_node"] = self.router.calculate_nearest_nodes(lats=read_data["dropoff_latitude"].values,
                                                                           lons=read_data["dropoff_longitude"].values)
        return read_data

    def _requests_generator(self) -> List[isc.CustomerRequest]:
        read_data = self._read_data_file()
        temp = NamedTemporaryFile(delete=False)
        temp.close()
        read_data.to_csv(temp.name)
        read_data = read_csv(temp.name, chunksize=10000, parse_dates=["tpep_pickup_datetime"])
        wait_time = timedelta(seconds=self.max_wait_time)
        nr = 0
        for df in read_data:
            for (_, row) in df.iterrows():
                req_id = "r{0}".format(nr)
                pickloc = isc.FixedNodesPoint(row["pickup_latitude"], row["pickup_longitude"], req_id + "_p", req_id,
                                              row["origin_zone"], row.get("pickup_node", None))
                droploc = isc.FixedNodesPoint(row["dropoff_latitude"], row["dropoff_longitude"], req_id + "_d", req_id,
                                              row["destination_zone"], row.get("dropoff_node", None))
                treq = row["tpep_pickup_datetime"]
                tpick_LA = treq + wait_time
                nr_passenger = row["passenger_count"]
                yield isc.CustomerRequest(pickloc, droploc, req_id, (treq, tpick_LA), (None, None),
                                          nr_passenger)
                nr += 1
        remove(temp.name)

    def calculate_time_factor(self, window):
        """ Calculates the time factor for scaling the travel time matrix. The factor is the ratio of mean actual
        trip time to the mean trip time calculated using fixed nodes of osrm

        :param window: the future requests window size in minutes based on which time factor is calculated
        :return:
        """
        assert isinstance(self.router, FixedNodesRouter), " time factor can be only with FixedNodesRouter " \
                                                          "at the moment"

        for df in self.generate_forecast_df(window):
            mean_actual = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).mean().total_seconds()
            time, _ = self.router.calculate_from_osm_ids_pairs(df.pickup_node,
                                                               df.dropoff_node,
                                                               factored_time=False)
            mean_time_router = time.mean()
            yield mean_actual / mean_time_router
        return

    def generate_forecast_df(self, window_size, add_buffer=False) -> DataFrame:
        """ Returns a generator that produces dataframe of the future rows of the data

        :param window_size: the window size in seconds for looking ahead compared to current time
        :param add_buffer: add additional time to start or end time according to window_size
        """

        if add_buffer is True:
            startdt = self.startdt + timedelta(seconds=window_size) if window_size < 0 else self.startdt
            enddt = self.enddt + timedelta(seconds=window_size) if window_size > 0 else self.enddt
            read_data = self._read_data_file(startdt, enddt)
        else:
            read_data = self._read_data_file()
        temp = NamedTemporaryFile(delete=False)
        temp.close()
        read_data.to_csv(temp.name)
        read_data = read_csv(temp.name, chunksize=10000, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        result = DataFrame()
        for df in read_data:
            result = result.append(df)
            while result.iloc[-1]["tpep_pickup_datetime"] > self.actual_time:
                result = result[result["tpep_pickup_datetime"] > self.actual_time]
                yield result[result["tpep_pickup_datetime"] < self.actual_time + timedelta(seconds=window_size)]
        os.remove(temp.name)
        return

    def _add_trip_info_to_requests(self, reqs: List[isc.CustomerRequest]):
        time_matrix, dist_matrix = self.router.calculate_from_points([r.orig for r in reqs], [r.dest for r in reqs],
                                                                     factored_time=True)
        for r in reqs:
            direct_duration = time_matrix[r.orig.key, r.dest.key]
            direct_distance = dist_matrix[r.orig.key, r.dest.key]
            r.dest_window = (r.orig_window[0] + timedelta(minutes = direct_duration),
                             r.orig_window[1] + timedelta(minutes = direct_duration))

    def _modify_reqs(self, reqs, start_time, cal_time_reach: bool = True):
        if cal_time_reach is True:
            self._add_trip_info_to_requests(reqs)
        for r in reqs:
            r.orig_window = ((r.orig_window[0] - start_time).total_seconds(),
                                (r.orig_window[1] - start_time).total_seconds())
            if cal_time_reach is True:
                r.dest_window = ((r.dest_window[0] - start_time).total_seconds(),
                                 (r.dest_window[1] - start_time).total_seconds())

    def dynamic_requests(self, cal_time_reach: bool = True):
        """
        Returns a generator that returns the newly generated requests based on the updated time
        """

        req_gen = self._requests_generator()
        new_reqs = []
        start_time = self.startdt - self.start_offset
        for req in req_gen:
            while req.orig_window[0] > self.actual_time + self.t_anticipate:
                if len(new_reqs) > 0:
                    self._modify_reqs(new_reqs, start_time, cal_time_reach)
                yield new_reqs
                new_reqs = []
            new_reqs.append(req)
        self._modify_reqs(new_reqs, start_time, cal_time_reach)
        yield new_reqs
        raise StopIteration

    def generate_cars(self, nr_cars, save_file_path=None, load_file_path=None, initial_price_per_kWh=None,
                     percentage_electric_cars=0) -> List[isc.MovingObject]:
        """ Generates random cars by taking the trip start positions from the given datafile

        :param nr_cars: number of cars to generate
        :param save_file_path: filepath if the generated cars need to be saved
        :param load_file_path: filepath if the cars need to be loaded from file
        :param initial_price_per_kWh: initial value of the unit price per kWh
        :param percentage_electric_cars: percentage of electric cars in the generated cars (0-100)
        :return: list of class car instances
        """
        if load_file_path is None:
            cars=[]
            diff = self.actual_time - self.startdt
            carStartT = diff.total_seconds()
            # Generate random car by picking random requests from data
            N = 0
            read_data = self._read_data_file()
            read_data = read_data.sample(nr_cars)
            car_origins = []
            for _, row in read_data.iterrows():
                zone = row.get("origin_zone", None)
                osm_id = row.get("pickup_node", None)
                key = "v{}".format(N)
                car_origins.append(isc.FixedNodesPoint(row["pickup_latitude"], row["pickup_longitude"],
                                                       key+"_origin", key, zone, osm_id))
                N += 1
            k = int(percentage_electric_cars * len(car_origins) / 100)
            cars = []
            for i in range(k):
                cars.append(isc.ElectricCar(car_origins[i], 0.0, car_origins[i].associated_object_id,
                                            initial_price_per_kWh, self.settings))
            for i in range(k, len(car_origins)):
                cars.append(isc.GasCar(car_origins[i], 0.0, car_origins[i].associated_object_id, self.settings))
            if save_file_path is not None:
                with open(save_file_path, 'wb') as f:
                    dump(cars, f)
        else:
            with open(load_file_path, 'rb') as f:
                cars = load(f)
        return cars

    @staticmethod
    def generate_service_stations(full_path_of_csv) -> List[isc.ServiceStation]:
        csv_df = read_csv(full_path_of_csv, delimiter=',', sep=',', header=0)
        service_stations = []
        for n, row in csv_df.iterrows():
            key = "ss"+str(n)
            location = isc.FixedNodesPoint(row.location_lat, row.location_lon, key, key,
                                           None, row.nearest_osm_id)
            if row.type == "electric":
                service_stations.append(isc.ElectricStation(location, key, row.vehicle_slots, row.prices,
                                                            row.charge_rate_max))
            elif row.type == "gas":
                service_stations.append(isc.GasStation(location, key, row.vehicle_slots, row.prices,
                                                       row.charge_rate_max))
        return service_stations

    def creat_static_example(self, nr_cars, use_last_cars=False, cal_time_reach=True, service_stations_file_path=None):
        """
        This method creates a static example for the requests and cars
        :param nr_cars: The number of cars to use, The cars are generated by randomly picking the
                        trip start locations from the given data file.
        :param UseLastCars: Whether to use the same cars from the last time or not
        :param cal_time_reach: if True calculatess the treach attribute of the request
        :param service_stations_file_path: full path for the csv file of the service station

        :returns : returns a List
                    - cars
                    - requests
                    - service stations if service_stations_file_path parameter is given
        """

        cars = []

        # Read the passenger requests points from the data
        reqs = list(self._requests_generator())
        if cal_time_reach is True:
            self._add_trip_info_to_requests(reqs)

        # the zero point starts t_anticipate before the startdt
        zero_time = self.startdt - self.t_anticipate
        self._modify_reqs(reqs, zero_time, True)

        # file for last generated cars locations
        path = os.path.join(os.path.dirname(os.getcwd()), 'GeneratedDriveNowCars.txt')
        if use_last_cars is False:
            cars = self.generate_cars(nr_cars, save_file_path=path)
        else:
            cars = self.generate_cars(nr_cars, load_file_path=path)
        # CarStart time at absolute 30 sec
        car_start = timedelta(seconds=30).total_seconds()
        for car in cars:
            car.treq = car_start
        if service_stations_file_path is not None:
            return cars, reqs, self.generate_service_stations(service_stations_file_path)
        else:
            return cars, reqs

    @classmethod
    def generate_scenario(cls, data_file_path: str, max_wait_time: float, startdt: str, enddt: str,
                          update_rate: float = 2 * 60, window_size: float = 3*60,
                          pickle_folder: Optional[str] = None, name_prefix: Optional[str] = None, **kwargs):
        """ Generate time (mintues) and distance (km) matrices for all the requests
        between self.startdt and self.enddt.

        The calculation is done in sliding window fashion to avoid calculation of a very large
        dictionary.

        :param data_file_path:      path of the given data file. The accepted formats are ".csv" or the pickled
                                    pandas dataframe file with ".pkl" extension.
        :param max_wait_time:       The Maximum waiting time for the request.
        :param startdt:             the starting date and time from where to read the given file in
                                    format: "year-month-day hour:min"
        :param enddt:               optinal endtime for the reading the give datafile
        :param update_rate: time in seconds for the update of sliding window
        :param window_size: time in seconds for the size of sliding window
        :param pickle_folder: if given, the current reader class instance and the time and distance
                            matrices are pickled as a dictionary
        :param name_prefix: additional prefix to the name of the pickled file
        :param kwargs:    Any additional arguments for creation of data_reader
        :return: data reader with osrm async router with updated time and distance matrices
        """
        import gc
        from copy import copy

        router = OSRMRouter(kwargs.get("url_osrm", None), incremental_matrices=True)
        reader = cls(data_file_path, max_wait_time, router, startdt, enddt, **kwargs)

        update_rate = timedelta(seconds=update_rate)
        time_dict, distance_dict = {}, {}
        start_time = reader.startdt
        end_time = reader.startdt + timedelta(seconds=window_size)
        orig_reader = copy(reader)
        reader.start_offset = timedelta()
        reader.t_anticipate = timedelta()
        reader.actual_time = start_time + update_rate
        reqs_generator = reader.dynamic_requests(cal_time_reach=False)
        new_reqs = next(reqs_generator)
        while start_time < reader.enddt:
            print("calculating matrices from time ", start_time, "to ", end_time)
            overlapping_reqs = new_reqs
            reader.actual_time = end_time
            new_reqs = next(reqs_generator)
            Reqs = overlapping_reqs + new_reqs
            points = [r.orig for r in Reqs]
            points.extend([r.dest for r in Reqs])
            time, dist = router.calculate_from_points(points, factored_time=True)
            time_dict.update(time)
            distance_dict.update(dist)
            gc.collect()
            start_time += update_rate
            end_time += update_rate
        full_pickle_path = None
        if pickle_folder is not None:
            if not os.path.isdir(pickle_folder):
                os.makedirs(pickle_folder)
            pickle_name = reader.startdt.strftime("%Y_%m_%d_from_%H_%M_%S_to_") \
                          + reader.enddt.strftime("%H_%M_%S") + ".pkl"
            if name_prefix:
                pickle_name = name_prefix + pickle_name
            full_pickle_path = os.path.join(pickle_folder, pickle_name)
            with open(full_pickle_path, "wb") as s:
                dump(orig_reader, s)
        return orig_reader, full_pickle_path

    @classmethod
    def from_pickled_scenario(cls, scenario_path: str):
        with open(scenario_path, "rb") as s:
            data_reader = load(s)
        return data_reader
