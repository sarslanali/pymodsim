
""" Main simulator class for PyMoDSim simulator. """

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

from timeit import default_timer
import logging
from collections import defaultdict, OrderedDict
from pymodsim.data_readers import implemented_simulation_classes as isc
from pymodsim.simulator.vehicle_emulator import VehicleEmulator
from datetime import timedelta
from copy import copy
import numpy as np
from pymodsim.data_readers.reader_class import DataReader
from pymodsim.router import AbstractRouter
import typing as tp
from tqdm.auto import tqdm
from pymodsim.simulator.assignment_algorithm import nearest_neighbour
from pymodsim.config import Settings
from pymodsim.simulator.history import History
from multiprocessing.managers import SyncManager
from pymodsim.simulator.live_plot import LivePlot
import os
from pickle import load, dump
import types
from pathlib import Path


T = tp.TypeVar('T')


class NamedSet(tp.Set[T]):
    def __init__(self, s=(), name=None):
        super().__init__(s)
        self.name: str = name
        self.count: int = len(self)

    def __repr__(self):
        return "{0}: Count:{1} : {2}".format(self.name, self.count, str(list(self.__iter__())))


class SimulatorClass(object):
    
    def __init__(self, nr_cars: int, data_reader: DataReader, savecars: str = None, loadcars: str = None, **kwargs):
        """ Class for dynamic simulation

        :param nr_cars:         Number of cars to simulate
        :param data_reader:     Instance of DataReader class
        :param savecars:        Path for saving the generated cars
        :param loadcars:        Path for loading the cars
        """

        # Attributes for some stats and real time plots
        self.veh_total_distance = 0
        self.veh_service_distance = 0
        self.veh_total_empty_distance = 0
        self.simulation_delay = {0: 0.0}
        self.PickupDelay = []
        self.MeanDelay = [0.0]
        self.settings: Settings = data_reader.settings

        # Setup the data reader and get the cars
        self.data_reader: DataReader = data_reader
        self.router: AbstractRouter = self.data_reader.router
        self.reqs_gen = None
        self.scale_factor_generator: tp.Optional[tp.Generator] = None

        # Cars
        self.cars_list: tp.List[isc.MovingObject] = self.data_reader.generate_cars(nr_cars, savecars, loadcars)
        self.nr_cars = len(self.cars_list)
        self.carbyid: tp.Dict[str, isc.Car] = {car.ID: car for car in self.cars_list}

        # A car can belong to a single group at a time
        self.cars_at_visitables_dict: tp.Dict[str, NamedSet[isc.MovingObject]] = {}
        self.cars_repositioning: NamedSet[isc.MovingObject] = NamedSet(name="Repositioning Vehicles")
        self.cars_serving_customers: NamedSet[isc.MovingObject] = NamedSet(name="Vehicles Serving Customers")
        self.cars_idle_scheduled: NamedSet[isc.MovingObject] = NamedSet(self.cars_list, name="Idle Vehicles with Schedule")
        self.cars_idle: NamedSet[isc.MovingObject] = NamedSet(self.cars_list, name="Idle Vehicles")
        self.cars_idle.count = len(self.cars_list)

        # Dictionary for label of the set to which car belings to; initially all are Idle
        self.car_label_dict: tp.Dict[isc.MovingObject, NamedSet[isc.MovingObject]] = \
            {car: self.cars_idle for car in self.cars_list}
        # Record the travelled distance of each vehicle as dict
        car_key_dict: tp.Dict[str, float] = {car.ID: 0 for car in self.cars_list}
        self.car_miles = {"pickup_distance": car_key_dict.copy(),
                          "in_car_distance": car_key_dict.copy(),
                          "service_distance": car_key_dict.copy()}

        # Attributes for tracking visitable objects, these objects are "time-invariant" meaning they don't get expired
        # over time
        self.visitables_by_id: tp.Dict[str, isc.Visitable] = {}
        self.visitables_list: tp.List[isc.Visitable] = []

        # Attributes for keeping track of servable with time window objects
        self.stw_list: tp.List[isc.ServableTW] = []      # List to maintain order of generation
        self.stw_label_dict: tp.Dict[isc.ServableTW, NamedSet[isc.ServableTW]] = {}
        self.unscheduled_stw: NamedSet[isc.ServableTW] = NamedSet(name="unscheduled_stw")
        self.scheduled_stw: NamedSet[isc.ServableTW] = NamedSet(name="scheduled_stw")
        self.serving_stw: NamedSet[isc.ServableTW] = NamedSet(name="serving_stw")
        self.expired_stw: NamedSet[isc.ServableTW] = NamedSet(name="expired_stw")
        self.fulfilled_stw: NamedSet[isc.ServableTW] = NamedSet(name="fulfilled_stw")
        self.recent_expired_stw: tp.List[isc.ServableTW] = []
        # visitable object ID to car dictionary for which car fulfilled which request
        self.fulfilled_by: tp.Dict[str, isc.MovingObject] = {}
        # Number of times a ServableWithTimeWindows has been considered for optimization
        self.nrReqsOptims: tp.Dict[str: int] = {}
        self.nr_all_reqs = 0

        # A dictionary of all stationary objects by their ids
        self.stationary_by_ids: tp.Dict[str, isc.StationaryObject] = {}

        if self.settings.DEBUG is True:
            # list of tuples data_readers.reqnode object, the time of arrival and passenger count
            self.carhistroy: tp.Dict[isc.MovingObject, tp.List[tp.Tuple[isc.PathNode, float, int]]] = \
                {car: [] for car in self.cars_list}
            # cars original positions
            self.car_initial_positions: tp.Dict[isc.MovingObject, isc.Point] = {car: car.orig for car in self.cars_list}

        self.process_pool = None

        self.passed_timedelta: tp.Optional[timedelta] = None
        self.vehicle_emulator: tp.Optional[VehicleEmulator] = None

        self.logger = None
        self.results_folder = None
        self.history = None
        self._graph_process: tp.Optional[LivePlot] = None
        self._log_thread = None
        self._log_queue = None
        self._manager = None
        self.enddt_backup_path: tp.Optional[Path] = None

    def __getstate__(self):
        attrbs = vars(self).copy()
        generators = [x for x, value in attrbs.items() if isinstance(value, types.GeneratorType)]
        [attrbs.pop(x) for x in ["_manager", "_log_queue", "_log_thread", "_graph_process",
                                 "history", "logger", "process_pool", "settings"] + generators]
        return attrbs

    def __setstate__(self, state):
        for name, value in state.items():
            setattr(self, name, value)
        [setattr(self,x, None) for x in ["_manager", "_log_queue", "_log_thread", "_graph_process",
                                         "logger", "process_pool"]]
        self.settings = self.data_reader.settings

    @classmethod
    def from_pickled(cls, filepath: tp.Union[str, Path]):
        """ Creates the SimulatorClass from pickled file

        :param filepath:    File path of the pickled file
        """

        with open(Path(filepath), 'rb') as f:
            simulator: SimulatorClass = load(f)
        return simulator

    def _create_backup(self):
        """ Creates backup of the current state in the results folder
        :return: filename of the backup file
        """

        folder = Path(self.results_folder, "backups")
        if not folder.exists():
            folder.mkdir()
        filename = "simulator_backup_{}.pickle".format(self.data_reader.actual_time.strftime("%Y-%m-%d %H-%M-%S"))
        with open(Path(folder, filename), "wb") as f:
            dump(self, f)
        return Path(folder, filename)

    def run(self, results_folder=None, live_plot=True, live_plot_class=None, save_live_plot=False,
            save_frames=False, log_output=False, save_results=True, progress_bar=True, tqdm_position=0,
            pre_bar_text="", resume_state=False, simulate_cooldown=True):
        """ Basic method for starting the simulation

        :param results_folder:  full path where simulation results are to be written
        :param live_plot:       whether to plot realtime plot or not. A separate process is used for realtime plots.
                                Default is True
        :param save_live_plot:  save the live plot as movie instead of showing plots in realtime. The live_plot
                                parameter must be True for it to work
        :param save_frames:     save the frames of each time step and make movie from it at the end. It does not need
                                separate process, and thus, can be used in jupyter notebook as well
        :param live_plot_class: A customized subclass of LivePlot used for plotting data
        :param log_output:      If True, a log file in generated in the results folder. Default is False
        :param save_results:    Save results to files. Default is True.
        :param progress_bar:    print the tqdm based progress bar. Default True.
        :param resume_state:    Resume from last state
        :param simulate_cooldown:    Finish already assigned trips at the end when all requests have been generated.
                                This runs the simulation for more time than enddt to finish the lastly assigned trips.
        :return:                History class object for the simulation
        """

        try:
            if save_results is True:
                self.results_folder = self.__get_results_folder(results_folder)
            self.initialize_run(live_plot, save_live_plot, log_output, live_plot_class, save_frames, resume_state)
            self.__main_simulation_loop(progress_bar, save_frames, tqdm_position, pre_bar_text, resume_state,
                                        simulate_cooldown)
            self._end_simulation(live_plot, save_frames)
            if save_results is True:
                self.history.write_to_file(self.results_folder)
            return self.history
        except Exception as e:
            if logging.getLogger().hasHandlers() is True:
                logging.getLogger().exception("An error occurred while running the simulator")
            self._end_simulation(live_plot, save_frames)
            raise e

    def initialize_generator_functions(self, resume_state):
        self.reqs_gen = self.data_reader.dynamic_requests(skip_initial=True)
        if self.settings.window_time_matrix_scale != 0:
            self.scale_factor_generator = self.data_reader.calculate_time_factor(self.settings.window_time_matrix_scale)

    def initialize_run(self, live_plot, save_live_plot, log_output, live_plot_class, save_frames, resume_state):
        """ Initializes some of the settings dependent class attributes and required processes and threads """

        if self.results_folder is not None:
            self.settings.update({"startdt": str(self.data_reader.startdt),
                                  "enddt": str(self.data_reader.enddt),
                                  "data_file": self.data_reader.data_file_path,
                                  "nr_cars": self.nr_cars})
            self.settings.export_to_file(os.path.join(self.results_folder, "simulator_settings.yaml"))

        # Thread based logger
        if log_output is True:
            from pymodsim.simulator.logger_setup import listener_process, worker_configurer
            from threading import Thread
            from queue import Queue

            self._log_queue = Queue()
            self._log_thread = Thread(target=listener_process, args=(self._log_queue, self.results_folder,))
            self._log_thread.start()
            worker_configurer(self._log_queue)
        self.logger = logging.getLogger()

        self.history = History(self.settings.to_dict())
        plot_class = LivePlot if live_plot_class is None else live_plot_class
        if live_plot is True:
            SyncManager.register("History", History)
            self._manager = SyncManager()
            self._manager.start()
            self.history = self._manager.History(self.settings.as_dict())
            self._graph_process = plot_class(self.history, self.settings.as_dict(), self.results_folder,
                                             save_live_plot)
            self._graph_process.start()
        elif save_frames is True:
            self._graph_process = plot_class(self.history, self.settings.as_dict(), self.results_folder,
                                             save_live_plot)

        self.initialize_generator_functions(resume_state)

        # Read the fixed visitable objects that don't change during simulation
        if self.settings.stations_file_path and resume_state is False:
            self.visitables_list = self.data_reader.generate_visitable_objects(self.settings.stations_file_path)
            self.visitables_by_id: tp.Dict[str, isc.ServiceStation] = \
                {visitable.ID: visitable for visitable in self.visitables_list}
            visitable_dict = defaultdict(list)
            for v in self.visitables_list:
                visitable_dict[type(v).__name__].append(v)
            self.cars_at_visitables_dict = {s: NamedSet(name="Vehicles at {}".format(s)) for s in
                                            visitable_dict.keys()}
            self.stationary_by_ids.update(self.visitables_by_id.copy())
            self.history.set_attrb_by_name("fixed_visitables", dict(visitable_dict))

    def _end_simulation(self, live_plot, save_frames):
        logger = logging.getLogger()
        logger.info("Ending Simulation")
        self.history.set_attrb_by_name("stop_animation", True)
        self.history.set_attrb_by_name("summary", self.get_summary_dict())
        if self._log_thread is not None:
            logger.handlers = []
            # Send signal to logger for ending the while loop
            self._log_queue.put_nowait(None)
            self._log_thread.join()
        if live_plot is True:
            self._graph_process.join()
            self.history = self.history._getvalue()
            self._manager.shutdown()
        if save_frames is True:
            self._graph_process.make_movie_from_images(2)

    def get_summary_dict(self):
        return OrderedDict({"Total Vehicles": len(self.cars_list),
                            "Total Reqs": self.nr_all_reqs,
                            "Total Expired": self.expired_stw.count,
                            "Total Served": self.fulfilled_stw.count,
                            "Total Service Distance": self.veh_service_distance,
                            "Total Empty Distance": self.veh_total_empty_distance,
                            "Total Distance": self.veh_total_distance})

    def __main_simulation_loop(self, progress_bar, save_frames, tqdm_position, pre_bar_text, resume_state,
                               simulate_cooldown):

        def single_iteration():
            self.passed_timedelta += timedelta(seconds=self.settings.synchronous_batching_period)
            self.vehicle_emulator.update(self.passed_timedelta)
            if self.settings.window_time_matrix_scale != 0:
                self.refactor_time_matrix()
            end_simulation = self.update_odm_controller()
            # assign stationary vehicles to next locations
            if len(self.vehicle_emulator.stationary_cars) > 0:
                self.send_vehicles(self.vehicle_emulator.stationary_cars)
            self.update_real_time_data()
            return end_simulation

        if resume_state is False:
            self.logger.info("Starting new simulation with startdt: {}, enddt: {}".format(self.data_reader.startdt,
                                                                                          self.data_reader.enddt))
            self.passed_timedelta = timedelta()
            self.vehicle_emulator = VehicleEmulator(self.cars_list, self.passed_timedelta, self.router)
            total_iterations = int((self.data_reader.enddt - self.data_reader.startdt).total_seconds() /
                                   self.settings.synchronous_batching_period)
            # initial scaling of the travel time factor
            if self.settings.window_time_matrix_scale != 0:
                self.router.reset_time_factor()
                self.refactor_time_matrix()
        else:
            self.logger.info("Resuming simulation from time: {} to enddt: {}".format(self.data_reader.actual_time,
                                                                                     self.data_reader.enddt))
            total_iterations = int((self.data_reader.enddt - self.data_reader.actual_time).total_seconds() /
                                   self.settings.synchronous_batching_period)

        if progress_bar is True:
            with tqdm(total=total_iterations, position=tqdm_position) as pbar:
                while True:
                    end_simulation = single_iteration()
                    if save_frames is True:
                        self._graph_process.save_single_plot(self.data_reader.actual_time)
                    pbar.update(1)
                    pbar.set_description(str(pre_bar_text) + "_" + str(self.data_reader.actual_time))
                    post_dict = OrderedDict({"all_reqs": self.nr_all_reqs, "expired": self.expired_stw.count,
                                             "fulfilled": self.fulfilled_stw.count, "scheduled": len(self.scheduled_stw),
                                             "serving": len(self.serving_stw), "idle_cars": len(self.cars_idle)})
                    if self.settings.DEBUG is True:
                        post_dict.update({"sim_delay": np.mean(np.array(list(self.simulation_delay.values())))})
                    pbar.set_postfix(post_dict)
                    if self.enddt_backup_path is None and self.data_reader.actual_time >= self.data_reader.enddt:
                        self.enddt_backup_path = self._create_backup()
                        if simulate_cooldown is False:
                            break
                    if end_simulation is True:
                        break
        else:
            while True:
                if single_iteration() is True:
                    break

    def refactor_time_matrix(self):
        current_time = self.passed_timedelta.total_seconds()
        if current_time % self.settings.window_time_matrix_scale == 0:
            try:
                self.router.time_factor = next(self.scale_factor_generator)
                self.vehicle_emulator.update_time_factor(self.router.time_factor)
                # Update the schedules of currently assigned moving objects paths
                for car in self.cars_serving_customers.union(self.cars_repositioning):
                    reach_time = self.vehicle_emulator.car_routes[car].reach_time
                    car.serving_node.reach_time = reach_time
                    car.recalculate_path_reach_times(current_time, self.settings, self.router)
                self.history.add_info("time_factor", {"ActualTime": str(self.data_reader.actual_time),
                                                      "Router Time Factor": self.router.time_factor})

            except StopIteration:
                return

    def __get_results_folder(self, results_folder):
        if results_folder is None:
            results_folder = Path("Results", "result_" + str(self.nr_cars))
        # if summary file exist in folder, then use another folder
        i = 0
        while Path(results_folder, "summary.csv").exists():
            i += 1
            results_folder = results_folder.joinpath("_{}".format(i))
        results_folder = Path(results_folder)
        if not results_folder.exists():
            results_folder.mkdir(parents=True)
        return results_folder

    def __mark_servable_wtw(self, servables: tp.Set[isc.ServableTW],
                          new_set: NamedSet[isc.ServableTW]):
        """ function for marking the ServableWithTimeWindows to a specific new_set set. The new servables are by
        defaults are placed in self.unscheduled_stw
        """
        if len(servables) > 0:
            for servable in servables:
                old_set = self.stw_label_dict[servable]
                if old_set.name == self.serving_stw.name:
                    assert new_set.name not in {"scheduled_stw", "unscheduled_stw", "expired_stw"}
                old_set.remove(servable)
                old_set.count -= 1
                self.stw_label_dict[servable] = new_set
            if new_set.name == "expired_stw":
                self.recent_expired_stw.extend(sorted(servables, key=lambda x: x.orig_window[0]))
            new_set.count += len(servables)
            # If the debug mode is off, then don't keep references to expired and fulfilled requests
            if self.settings.DEBUG is False and new_set.name in {"expired_stw", "fulfilled_stw"}:
                # also remove reference to the request to free memory
                self.stw_list = [x for x in self.stw_list if x not in servables]
                for servable in servables:
                    del self.stationary_by_ids[servable.ID]
                    del self.stw_label_dict[servable]
            else:
                new_set.update(servables)

    def __mark_car(self, car: isc.MovingObject, new_set: NamedSet[isc.MovingObject]):
        """ function for marking list of cars to a new sets"""
        old_set = self.car_label_dict[car]
        old_set.count -= 1
        old_set.remove(car)
        self.car_label_dict[car] = new_set
        new_set.add(car)
        new_set.count += 1

    def __assign_moving_objects_to_set(self, cars: tp.Set[isc.MovingObject]):
        for car in cars:
            if car.serving_node is None:
                if len(car.path) > 0:
                    self.__mark_car(car, self.cars_idle_scheduled)
                else:
                    self.__mark_car(car, self.cars_idle)
            elif isinstance(car.serving_node.stationary_object, isc.Visitable):
                st_object = car.serving_node.stationary_object
                self.__mark_car(car, self.cars_at_visitables_dict[type(st_object).__name__])
            elif isinstance(car.serving_node.stationary_object, isc.CustomerRequest):
                self.__mark_car(car, self.cars_serving_customers)
            elif isinstance(car.serving_node.stationary_object, isc.RepositioningRequest):
                self.__mark_car(car, self.cars_repositioning)
            else:
                raise ValueError("Unknown stationary object type {} for "
                                 "marking the cars".format(type(car.serving_node.stationary_object)))

    def __test_customer_sets(self):
        " Assertion tests for the consistency of all the customer request sets"
        assert len(self.scheduled_stw) + len(self.unscheduled_stw) + len(self.serving_stw) + \
               self.fulfilled_stw.count + self.expired_stw.count == self.nr_all_reqs, \
            " ".join(["Count Failed: "] + [str(len(x)) for x in [self.scheduled_stw,
                                                                 self.unscheduled_stw,
                                                                 self.serving_stw,
                                                                 self.fulfilled_stw,
                                                                 self.expired_stw]] + str(self.nr_all_reqs))
        assert len(self.serving_stw.intersection(self.scheduled_stw)) == 0

        # test if the requests in car paths and scheduled set are same, and that some request is not scheduled for
        #   more than one car
        scheduled_stw = set()
        for car in self.cars_list:
            for node in car.path:
                node_object = node.stationary_object
                if isinstance(node_object, isc.ServableTW):
                    if node.geographical_point == node_object.locations[0]:
                        assert node_object not in scheduled_stw, "same request is scheduled for multiple cars. " \
                                                            "{}: {},  node={}".format(type(car), car, node)
                        scheduled_stw.add(node_object)
        assert len(scheduled_stw.difference(self.scheduled_stw)) == 0, "Some requests in vehicle path " \
                                                                  "not found in scheduled requests set"

        reqs = self.unscheduled_stw.union(self.scheduled_stw)
        for car in self.cars_list:
            if car.serving_node is not None:
                assert car.serving_node.stationary_object not in reqs, " Serving Request found in other sets"

    def merge_batch_solution(self, paths_dict: tp.Dict[isc.MovingObject, tp.List[isc.Point]],
                             matched_servables: tp.Set[isc.ServableTW],
                             unmatched_servables: tp.Set[isc.ServableTW],
                             next_loop_servables: tp.Set[isc.ServableTW]):
        """ Merges the batch optimization solution into the simulation

        :param paths_dict:              The batch solution dictionary with the duplicated cars as keys and list of
                                        points or path as values
        :param matched_servables:       The servables that have been assigned to a vehicle
        :param unmatched_servables:     The servables that could not be assigned to any vehicle
        :param next_loop_servables:     The servables whose decisions is postponed for now. They will not be marked as
                                        expired. Thus included in the next call to optimization
        """
        def unequal_intersection(set1: tp.Set[T], set2: tp.Set[T]) -> tp.Set[T]:
            """ Intersection with preference given to first set elements"""
            return {element for element in set1 if element in set2}

        current_time = self.passed_timedelta.total_seconds()
        original_moveables = {self.carbyid[car.ID] for car in paths_dict}

        # remove rescheduled servables from older cars
        rescheduled = unequal_intersection(matched_servables, self.scheduled_stw)
        # Also remove servables that were previously scheduled but now not assigned to anyone
        total_remove = unequal_intersection(set(unmatched_servables), self.scheduled_stw)
        rescheduled = total_remove.union(rescheduled)

        removal_dict: tp.Dict[isc.MovingObject, tp.Set[isc.ServableTW]] = defaultdict(set)
        for servable_copy in rescheduled:
            servable_original = self.stationary_by_ids[servable_copy.ID]
            assert isinstance(servable_original, (isc.Servable, isc.ServableTW)), "Non-Servable for rescheduling"
            if servable_copy.serving_moving_object != servable_original.serving_moving_object:
                original_car = self.carbyid[servable_original.serving_moving_object.ID]
                servable_original.serving_moving_object = None
                self.__mark_servable_wtw({servable_original}, self.unscheduled_stw)
                removal_dict[original_car].add(servable_original)

        # Only remove servables and recalculate for moving objects whose schedule will not be recalculated later
        for car in set(removal_dict.keys()).difference(original_moveables):
            car.remove_object_from_path(current_time, removal_dict[car], self.settings, self.router)

        # Recalculate the schedule
        servables_for_mark = set()
        fulfilled_serving = self.serving_stw.union(self.fulfilled_stw)

        for car, path_points in paths_dict.items():
            original_car = self.carbyid[car.ID]
            if original_car.serving_node is not None :
                serving_object = {original_car.serving_node.stationary_object}
            else:
                serving_object = set()
            # remove the points that have been visited already, except the already serving one for the
            #   current moving object

            new_path = [point for point in path_points if self.stationary_by_ids[point.associated_object_id]
                        not in fulfilled_serving.difference(serving_object)]
            # change the node's stationary objects with the original objects
            for i, point in enumerate(new_path):
                stationary_object = self.stationary_by_ids[point.associated_object_id]
                if isinstance(stationary_object, (isc.ServableTW, isc.Servable)):
                    stationary_object.serving_moving_object = original_car
                    servables_for_mark.add(stationary_object)
                original_car.add_to_path(stationary_object, 0, point)
            servables_for_mark.difference_update(serving_object)
            assert len(original_car.path) == len(set(original_car.path)), "duplicate nodes found in new " \
                                                                          "path = {}".format(new_path)
            original_car.recalculate_path_reach_times(current_time, self.settings, self.router)

        self.__assign_moving_objects_to_set(original_moveables.union(removal_dict.keys()))
        self.__mark_servable_wtw(servables_for_mark, self.scheduled_stw)
        unmatched_expired = set()
        # Mark requests that have been considered maximum allowed number of times as expired
        if self.settings.max_times_unmat_opt is not None:
            unmatched_expired = {self.stationary_by_ids[r.ID] for r in unmatched_servables
                                 if self.nrReqsOptims[r.ID] >= self.settings.max_times_unmat_opt}
        unmatched_expired = {r for r in unmatched_expired if r not in next_loop_servables}
        self.__mark_servable_wtw(unmatched_expired, self.expired_stw)
        self.__test_customer_sets()

    def send_vehicles(self, moving_objects: tp.Set[isc.MovingObject]):
        current_time = self.passed_timedelta.total_seconds()
        cars_send_dict = {}
        for car in moving_objects:
            lastnode = car.serving_node
            node_reach_time = self.vehicle_emulator.point_reached_time[car]
            covered_distance = 0

            # First store the necessary updates from last node served
            if lastnode is not None and str(lastnode.geographical_point) not in self.simulation_delay:
                self.simulation_delay.update({str(lastnode.geographical_point): node_reach_time - lastnode.reach_time})
                if self.settings.DEBUG:
                    self.carhistroy[car].append((lastnode, node_reach_time, car.nr_current_onboard))
                if abs(node_reach_time - lastnode.reach_time) > 1:
                    self.logger.error("-ve sim delay, timedelta: " + str(self.passed_timedelta) + " car: " +
                                      str(car) + " point: " + str(lastnode) + " simulation_delay: " +
                                      str(node_reach_time - lastnode.reach_time))
                if isinstance(lastnode.stationary_object, isc.CustomerRequest):
                    if lastnode.geographical_point == lastnode.stationary_object.orig:
                        self.PickupDelay.append(max(0, node_reach_time - lastnode.stationary_object.orig_window[0]))
                    else:
                        self.__mark_servable_wtw({lastnode.stationary_object}, self.fulfilled_stw)

                _, covered_distance = self.vehicle_emulator.get_time_distance_route(car)

                # Record history
                self.history.add_info("trip_info",
                                      {"CurrentTime": current_time,
                                       "ActualTime": str(self.data_reader.actual_time),
                                       "Arrival Time": node_reach_time,
                                       "Previous Estimated Arrival Time": lastnode.reach_time,
                                       "moving object": car.ID,
                                       "moving object type": type(car).__name__,
                                       "stationary object": str(lastnode.stationary_object),
                                       "stationary object type": type(lastnode.stationary_object).__name__,
                                       "nr of times optimized": self.nrReqsOptims.get(lastnode.stationary_object.ID,
                                                                                      None)
                                       })
            next_point, move_time = car.reached_serving_node(current_time, node_reach_time, covered_distance,
                                                             self.settings)
            if next_point is not None:
                self.logger.info("timedelta: {} sending {} from {} to {}".format(self.passed_timedelta, car, car.orig,
                                                                                 car.serving_node))
                cars_send_dict.update({car: (next_point, move_time)})
                if isinstance(car.serving_node.stationary_object, isc.ServableTW):
                    self.__mark_servable_wtw({car.serving_node.stationary_object}, self.serving_stw)
        self.__test_customer_sets()
        self.__assign_moving_objects_to_set(moving_objects)

        # Send vehicles
        if len(cars_send_dict) > 0:
            sendcars, destination_and_movetime = list(zip(*cars_send_dict.items()))
            destinations, move_times = zip(*destination_and_movetime)
            self.vehicle_emulator.send_cars_to_points(sendcars, destinations, move_times)

    def update_real_time_data(self):
        # Update data for the plots after every self.settings.plotrate seconds
        timedelta_sec = self.passed_timedelta.total_seconds()

        # Update the total vehicle miles
        self.veh_total_distance = sum(car.total_distance_covered for car in self.carbyid.values())
        self.veh_service_distance = sum(car.service_distance_covered for car in self.carbyid.values())
        self.veh_total_empty_distance = sum(car.empty_distance_covered for car in self.carbyid.values())

        # Record the data for later plots
        real_time_info = {"TimeDelta": str(self.passed_timedelta),
                          "ActualTime": str(self.data_reader.actual_time),
                          "ScheduledReqs": len(self.scheduled_stw),
                          "ServingReqs": len(self.serving_stw),
                          "ExpiredReqs": self.expired_stw.count,
                          "UnscheduledReqs": len(self.unscheduled_stw),
                          "TotalReqs": self.nr_all_reqs,
                          "ServiceVehicleDistance": self.veh_service_distance,
                          "TotalEmptyDistance": self.veh_total_empty_distance,
                          "TotalVehicleDistance": self.veh_total_distance,
                          "PickUpDelay": np.mean(np.array(self.PickupDelay)) / 60 if self.PickupDelay else 0.0
                          }
        real_time_info.update({"{}Vehicles".format(key): len(named_set) for key, named_set in self.cars_at_visitables_dict.items()})
        self.history.add_info("realtime_info", real_time_info)

        # Reset the accumulated delays
        self.PickupDelay = []

        # Record vehicle miles
        self.history.set_attrb_by_name("vehicle_miles", self.car_miles)

        # Update vehicle locations
        all_vehicle_sets = [self.cars_idle, self.cars_serving_customers, self.cars_repositioning] + \
                           list(self.cars_at_visitables_dict.values())
        vehicle_positions = {car_set.name: [] for car_set in all_vehicle_sets}

        [vehicle_positions[car_set.name].append(self.vehicle_emulator.car_locations[car].latlon)
            for car, car_set in self.car_label_dict.items()]

        self.history.set_attrb_by_name("vehicle_positions", vehicle_positions)
        # recently expired requests in last 5 minutes
        self.recent_expired_stw = [req for req in self.recent_expired_stw if req.orig_window[0] > timedelta_sec - 5*60]
        expired_pos = [req.orig.latlon for req in self.recent_expired_stw]
        self.history.set_attrb_by_name("recent_expired_requests", expired_pos)

    def __create_optimization_problem(self) -> (tp.List[isc.MovingObject],
                                                tp.List[isc.ServableTW],
                                                tp.Dict[str, tp.List[isc.PathNode]],
                                                dict):
        current_time = self.passed_timedelta.total_seconds()
        reqs = self.unscheduled_stw.union(self.scheduled_stw)
        submitted_reqs = reqs.copy()

        removed_nodes_dict = defaultdict(tp.List[isc.PathNode])
        submitted_cars = []
        for car in self.cars_list:
            car_copy, removed_nodes = car.get_optimization_copy(current_time, self.nrReqsOptims, self.settings)
            if car_copy is not None:
                if len(removed_nodes) > 0:
                    removed_nodes_dict[car.ID] = removed_nodes
                submitted_cars.append(car_copy)
        removed_objects = set().union(*removed_nodes_dict.values())
        removed_objects = {r.stationary_object for r in removed_objects}
        submitted_reqs.difference_update(removed_objects)
        for req in submitted_reqs:
            self.nrReqsOptims[req.ID] += 1
        # Copy the requests and keep the original order of generation
        submitted_reqs = [copy(r) for r in self.stw_list if r in submitted_reqs]

        matching_stats = OrderedDict({"ActualTime": str(self.data_reader.actual_time),
                                      "NrCars": len(submitted_cars), "NrReqs": len(reqs),
                                      "NrOriginalFollowups": len(self.scheduled_stw),
                                      "Removed Scheduled Reqs": len(removed_objects),
                                      "Total Considered Reqs": len(submitted_reqs)})

        return submitted_cars, submitted_reqs, removed_nodes_dict, matching_stats

    def __call_optimization(self):

        if len(self.unscheduled_stw) > 0:
            submitted_cars, submitted_reqs, removed_nodes_dict, matching_stats = self.__create_optimization_problem()

            if len(submitted_reqs) > 0:
                t1 = default_timer()
                visitables = self.visitables_list.copy()
                time_dict, distance_dict = self.router.calculate_dict(submitted_cars, submitted_reqs + visitables,
                                                                      factored_time=True)
                time_matrix_calc_time = default_timer() - t1
                paths_dict, skipped, info_dict = self.solve_assignment_problem(submitted_cars, submitted_reqs,
                                                                               visitables, time_dict, distance_dict)
                matched = set()
                submitted_by_id = {r.ID: r for r in submitted_reqs}
                for car, point_list in paths_dict.items():
                    for pt in point_list:
                        if isinstance(self.stationary_by_ids[pt.associated_object_id], (isc.Servable, isc.ServableTW)):
                            submitted_servable = submitted_by_id[pt.associated_object_id]
                            submitted_servable.serving_moving_object = car
                            matched.add(submitted_servable)

                matching_stats.update({"NrMatched": len(matched), "TMMatrix": time_matrix_calc_time,
                                       "TotalTime": default_timer() - t1})
                matching_stats.update(info_dict)
                self.logger.info("Assignment Stats \t " + str(matching_stats))
                self.merge_batch_solution(paths_dict, matched, set(submitted_reqs).difference(matched), skipped)

            self.history.add_info("matching_stats", matching_stats, append_to_not_present=True)

    def solve_assignment_problem(self, cars: tp.List[isc.MovingObject],
                                 requests: tp.List[isc.ServableTW],
                                 visitables: tp.List[isc.Visitable],
                                 time_dict: dict, distance_dict: dict) -> (tp.Dict[isc.MovingObject,
                                                                                   tp.List[isc.Point]],
                                                                           tp.Set[isc.ServableTW],
                                                                           dict):
        """ Method for solving the assignment problem

        Any subclass that wants to implement its own strategy for the assignment problem should override this method.
        By default it uses the simplest nearest neighbor policy

        :param cars: list of moving objects
        :param requests: list of time bounded servable objects
        :param visitables: list of visitable stationary objects
        :param time_dict: travel time dictionary in seconds with point.key as keys
        :param distance_dict: travel distances in meters with point.key as keys
        :return:
                - paths_dict -              Dictionary of lists of Points to visit with MovingObjects as key
                - unmatched_requests -      Set of unmatched servable objects
                - skip_requests -           Set of unmatched servable objects whose decisions is skipped for now.
                                            These servables will be included in the next call
                - info_dict -               Dictionary of any statistical information that should be stored for analysis
        """

        paths_dict = nearest_neighbour(cars, requests, time_dict, self.settings)
        return paths_dict, set(), {}

    def update_odm_controller(self):

        current_time = self.passed_timedelta.total_seconds()
        self.data_reader.update_time(self.passed_timedelta)

        new_reqs = []
        try:
            new_reqs = next(self.reqs_gen)
        except (StopIteration, RuntimeError):
            if len( self.scheduled_stw) == 0 and len(self.serving_stw) == 0:
                # Stop the simulation
                return True

        if len(new_reqs) > 0:
            self.stw_list.extend(new_reqs)
            self.stationary_by_ids.update({r.ID: r for r in new_reqs})
            self.nr_all_reqs += len(new_reqs)
            self.nrReqsOptims.update({r.ID: 0 for r in new_reqs})
            self.unscheduled_stw.update(new_reqs)
            self.unscheduled_stw.count += len(new_reqs)
            self.stw_label_dict.update({r: self.unscheduled_stw for r in new_reqs})
            for r in new_reqs:
                self.logger.info("Request generated: ID: {}, orig_window: {}, "
                                 "dest_windows: {}".format(r, r.orig_window, r.dest_window))

        # Mark the requests which are not scheduled and have expired
        expired_stws = {r for r in self.unscheduled_stw if current_time > r.time_windows[0][1]}
        self.__mark_servable_wtw(expired_stws, self.expired_stw)

        # call optimization
        self.__call_optimization()

        return False
