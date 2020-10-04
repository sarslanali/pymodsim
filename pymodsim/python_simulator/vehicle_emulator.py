import numpy as np
from pymodsim.data_readers.implemented_simulation_classes import Point, MovingObject
from typing import List
from pymodsim.router import RouteElement
import typing as tp
from datetime import timedelta
from pymodsim.router import AbstractRouter
from shapely.geometry import MultiLineString


class Route:
    __slots__ = ["current_position", "_current_speed", "distance_covered_so_far", "serving_step_index",
                 "complete_geometry", "route_elements", "current_time", "initial_time", "reach_time",
                 "cum_distance", "destination", "time_factor"]

    def __init__(self, steps_list: List[RouteElement], start_time: float, destination: Point, time_factor: float):

        initial_lon, initial_lat = steps_list[0].geometry.coords[0]
        self.current_position: Point = Point(initial_lat, initial_lon, "on_way_to_" + destination.key)
        self._current_speed: float = steps_list[0].speed
        self.distance_covered_so_far: float = 0.0
        self.serving_step_index: int = 0
        self.complete_geometry: MultiLineString = MultiLineString([s.geometry.coords for s in steps_list])
        self.route_elements: List[RouteElement] = steps_list
        self.current_time: float = start_time
        self.initial_time: float = start_time
        self.reach_time: float = start_time + sum([r.duration * time_factor for r in steps_list])
        # cumulative distance at the beginning of the route leg
        self.cum_distance: np.array = np.cumsum(np.array([0] + [r.distance for r in steps_list]))
        self.destination: Point = destination
        self.time_factor: float = time_factor

    def __repr__(self):
        return "Destination: {}, reach_time: {:.2f}, speed: {:.2f}, " \
               "time_factor: {:.2f}".format(self.destination, self.reach_time, self.current_speed, self.time_factor)

    @property
    def current_speed(self):
        return self._current_speed / self.time_factor

    @property
    def total_distance(self):
        return self.cum_distance[-1]

    def __interpolate_position(self, current_time):
        """ Interpolates the geographical location of the car based on current_time

        :param current_time: current simulated time
        """

        serv_inx = self.serving_step_index
        delta_time = current_time - self.current_time
        delta_distance = delta_time * self._current_speed / self.time_factor
        distance_covered_so_far = self.distance_covered_so_far + delta_distance

        # compute the next route step index based on the distance covered so far
        point_index = np.searchsorted(self.cum_distance, distance_covered_so_far) - 1

        if len(self.route_elements) == 1 and \
            (point_index + 1 == len(self.cum_distance) or self.route_elements[0].duration == 0
            or self.route_elements[0].distance == 0):
            # If it is single element route and one of the following condition is true;
            # 1) means we have reached the destination as covered distance is greater than sum of distances
            # 2) it is zero length or duration route, so we reached the destination
            return None, None, None
        else:
            if point_index != serv_inx:
                # If we are not on the same leg, then consider the speeds of both current and next legs to
                # calculate the next point.
                # We want to skip all the legs that would have been traversed during the time step.
                assert distance_covered_so_far > self.cum_distance[serv_inx+1]
                next_inx = serv_inx + 1
                last_speed = self._current_speed / self.time_factor
                while distance_covered_so_far > self.cum_distance[next_inx]:
                    if next_inx == len(self.route_elements):
                        # The whole route has been travelled
                        return None, None, None
                    remaining_distance = distance_covered_so_far - self.cum_distance[next_inx]
                    remaining_time_for_next = remaining_distance / last_speed
                    next_speed = self.route_elements[next_inx].speed / self.time_factor
                    delta_distance = next_speed * remaining_time_for_next
                    distance_covered_so_far = self.cum_distance[next_inx] + delta_distance
                    last_speed = next_speed
                    next_inx += 1
                serv_inx = next_inx - 1

            # calculate the fraction of the current leg that has been covered so far
            fraction = (distance_covered_so_far - self.cum_distance[serv_inx]) / self.route_elements[serv_inx].distance
            shapely_point = self.route_elements[serv_inx].geometry.interpolate(fraction, normalized=True)
            lat, lon = shapely_point.y, shapely_point.x
            return (lat, lon), distance_covered_so_far, serv_inx

    def move(self, current_time):
        """ Moves the car location and updates the serving step index according
        to the current_time

        :param current_time: current simulation time in minutes
        :return:
                - new_point - the interpolated geographical point
                - delta_distance - the distance covered (m) in the current call of the function
        """
        new_point, acc_dist, index = self.__interpolate_position(current_time)
        if new_point is not None:
            self.serving_step_index = index
            delta_distance = acc_dist - self.distance_covered_so_far
            self.distance_covered_so_far = acc_dist
            self.current_position = Point(new_point[0], new_point[1], self.destination.key)
            self._current_speed = self.route_elements[index].distance / self.route_elements[index].duration
        else:
            # This means that the car has reached its destination
            self.serving_step_index = None
            delta_distance = self.cum_distance[-1] - self.distance_covered_so_far
            self.distance_covered_so_far = self.cum_distance[-1]
            self.current_position = self.destination
            self._current_speed = 0
        self.current_time = current_time

        return self.current_position, delta_distance

    def update_time_factor(self, current_time: float, new_time_factor: float):
        """ Updates the time factor and calculates updated reach time """

        # calculate the new reach time
        if self._current_speed != 0:
            current_leg_distance = self.cum_distance[self.serving_step_index + 1] - self.distance_covered_so_far
            reach_time = current_time + current_leg_distance / self._current_speed * new_time_factor
            for route_element in self.route_elements[self.serving_step_index+1:]:
                reach_time += route_element.duration * new_time_factor
            self.reach_time = reach_time
        self.time_factor = new_time_factor


class VehicleEmulator:

    def __init__(self, cars: tp.List[MovingObject], current_time: timedelta, router: AbstractRouter):
        """ Class for emulating the movement of vehicles """
        self.cars: tp.List[MovingObject] = cars
        self.car_locations: tp.Dict[MovingObject, Point] = {car: car.orig for car in cars}
        self.moving_cars: tp.Set[MovingObject] = set()
        self.stationary_cars: tp.Set[MovingObject] = set(cars)
        # Total distance covered by vehicles so far in meters
        self.total_cars_distance_travelled: tp.Dict[MovingObject, float] = {car: 0.0 for car in cars}
        self.current_time: float = current_time.total_seconds()
        self.point_reached_time: tp.Dict[MovingObject, float] = {car: self.current_time for car in cars}
        self.last_point_left_time: tp.Dict[MovingObject, float] = {car: None for car in cars}
        self.car_routes: tp.Dict[MovingObject, Route] = {}
        self.router: AbstractRouter = router
        self.route_time_factor = router.time_factor

    def update(self, current_time: timedelta):
        self.current_time = current_time.total_seconds()
        self.__move_cars()
        assert len(self.moving_cars.intersection(self.stationary_cars)) == 0, "A car can be either moving or stationary"

    def update_time_factor(self, time_factor):
        """ Update the remaining part of moving object routes according to new time factor """
        for car in self.moving_cars:
            self.car_routes[car].update_time_factor(self.current_time, time_factor)
        self.route_time_factor = time_factor

    def __move_cars(self):
        cars_reached = []
        for car in self.moving_cars:
            car_route = self.car_routes[car]
            new_position, delta_distance = car_route.move(self.current_time)
            self.total_cars_distance_travelled[car] += delta_distance
            self.car_locations[car] = new_position
            if car_route.serving_step_index is None:
                # car has reached it's destination
                self.stationary_cars.add(car)
                cars_reached.append(car)
                self.point_reached_time[car] = car_route.reach_time
        self.moving_cars.difference_update(cars_reached)

    def send_cars_to_points(self, cars: tp.List[MovingObject], points: tp.List[Point],
                            movement_times: tp.Optional[tp.List[float]] = None):
        """ Assigns the stationary cars to new destinations

        :param cars: list of stationary cars
        :param points: list of destination points of type data_readers.point
        :param movement_times: list of times (minutes) when the vehicle leave the last point.
                                It is used mainly for synchronous simulation
        :param duration_dict: if given, the vehicles travel times are emulated based on a straight line
        :param distance_dict: if given, the vehicles travel distances are emulated based on a straight line
        """
        assert len(set(cars).intersection(self.moving_cars)) == 0, "Assigning new" \
                                        " destination to already moving car"
        car_positions = [self.car_locations[car] for car in cars]
        steps = self.router.route_steps(list(zip(car_positions, points)))
        if movement_times is None: movement_times = len(cars) * [self.current_time]

        for i, (car, origin, dest, start_time) in enumerate(zip(cars, car_positions, points, movement_times)):
            route = Route(steps[i], start_time, dest, self.route_time_factor)
            self.car_routes.update({car: route})
            self.stationary_cars.remove(car)
            self.moving_cars.add(car)
            self.car_locations[car] = route.current_position
            self.last_point_left_time[car] = start_time

    def get_stationary_vehicle_ids(self) -> tp.List[str]:
        return [car.ID for car in self.stationary_cars]

    def get_car_positions(self, cars):
        return [self.car_locations[car] for car in cars]

    def get_time_distance_route(self, car):
        """ returns the travel time and distance in km for the current route if the car is moving, or of the last route
        if the car is stationary

        :param car: car object
        :return: tuple of:
                    - time in seconds
                    - distance in meters
        """
        if car in self.car_routes:
            route = self.car_routes[car]
            return route.reach_time, route.distance_covered_so_far
