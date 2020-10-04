from pymodsim.data_readers.simulation_classes import *
from logging import getLogger


class Car(MovingObject):
    __slots__ = ["service_distance_covered", "empty_distance_covered", "total_distance_covered"]
    max_capacity = 4

    def __init__(self, orig: Point, treq: float, key: str):
        self.empty_distance_covered: float = 0
        self.total_distance_covered: float = 0
        self.service_distance_covered: float = 0
        super().__init__(orig, treq, key)

    def reached_serving_node(self, current_time, time_of_arrival, covered_distance, settings):
        if self.nr_current_onboard == 0:
            self.empty_distance_covered += covered_distance
        if self.serving_node is not None and isinstance(self.serving_node.stationary_object,
                                                        (RepositioningRequest, ServiceStation)):
            self.service_distance_covered += covered_distance
        self.total_distance_covered += covered_distance
        return super().reached_serving_node(current_time, time_of_arrival, covered_distance, settings)

    def get_optimization_copy(self, current_time, optimization_counts, settings):
        car = self.__deepcopy__()
        removed_nodes = []
        if car.serving_node is None:
            car.treq = current_time
        else:
            car.orig = car.serving_node.geographical_point
            car.treq = car.serving_node.reach_time
            serving_object = car.serving_node.stationary_object
            if isinstance(serving_object, CustomerRequest):
                if car.serving_node.geographical_point == serving_object.orig:
                    # Ride Hailing scenario, make sure that the request is dropped
                    assert car.path[0].geographical_point == serving_object.dest
                    removed_nodes.append(car.path[0])
                    car.orig = car.path[0].geographical_point
                    car.treq = car.path[0].reach_time
                    car.path = car.path[1:]
            else:
                car.treq = serving_object.estimate_leave_time(car, current_time, car.serving_node.reach_time,
                                                              car.serving_node.geographical_point, settings)

        # Remove PairedPointsStationaryObject from optimization that have already been optimized
        # "max_times_reqs_opt" times
        if settings.max_times_schreqs_opt is not None:
            for i, node in reversed(list(enumerate(car.path))):
                stationary_object = node.stationary_object
                if optimization_counts.get(stationary_object.ID, 0) >= settings.max_times_schreqs_opt:
                    removed_nodes.extend(car.path[:i + 1])
                    car.orig = node.geographical_point
                    car.treq = node.reach_time
                    break
        car.path = []
        car.serving_node = None
        return car, removed_nodes


class ElectricCar(Car):
    __slots__ = ["charge_percentage", "price_per_kWh", "max_charge_capacity", "max_battery_capacity"]

    def __init__(self, orig: Point, treq: float, key: str, initial_price_per_kWh: float, settings: config.Settings):
        self.charge_percentage: float = 100
        self.price_per_kWh: float = initial_price_per_kWh
        self.max_charge_capacity: float = settings.max_charge_capacity
        self.max_battery_capacity: float = settings.max_battery_capacity
        super().__init__(orig, treq, key)

    def consume_energy(self, covered_distance, settings):
        battery_discharge_rate = settings.max_battery_capacity / settings.vehicle_range_battery
        percent_drop = 100 * battery_discharge_rate * covered_distance / self.max_battery_capacity
        self.charge_percentage -= percent_drop

    def get_optimization_copy(self, current_time, optimization_counts, settings):
        if self.serving_node is not None and isinstance(self.serving_node.stationary_object, ElectricStation):
            # If the electric car is at an electric station and the charge is
            #   less than st.minimim_charge_for_leaving_station then don't send submit the vehicle for optimization

            if self.charge_percentage < settings.minimim_charge_for_leaving_station:
                return None, None
        return super().get_optimization_copy(current_time, optimization_counts, settings)

    def reached_serving_node(self, current_time, time_of_arrival, covered_distance, settings):
        last_node = self.serving_node
        next_point, move_time = super().reached_serving_node(current_time, time_of_arrival, covered_distance)
        if last_node is not None and isinstance(last_node.stationary_object, ElectricStation):
            # If the current node is no more electric station, then remove the car from Electric Station
            if self.serving_node is None or (self.serving_node is not None
                                             and not isinstance(self.serving_node.stationary_object, ServiceStation)):
                last_node.stationary_object.remove_object_from_charging_set(self, settings)
        return next_point, move_time


class GasCar(Car):
    __slots__ = ["fuel_percentage", "max_fuel_capacity"]

    def __init__(self, orig: Point, treq: float, key: str, settings: config.Settings):
        self.fuel_percentage: float = 100
        self.max_fuel_capacity: float = settings.vehicle_max_capacity_gas
        super().__init__(orig, treq, key)

    def consume_energy(self, covered_distance, settings):
        percent_drop = 100 * (covered_distance / settings.vehicle_km_per_litre) / settings.vehicle_max_capacity_gas
        self.fuel_percentage -= percent_drop


class ServiceStation(SinglePointVisitable):
    __slots__ = ["nr_vehicle_slots", "vehicles_present"]

    def __init__(self, location: Point, key: str, nr_vehicle_slots: int):
        self.nr_vehicle_slots: int = nr_vehicle_slots
        self.vehicles_present: Set[MovingObject] = set()
        super().__init__(location, key)


class ElectricStation(ServiceStation):
    __slots__ = ["max_charge_rate", "price"]

    def __init__(self, location: Point, key: str, nr_vehicle_slots: int, price: float, max_charge_rate: float):
        self.nr_vehicle_slots: int = nr_vehicle_slots
        self.price: float = price
        self.max_charge_rate: float = max_charge_rate
        super().__init__(location, key, nr_vehicle_slots)

    def __recharge(self, moving_object: ElectricCar, delta_time: float):
        percent_increase = min(moving_object.max_charge_capacity, self.max_charge_rate) * delta_time / 60
        percent_increase = 100 * percent_increase / moving_object.max_battery_capacity
        moving_object.charge_percentage = min(100.0, moving_object.charge_percentage + percent_increase)

    def estimate_minimum_charge_time(self, moving_object: ElectricCar, settings: config.Settings) -> float:
        """ returns the minimum time remaining (minutes) to reach minimim_charge_for_leaving_station"""
        remaining_percentage = settings.minimim_charge_for_leaving_station - moving_object.charge_percentage
        if remaining_percentage <= 0:
            return 0.0
        else:
            remaining_battery = remaining_percentage * moving_object.max_battery_capacity / 100
            charge_rate = min(moving_object.max_charge_capacity, self.max_charge_rate)
            remaining_time = remaining_battery / charge_rate * 60
            return remaining_time

    def estimate_leave_time(self, moving_object: ElectricCar, current_time, time_of_arrival, point_reached, settings):
        remaining_time = self.estimate_minimum_charge_time(moving_object)
        return max(current_time, time_of_arrival) + remaining_time

    def reached_location(self, moving_object: ElectricCar, current_time, time_of_arrival, settings):
        assert isinstance(moving_object, ElectricCar), "A {} {} came at ElectricStation: {}".format(type(moving_object),
                                                                                                    moving_object,
                                                                                                    self)
        if moving_object not in self.vehicles_present:
            assert moving_object.charge_percentage <= \
                   settings.min_fuel_charge_percentage, "Vehicle {} with charge {}% came to ElectricStation {} with " \
                                                        "charge more than minimum limit for " \
                                                        "recharge".format(moving_object,
                                                                          moving_object.charge_percentage, self)
        delta_time = (current_time - time_of_arrival) % settings.synchronous_batching_period
        self.__recharge(moving_object, delta_time)
        self.vehicles_present.add(moving_object)
        if moving_object.charge_percentage >= settings.minimim_charge_for_leaving_station:
            return current_time
        else:
            return current_time + self.estimate_minimum_charge_time(moving_object)

    def remove_object_from_charging_set(self, moving_object: ElectricCar, settings: config.Settings):
        assert moving_object.charge_percentage >= settings.minimim_charge_for_leaving_station
        self.vehicles_present.remove(moving_object)


class GasStation(ServiceStation):
    __slots__ = ["max_charge_rate", "price"]

    def __init__(self, location: Point, key: str, nr_vehicle_slots: int, price: float, max_charge_rate: float):
        self.nr_vehicle_slots = nr_vehicle_slots
        self.price = price
        self.max_charge_rate: float = max_charge_rate
        super().__init__(location, key, nr_vehicle_slots)

    def __recharge(self, moving_object: GasCar):
        moving_object.fuel_percentage = 100

    def estimate_leave_time(self, moving_object: GasCar, current_time, time_of_arrival, point_reached, settings):
        assert moving_object.fuel_percentage <= \
               settings.min_fuel_charge_percentage, "Vehicle {} with gas {:.2f}% came to GasStation {} with charge " \
                                                    "more than minimum limit for " \
                                                    "recharge".format(moving_object, moving_object.fuel_percentage,
                                                                      self)
        self.__recharge(moving_object)
        return time_of_arrival + settings.gas_station_service_time

    def reached_location(self, moving_object: GasCar, current_time, time_of_arrival, settings):
        assert isinstance(moving_object, GasCar), "A {} {} came at GasStation: {}".format(type(moving_object),
                                                                                          moving_object,
                                                                                          self)
        assert moving_object.fuel_percentage <= \
               settings.min_fuel_charge_percentage, "Vehicle {} with gas {:.2f}% came to GasStation {} with charge " \
                                                    "more than minimum limit for " \
                                                    "recharge".format(moving_object, moving_object.fuel_percentage,
                                                                      self)
        leave_time = time_of_arrival + settings.gas_station_service_time
        self.vehicles_present.add(moving_object)
        if current_time >= leave_time:
            self.__recharge(moving_object)
            self.vehicles_present.remove(moving_object)
        return leave_time


class CustomerRequest(PairedPointsServableTW):
    __slots__ = ["nr_passengers"]

    def __init__(self, orig: Point, dest: Point, key: str, orig_window: tuple, dest_window: tuple,
                 nr_passengers: int):
        self.nr_passengers: float = nr_passengers
        super().__init__(orig, dest, key, orig_window, dest_window)

    def reached_orig(self, moving_object, current_time, time_of_arrival, settings):
        # wait for customer to come
        pickup_violation = time_of_arrival - self.orig_window[1]
        if pickup_violation > 0 and settings.window_time_matrix_scale is None:
            getLogger().warning("Origin window violated with difference of {:.2f}s for customer:{} and "
                                "{}: {}".format(pickup_violation, time_of_arrival > self.orig_window[1], self,
                                                type(moving_object).__name__, moving_object))
        moving_object.nr_current_onboard += self.nr_passengers
        return max(time_of_arrival, self.orig_window[0])

    def reached_dest(self, moving_object, current_time, time_of_arrival, settings):
        moving_object.nr_current_onboard -= self.nr_passengers
        # the car can immediately leave the point
        return time_of_arrival


class RepositioningRequest(SinglePointVisitable):

    def reached_location(self, moving_object, current_time, time_of_arrival, settings):
        assert moving_object.nr_current_onboard == 0, " A non empty {}: {} has been " \
                                                      "repositioned".format(type(moving_object), moving_object)
        return time_of_arrival
