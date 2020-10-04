from datetime import timedelta
from numpy import array
from os.path import join
from multiprocessing import Process
from pymodsim.python_simulator.history import History
import matplotlib.animation as animation
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from typing import List, Optional, Tuple
from geopandas import read_file as georeader
from geopandas import geodataframe


class LivePlot(Process):

    def __init__(self, shared_history: History, settings_dict: dict, animation_folder: str,
                 save_animation:bool = False):
        """ Class for plotting real time information

        :param shared_history: a shared history object with the main process
        :param settings_dict: dictionary of settings parameters
        :param animation_folder: full path of the folder for saving the animation
        :param save_animation: If true the real time plots are saved in a file instead of showing in a window
        """

        super().__init__()
        self.shared_history: History = shared_history
        self.settings_dict: dict = settings_dict
        self.animation_folder: str = animation_folder
        self.save_animation: bool = save_animation
        self.fig, self.grid_spec, self.axes = None, None, None
        self.map_geo_df: Optional[geodataframe] = None
        if settings_dict["SUBZONE_FILEPATH"]:
            self.map_geo_df = georeader(settings_dict["SUBZONE_FILEPATH"])

    def generate_plot_axes(self):
        fig = plt.figure(1, figsize=self.settings_dict["REAL_TIME_FIG_SIZE"])
        gs = gridspec.GridSpec(3, 3, figure=fig)
        gs.update(wspace=0.025, hspace=0.5)
        axes = [plt.subplot(gs[0, 2]), plt.subplot(gs[1, 2]), plt.subplot(gs[2, 2]), plt.subplot(gs[:, 0:2])]
        return fig, gs, axes

    def draw_plots(self):
        axes = self.axes
        realtimeDF = self.shared_history.get_realtime_info_df()
        start = realtimeDF.index[-1] - timedelta(minutes=self.settings_dict["PLOT_LENGTH"])
        realtimeDF = realtimeDF.between_time(start.time(), realtimeDF.index[-1].time())
        realtimeDF.plot(y=["ServingReqs", "ScheduledReqs", "UnscheduledReqs", "ServiceStationVehicles"],
                        ax=axes[0], legend=False)
        axes[0].set_title("# Expired = {}".format(realtimeDF["ExpiredReqs"].iloc[-1]))
        axes[0].legend(loc='lower left', bbox_to_anchor=(1.02, 0.5), frameon=False, prop={'size': 10})
        realtimeDF["PickUpDelay"].plot(ax=axes[1])
        axes[1].set_ylabel("Pick up Delay (min)")
        time_factor_df = self.shared_history.get_time_factor_df()
        if len(time_factor_df) > 0:
            time_factor_df.iloc[-30:].plot(y="Router Time Factor", ax=axes[2], legend=False)
            axes[2].set_title("Router Time Factor")
        if self.map_geo_df is not None:
            self.map_geo_df.plot(ax=axes[3], color="black")
            for key, positions in self.shared_history.get_attrb_by_name("vehicle_positions").items():
                positions = array(positions).reshape((-1, 2))
                axes[3].scatter(positions[:, 1], positions[:, 0], s=4, label=key)
            expired_reqs = self.shared_history.get_attrb_by_name("recent_expired_requests")
            if expired_reqs is not None:
                expired_reqs = array(expired_reqs).reshape((-1, 2))
                axes[3].scatter(expired_reqs[:, 1], expired_reqs[:, 0], s=4, label="expired request")

            gas_station_position, electric_station_position = self.shared_history.get_stations_locations()
            if gas_station_position is not None:
                axes[3].scatter(gas_station_position[:, 1], gas_station_position[:, 0], s=25, marker = "+",
                                label="Gas Station")
            if electric_station_position is not None:
                axes[3].scatter(electric_station_position[:, 1], electric_station_position[:, 0], s=25,
                                marker = "x", label="Electric Station")
            axes[3].legend(loc="upper left")

    def __animate(self, i):
        [ax.clear() for ax in self.axes]
        try:
            self.draw_plots()
            if self.grid_spec is not None:
                self.grid_spec.tight_layout(self.fig)
        except KeyError:
            pass

    def run(self):
        def frame():
            i = 0
            while True:
                if self.shared_history.get_attrb_by_name("stop_animation") is False:
                    i = i + 1
                    yield i
                else:
                    return

        self.fig, self.grid_spec, self.axes = self.generate_plot_axes()
        # Set up plot to call animate() function periodically
        if self.save_animation is True:
            ani = animation.FuncAnimation(self.fig, self.__animate, frames=frame, interval=200, save_count=1e10)
            Writer = animation.writers['ffmpeg']
            writer = Writer(fps=15, metadata=dict(artist='Me'), bitrate=1800)
            ani.save(join(self.animation_folder, "real_time_plots.mp4"), writer=writer)
        else:
            ani = animation.FuncAnimation(self.fig, self.__animate, interval=200)
            plt.show()
