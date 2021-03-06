
""" Settings class for PyMoDSim """

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

from dynaconf import Dynaconf, loaders
from dynaconf.utils.boxing import DynaBox
import pathlib
import typing as tp
from shutil import copyfile
from logging import getLogger
from typing import List

_simulator_path = pathlib.Path(__file__).parent.absolute()


class Settings(Dynaconf):
    __default_settings_files: List[pathlib.Path] = [_simulator_path.joinpath("settings.toml")]

    def __init__(self, settings_files: tp.Union[str, tp.List[str], None] = None,
                 settings_dict: tp.Optional[dict] = None):
        """ A wrapper class around Dynaconf for simulator settings

        :param settings_files:  Paths of configuration files. The default parameter values will be overwritten
                                by the values in the files according to file order in the list. Additional parameters
                                to be used in the subclasses can be added as well. The format could be
                                toml|yaml|json|ini
        :param settings_dict:   Additional or overridden parameters as dictionary. The parameter values in settings_dict
                                are given preference over values in settings_files
        """

        files = self.__generate_paths(settings_files)
        default_files_strings = {str(x) for x in self.__default_settings_files}
        files = [x for x in files if str(x) not in default_files_strings]
        default_files = self.__generate_paths(self.__default_settings_files)
        files = default_files + files
        super().__init__(settings_files=files, environments=True)
        if settings_dict is not None:
            self.update(settings_dict)

    def __generate_paths(self, settings_files: tp.Union[str, tp.List[pathlib.PurePath], tp.List[str]]):
        files = []
        if isinstance(settings_files, (str, pathlib.PurePath)):
            files = [pathlib.Path(settings_files)]
        elif isinstance(settings_files, list):
            files = [pathlib.Path(path) for path in settings_files]
        not_found = [path.absolute() for path in files if path.is_file() is False]
        files = [path.absolute() for path in files if path.is_file() is True]
        if len(not_found) > 0:
            getLogger().warning("Could not find settings files: {}".format(not_found))
            getLogger().warning("using files: {}".format(files))
        return files

    def export_to_file(self, file_path):
        """ Export the settings to a file

        :param file_path:   path and name of the export file. The extensions be any out of
                            .yaml, .toml, .ini, .json, .py
        """

        # generates a dict with all the keys for `development` env
        data = self.as_dict()
        loaders.write(file_path, DynaBox(data).to_dict(), env=self.current_env)

    @staticmethod
    def generate_default_settings_file(destination: tp.Optional[str] = None):
        """ Generates the default settings files

        :param destination: path of the destination folder
        """

        for file in Settings.__default_settings_files:
            if destination is None:
                destination = pathlib.Path().joinpath(file.name).absolute()
            else:
                destination = pathlib.Path(destination).joinpath(file.name).absolute()
            print("copied {} to {}".format(file.name, destination))
            copyfile(file.absolute(), destination)

    def copy(self):
        return Settings(self.settings_file, self.as_dict())

    @staticmethod
    def update_default_files(file_paths: tp.Union[List[str], str], keep_current_global_values=False):
        """ Updates the default settings files and reloads the global_settings object

        :param file_paths:                  File paths of the new settings files
        :param keep_current_global_values:  Whether to keep the current values of the already defined parameters or not
        """

        if isinstance(file_paths, str):
            Settings.__default_settings_files.append(pathlib.Path(file_paths))
        else:
            for file in file_paths:
                Settings.__default_settings_files.append(pathlib.Path(file))
        global global_settings
        current_dict = global_settings.as_dict()
        global_settings = Settings()
        if keep_current_global_values is True:
            global_settings.update(current_dict)


global_settings = Settings()
