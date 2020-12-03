
""" Logger configuration for PyMoDSim simulator. """

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

import os
import logging
import yaml
from os.path import join
    
def listener_process(queue, logger_folder):
    import logging.config
    folderpath = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(folderpath, 'logging.yaml')
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
        logfilename = config["handlers"]["file_handler"]["filename"]
        config["handlers"]["file_handler"]["filename"] = join(logger_folder, logfilename)
        logging.config.dictConfig(config)
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import sys, traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

def worker_configurer(queue):
    import logging.handlers
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    logger = logging.getLogger()
    logger.addHandler(h)
    logger.setLevel(logging.INFO)