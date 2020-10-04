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