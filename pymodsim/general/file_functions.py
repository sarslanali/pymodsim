'''
Created on Apr 19, 2019

@author: Arslan Ali Syed
'''
import os
import yaml
from collections import OrderedDict


def check_folder_name(folder):
    if not os.path.isdir(folder):
        foldername = folder
        os.makedirs(folder)
    else:
        i = 1
        while os.path.isdir(folder + "_" + str(i)):
            i+=1
        foldername = folder + "_" + str(i)
        os.makedirs(foldername)
    return foldername


def dump_dict_to_yml(full_path, dump_dict):
    yaml.add_representer(OrderedDict,
                         lambda dumper, data: dumper.represent_mapping('tag:yaml.org,2002:map', data.items()))
    with open(full_path, 'w') as outfile:
        yaml.dump(dump_dict, outfile, default_flow_style=False)