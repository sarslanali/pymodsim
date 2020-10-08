'''
Created on Apr 19, 2019

@author: Arslan Ali Syed
'''
import os


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
