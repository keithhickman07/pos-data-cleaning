# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 06:44:34 2020

@author: khickman
"""

import pandas as pd
import os


def already_run():
    if os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\test.csv"):
        runs = pd.read_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\test.csv")
    else:
        print("no files have been run")
    
    already_in_file = pd.Series(runs['ACCT MAJOR'].unique()).to_list()
    already_in_file = [x.lower() for x in already_in_file]
    
    return runs

runs = already_run()
    