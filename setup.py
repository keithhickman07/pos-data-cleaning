# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 08:18:03 2020

@author: khickman
"""

from cx_Freeze import setup, Executable

base = None    

executables = [Executable("main.py", base=base)]

packages = ["idna", "pandas", "os","numpy", "xlrd", "datetime", "re"]

options = {
    'build_exe': {    
        'packages':packages,
    },    
}

setup(
    name = "POS Cleanup",
    options = options,
    version = "0.2",
    description = 'Script to clean/load point-of-sale data',
    executables = executables
)