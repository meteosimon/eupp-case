#!/usr/bin/env python3
# -------------------------------------------------------------------
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

import os
import logging as log
log.basicConfig(level = log.INFO)

# -------------------------------------------------------------------
def get_csv_filename(param, station_id, step, prefix = "euppens", csvdir = "csvdata"):
    """get_csv_filename(param, station_id, step, prefix = "euppens", csvdir = "csvdata")

    Params
    ------
    param : str
        Parameter processed.
    station_id : int
        Station identifier.
    step : int
        Forecast horizon in hours, integer.
    prefix : str
        Prefix for the file name, defaults to 'euppens'.
    csvdir : str
        Directory where the csvs should be stored.

    Return
    ------
    str : Name of the CSV file to store the final data set.
    """

    import os

    if not os.path.isdir(csvdir):
        try: os.makedirs(csvdir)
        except Exception as e: raise Exception(e)

    assert isinstance(param, str),      TypeError("argument 'param' must be str")
    assert isinstance(station_id, int), TypeError("argument 'station_id' must be int")
    assert isinstance(step, int),       TypeError("argument 'step' must be int")
    assert isinstance(prefix, str),     TypeError("argument 'prefix' must be str")
    assert isinstance(csvdir, str),     TypeError("argument 'csvdir' must be str")
    return os.path.join(csvdir, f"{prefix}_{param}_{station_id}_{step:03d}.csv")

