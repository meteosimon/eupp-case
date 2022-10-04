#!/usr/bin/env python3
# -------------------------------------------------------------------
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

import os
import argparse
import logging as log
log.basicConfig(level = log.INFO)

# -------------------------------------------------------------------
def get_csv_filename(args, station_id, step):
    """get_csv_filename(args, station_id, step)

    Params
    ------
    args : argparse.Namespace
        Object as returned by the argparser of the main script.
    station_id : int
        Station identifier.
    step : int
        Forecast horizon in hours, integer.

    Return
    ------
    str : Name of the CSV file to store the final data set.
    """


    if not os.path.isdir(args.prefix):
        try: os.makedirs(args.prefix)
        except Exception as e: raise Exception(e)

    assert isinstance(args, argparse.Namespace), TypeError("argument 'args' must be argparse.Namespace")
    assert isinstance(station_id, int), TypeError("argument 'station_id' must be int")
    assert isinstance(step, int),       TypeError("argument 'step' must be int")
    return os.path.join(args.prefix, f"{args.prefix}_{args.param}_{args.country}_{station_id}_{step:03d}.csv")

