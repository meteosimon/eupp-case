#!/usr/bin/env python3
# -------------------------------------------------------------------
# Downloader for the EUPP Hacky Benchmark Station Dataset
#
# Also works on the European Weather Cloud for data which is not
# publicly available (Switzerland).
#
# Developed on Python version 3.10.4
#
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

import sys
import os
import re
import glob
import pickle
import fsspec
import argparse
from zipfile import ZipFile

import xarray as xr
import pandas as pd
import numpy as np

from functions import *

import logging as log
log.basicConfig(level = log.INFO)


def main(args):
    """main(args)

    Params
    ------
    args : argparse.Namespace or dict
        Parsed argument, object as returned by parse_args().
        Must contain 'country' (str), 'param' (str), and 'nocache' (bool).
        If it is a dictionary, it will be converted into argparse.Namespace internally.

    Return
    ------
    No return, but saves a bunch of files into CSVDIR in the best case.
    """
    if isinstance(args, dict): args = argparse.Namespace(**args)
    assert isinstance(args, argparse.Namespace), TypeError("argument 'args' must be argparse.Namespace")
    for k in ["country", "param", "nocache", "prefix"]:
        if not k in args: ValueError(f"option '{k}' not in object 'args'")
    assert isinstance(args.prefix, str),   TypeError("args.country must be str")
    assert isinstance(args.country, str),  TypeError("args.country must be str")
    assert isinstance(args.param, str),    TypeError("args.param must be str")
    assert isinstance(args.nocache, bool), TypeError("args.nocache must be bool")

    # ---------------------------------------------------------------
    # Prevent the script from running again if the final zip file exists
    # ---------------------------------------------------------------
    final_zip = os.path.join(args.prefix, f"{args.prefix}_{args.param}_{args.country}.zip")
    if os.path.isfile(final_zip):
        print(f"Final file {final_zip} exists; do not continue (return None)")
        return None

    # ---------------------------------------------------------------
    # Make sure CSVDIR exists
    # ---------------------------------------------------------------
    if not os.path.isdir(args.prefix):
        try: os.makedirs(args.prefix)
        except Exception as e: raise Exception(e)

    # Loading data (uses cache file if existing)
    [fcs, obs] = get_data(args.country, args.param, do_cache = not args.nocache)

    # ---------------------------------------------------------------
    # Fetching station meta if needed
    # ---------------------------------------------------------------
    station_meta_csv = os.path.join(args.prefix, f"{args.prefix}_{args.param}_{args.country}_stationdata.csv")
    if args.nocache or not os.path.isfile(station_meta_csv):
        log.info("Extracting station meta data")
        station_meta = get_station_meta(fcs, obs)
        station_meta.to_csv(station_meta_csv)
        print(station_meta.head())
        del station_meta # Not used anymore in this script

    # ---------------------------------------------------------------
    # Time check
    # ---------------------------------------------------------------
    assert obs.get("step").dtype == np.dtype("<m8[ns]"), TypeError("dimension 'step' not of dtype '<m8[ns]'")
    assert obs.get('step').attrs["long_name"] == "time since forecast_reference_time", \
            ValueError("dimension 'step' not 'time since forecast_reference_time")

    # ---------------------------------------------------------------
    # Looping over all stations/steps
    # ---------------------------------------------------------------
    for station_id in obs.get("station_id").values:
        station_id = int(station_id)
        for step in obs.get("step").values:
            # Convert forecast step to hours
            step_hours = int(step / 1e9 / 3600) # convert to hours
            log.info(f"Processing data for station {station_id:5d} {step_hours:+4d}h ahead.")

            # -----------------------------------
            # Define output file name and subset args
            csvfile = get_csv_filename(args, station_id, step_hours)

            # Subsetting station/step for data processing
            subset = {"station_id": station_id, "step": step}

            # -----------------------------------
            # Prepare observation data
            obs_subset = obs.loc[subset]
            # Skip the rest if the data output file exists already
            if not args.nocache and os.path.isfile(csvfile): continue # Skip if output file exists
            valid_time   = pd.DatetimeIndex(obs.time.values + step, name = "valid_time")
            df_obs       = obs_subset.rename({args.param: f"{args.param}_obs"}).to_dataframe()[[f"{args.param}_obs"]]
            df_obs.index = valid_time

            # -----------------------------------
            # Prepare forecast data
            fcs_subset = fcs[["time", "step", args.param]].loc[subset]
            valid_time = pd.DatetimeIndex(fcs.time.values + step, name = "valid_time")
            df_fcs = fcs_subset[[args.param]].to_dataframe()[[args.param]].unstack("number")

            # drop multi-index on columns; rename columns
            df_fcs.columns = [f"{args.param}_{x:02d}" for x in df_fcs.columns.droplevel()]
            df_fcs.index   = df_fcs.index.droplevel(1)
            df_fcs.index   = valid_time

            # -----------------------------------
            # Calculate ensemble mean and standard deviation (including control run)
            tmp_mean = df_fcs.mean(axis = 1).to_frame("ens_mean")
            tmp_std  = df_fcs.std(axis = 1).to_frame("ens_sd")

            # -----------------------------------
            # Extract valid time, append julian day (0-based; 0 = January 1th)
            yday = [int(x.strftime("%j")) - 1 for x in df_fcs.index]
            yday = pd.DataFrame({"yday": yday}, index = df_fcs.index)

            # -----------------------------------
            # Combine valid time, observation, ensemble mean and standard deviation
            # as well as the individual forecasts
            assert df_fcs.shape[0] == df_obs.shape[0], Exception("number of rows of df_fcs and df_obs differ")
            assert df_fcs.shape[0] == yday.shape[0], Exception("number of rows of df_fcs and yday differ")
            data = pd.concat([yday, df_obs, tmp_mean, tmp_std, df_fcs], axis = 1)

            ##log.info(f"Writing final data set to {csvfile} now")
            data.to_csv(csvfile)

            del tmp_mean, tmp_std, yday, csvfile
            del subset, data, df_fcs, df_obs


    # ---------------------------------------------------------------
    # All stations processes
    # ---------------------------------------------------------------
    log.info(f"All stations processed for {args.country}, {args.param}, create zip file")

    # Find files to be zipped
    keepwd = os.getcwd()
    os.chdir(os.path.dirname(final_zip))
    pattern = re.compile(f"{args.prefix}_{args.param}_{args.country}_.*\.csv$")
    files = []
    for f in glob.glob("*"):
        if pattern.match(f): files.append(f)
    files.sort()
    try:
        with ZipFile(os.path.basename(final_zip), "w") as fid:
            for f in files:
                fid.write(f) # Store in zip
    except:
        log.error("Problems with zipping do not delete files")
        if os.path.isfile(final_zip): os.remove(final_zip)
    finally:
        log.info("Zip file created, delete source files")
        for f in files: os.remove(f)
    os.chdir(keepwd)


# -------------------------------------------------------------------
# Main part of the Script
# -------------------------------------------------------------------
if __name__ == "__main__":

    # ---------------------------------------------------------------
    # Parsing console arguments
    # ---------------------------------------------------------------
    parser = argparse.ArgumentParser(f"{sys.argv[0]}")
    parser.add_argument("-c", "--country",
            choices = ["germany", "france", "netherlands", "switzerland", "austria"],
            type = str.lower, default = "germany",
            help = "Name of the country to be processed.")
    parser.add_argument("-p", "--param", type = str.lower, default = "t2m",
            help = "Name of the parameter to be processed.")
    parser.add_argument("--prefix", type = str, default = "euppens",
            help = "Used as name of the output directory for the results as well as prefix for all files created by this script.")
    parser.add_argument("-n", "--nocache", action = "store_true", default = False,
            help = "Disables auto-caching zarr file content (stored as pickle files). Defaults to 'False' (will do caching). Also forces all files to be recreated.")
    args = parser.parse_args()
    if not args.country:
        parser.print_help()
        raise ValueError("argument -c/--country not set (has no default)")

    # Start downloading
    main(args)



