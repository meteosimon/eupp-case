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
import pickle
import fsspec

from argparse import ArgumentParser
import xarray as xr
import pandas as pd
import numpy as np
import logging as log

log.basicConfig(level = log.INFO)

# -------------------------------------------------------------------
def get_data(country, param, cachedir = "_cache", do_cache = True):
    """get_data(cachefile)

    ... [tbd]
    """
    assert isinstance(country, str), TypeError("argument 'country' must be string")
    assert isinstance(param, str), TypeError("argument 'param' must be string")

    # If we have caching on, make sure the output dir exists and generate output name
    cachefile = os.path.join(cachedir, f"_cached_{country.lower()}_{param}.pickle")
    if do_cache:
        if not os.path.isdir(cachedir):
            print(f"Creating cache directory '{cachedir}'")
            try:
                os.makedirs(cachedir)
            except Exception as e:
                raise Exception(e)
        print(f"Cachefile: {cachefile}")

    # Prepare data if needed
    if not do_cache or not os.path.isfile(cachefile):
        log.info(f"{cachefile} not existing: Downloading data")
        target_fcs = fsspec.get_mapper(f"https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_ensemble_forecasts_surface_{country.lower()}.zarr")
        fcs = xr.open_zarr(target_fcs)[[param]]
        fcs_vars = fcs.var().variables

        log.info("Load and subset Obs")
        target_obs = fsspec.get_mapper(f"https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_forecasts_observations_surface_{country.lower()}.zarr")
        obs = xr.open_zarr(target_obs)[[param]]
        obs_vars = obs.var().variables

        if not param in fcs_vars: raise ValueError(f"cannot find '{param}' in fcs")
        if not param in obs_vars: raise ValueError(f"cannot find '{param}' in obs")
        obs = obs[[param]]
        fcs = fcs[[param]]

        if do_cache:
            with open(cachefile, "wb") as fid:
                log.info(f"Saving data into {cachefile}")
                pickle.dump([fcs, obs], fid)

    # Read prepared data from pickle file
    if do_cache:
        with open(cachefile, "rb") as fid:
            log.info(f"Reading data from {cachefile}")
            [fcs, obs] = pickle.load(fid)

    return [fcs, obs]


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

    if not os.path.isdir(csvdir):
        try: os.makedirs(csvdir)
        except Exception as e: raise Exception(e)

    assert isinstance(param, str),      TypeError("argument 'param' must be str")
    assert isinstance(station_id, int), TypeError("argument 'station_id' must be int")
    assert isinstance(step, int),       TypeError("argument 'step' must be int")
    assert isinstance(prefix, str),     TypeError("argument 'prefix' must be str")
    assert isinstance(csvdir, str),     TypeError("argument 'csvdir' must be str")
    return os.path.join(csvdir, f"{prefix}_{param}_{station_id}_{step:03d}.csv")

def get_station_meta(fcs, obs):
    """get_station_meta(fcs, obs)

    Params
    ------
    fcs : xarray.core.dataset.Dataset
        Object which contains the station-based forecasts (all stations, ...).
    obs : xarray.core.dataset.Dataset
        Object which contains the station-based observations (all stations, ...).

    Return
    ------
    pandas.core.frame.DataFrame : Pandas DataFrame with station information.
    """

    from xarray.core.dataset import Dataset
    assert isinstance(fcs, Dataset), TypeError("argument 'fcs' must be an xarray Dataset")
    assert isinstance(obs, Dataset), TypeError("argument 'obs' must be an xarray Dataset")

    # Check if required fields are available. These lists will also later be used to 
    # extract and later return station meta data fields.
    fcs_req = ["model_altitude", "model_land_usage", "model_longitude", "station_id"]
    for k in fcs_req:
        if not k in fcs.coords: raise Exception(f"coord '{k}' missing in fcs")
    obs_req = ["altitude", "land_usage", "latitude", "longitude", "station_id", "station_name"]
    for k in obs_req:
        if not k in obs.coords: raise Exception(f"coord '{k}' missing in obs")

    # Check that we have the same stations in both datasets
    obs_stnid = obs.coords.get("station_id").values
    fcs_stnid = fcs.coords.get("station_id").values
    if not all(obs_stnid == fcs_stnid):
        raise Exception("station_id not identical in both fcs and obs")

    # Fetching information
    res = []
    for i in range(obs.dims["station_id"]):
        tmp = {}
        for k in fcs_req: tmp[k] = fcs.get(k).values[i]
        for k in obs_req: tmp[k] = obs.get(k).values[i]
        res.append(tmp)

    return pd.DataFrame.from_dict(res)

# -------------------------------------------------------------------
# Main part of the Script
# -------------------------------------------------------------------
if __name__ == "__main__":

    # Some settings
    CSVDIR = "csvdata"

    # ---------------------------------------------------------------
    # Parsing console arguments
    # ---------------------------------------------------------------
    parser = ArgumentParser(f"{sys.argv[0]}")
    parser.add_argument("-c", "--country",
            choices = ["germany", "france", "netherlands", "switzerland", "austria"],
            type = str.lower, default = "germany",
            help = "Name of the country to be processed.")
    parser.add_argument("-p", "--param", type = str.lower, default = "t2m",
            help = "Name of the parameter to be processed.")
    parser.add_argument("-n", "--nocache", action = "store_true", default = False,
            help = "Disables auto-caching zarr file content (stored as pickle files). Defaults to 'False' (will do caching). Also forces all files to be recreated.")
    args = parser.parse_args()
    if not args.country:
        parser.print_help()
        raise ValueError("argument -c/--country not set (has no default)")

    # ---------------------------------------------------------------
    # Make sure CSVDIR exists
    # ---------------------------------------------------------------
    if not os.path.isdir(CSVDIR):
        try: os.makedirs(CSVDIR)
        except Exception as e: raise Exception(e)

    # Loading data (uses cache file if existing)
    [fcs, obs] = get_data(args.country, args.param, do_cache = not args.nocache)

    # ---------------------------------------------------------------
    # Fetching station meta if needed
    # ---------------------------------------------------------------
    station_meta_csv = os.path.join(CSVDIR, f"eupp_stationmeta_{args.country}.csv")
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
            csvfile = get_csv_filename(args.param, station_id, step_hours, csvdir = CSVDIR)
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



