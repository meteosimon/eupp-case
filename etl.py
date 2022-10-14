#!/usr/bin/env python3
# -------------------------------------------------------------------
# Small downloader for a toy/demo data set of ensemble forecasts
# and corresponding observations from the European Postprocessing
# Benchmark data set.
#
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

import os
import pickle
import fsspec

import xarray as xr
import pandas as pd
import logging as log
import numpy as np

log.basicConfig(level = log.INFO)

# -------------------------------------------------------------------
def get_data(reforecast, cachefile):
    """get_data(cachefile)

    Params
    ------
    reforecast : bool
        Should reforecasts (training) or forecasts (test) be loaded?
    cachefile : None or str
        Can be used to cache data; stored as pickle.
    """
    assert isinstance(reforecast, bool), TypeError("argument 'reforecast' must be bool")
    assert isinstance(cachefile, (str, type(None))), TypeError("argument 'cachefile' must be None or str")

    # Warning: no slash at the end, this will break the request
    baseurl = "https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data"
    ftype   = "reforecasts" if reforecast else "forecasts"

    # Prepare data if needed
    if cachefile is None or not os.path.isfile(cachefile):
        log.info(f"Downloading zarr meta '{ftype}' ({reforecast=})")
        target_fcs = f"{baseurl}/stations_ensemble_{ftype}_surface_germany.zarr"
        log.info(f"FCS:   {target_fcs}")
        target_fcs = fsspec.get_mapper(target_fcs)
        fcs = xr.open_zarr(target_fcs)

        log.info("Load and subset Obs")
        target_obs = f"{baseurl}/stations_{ftype}_observations_surface_germany.zarr"
        log.info(f"OBS:   {target_obs}")
        target_obs = fsspec.get_mapper(target_obs)
        obs = xr.open_zarr(target_obs)

        if cachefile is not None:
            with open(cachefile, "wb") as fid:
                log.info(f"Saving data into {cachefile}")
                pickle.dump([fcs, obs], fid)
    # Read prepared data from pickle file
    else:
        with open(cachefile, "rb") as fid:
            log.info(f"Reading data from {cachefile}")
            [fcs, obs] = pickle.load(fid)

    return [fcs, obs]


# -------------------------------------------------------------------
def step_to_hours(x):
    """step_to_hours(x)

    Convert string to hours. Expects 'x' to be
    a string of format "X days HH:00:00".

    Param
    -----
    x : str
        String following the format "X days HH:00:00"

    Return
    ------
    int : Number of hours (lead time/step) of the string
    represented by argument 'x'.
    """
    import re
    assert isinstance(x, str), TypeError("argument 'x' must be string")
    tmp = re.match("^([0-9]+)\sdays ([0-9]+):00:00$", x)
    assert isinstance(tmp, re.Match), ValueError("wrong format of object 'x'")
    return int(tmp[1]) * 24 + int(tmp[2])


# -------------------------------------------------------------------
def get_csv_filename(station_id, step, reforecast, prefix = "euppens"):
    """get_csv_filename(station_id, step)

    Params
    ------
    station_id : int
        Station identifier.
    step : str
        String of format 'X days HH:00:00' minutes. Will be parsed
        to calculate forecast hours (step in hours).
    reforecast : bool
        Processing reforecasts or forecasts? If true, the csv will
        contain the word 'training', else 'test'.
    prefix : str
        Prefix for the file name, defaults to 'euppens'.

    Return
    ------
    str : Name of the CSV file to store the final data set.
    """
    assert isinstance(station_id, int),  TypeError("argument 'station_id' must be int")
    assert isinstance(step, str),        TypeError("argument 'step' must be str")
    assert isinstance(reforecast, bool), TypeError("argument 'reforecast' must be bool")
    assert isinstance(prefix, str),      TypeError("argument 'prefix' must be str")
    rf = "training" if reforecast else "test"
    return f"{prefix}_{station_id}_{rf}_{step_to_hours(step):03d}.csv"

# -------------------------------------------------------------------
def get_station_meta(x):
    """get_station_meta(x)

    Extract station meta information from dataset coordinates.

    Params
    ------
    x : xarray.core.coordinates.DatasetCoordinates
        Object from which the meta information will be extracted.

    Return
    ------
    dict : Returns a dictionary with station_name (str),
    station_id (int), land_usage (int), as well as altitude, longitude
    and latitude (all float).
    """
    from xarray.core.coordinates import DatasetCoordinates
    assert isinstance(x, DatasetCoordinates), TypeError("argument 'x' must be of type xarray.core.coordinates.DatasetCoordinates")

    tmp = {"station_name": str(x["station_name"].data)}
    for k in ["station_id", "land_usage"]:          tmp[k] = int(x[k].data)
    for k in ["altitude", "longitude", "latitude"]: tmp[k] = float(x[k].data)
    return tmp


def modify_date(df, nyears = 20):
    """modify_date(df)

    Modify DatetimeIndex column for reforecast data.

    Params
    ------
    df : pandas.DataFrame
        Data.frame with a DatetimeIndex column to be modified
    nyears : int
        Number of years. Must be known as '1' in the multiindex
        is 'nyears ago'.

    Return
    ------
    pandas.DataFrame : Returns a manipulated version of the input 'df'.
    """
    assert isinstance(df, pd.DataFrame), TypeError("argument 'df' must be a pandas.DataFrame")
    assert isinstance(nyears, int), TypeError("argument 'nyears' must be int")
    if isinstance(df.index, pd.MultiIndex):
        from datetime import datetime as dt
        tmp = []
        for x in df.index:
            x = x[0].utctimetuple()
            tmp.append(dt(x.tm_year - (nyears - x[1] + 1), x.tm_mon, x.tm_mday, x.tm_hour, x.tm_min))
        df.index = pd.DatetimeIndex(tmp, name = df.index.name)
    return df


# -------------------------------------------------------------------
# Main part of the Script
# -------------------------------------------------------------------
if __name__ == "__main__":

    # Stations and forecast steps (lead times) to process
    stations = {"Wasserkuppe": 5371, "Emden": 5839, "Oberstdorf": 3730}
    steps    = ["4 days 12:00:00", "5 days 00:00:00"]

    # Fetching station meta
    station_meta_csv = "eupp2_station_meta.csv"
    station_meta = {}

    cachefile = "_data2.pickle" # Used to cache the data request

    # Processing reforecasts (training) and forecasts (test)
    ##for reforecast in [True, False]:
    for reforecast in [False]:
        log.info(f"Start processing reforecast = {reforecast}")

        # Loading data (uses cache file if existing)
        cachefile = "_cached_reforecasts.pickle" if reforecast else "_cached_forecasts.pickle"
        [fcs, obs] = get_data(reforecast = reforecast, cachefile = cachefile)

        # Looping over all stations/steps
        for station_name,station_id in stations.items():

            for step in steps:

                # Define output file name and subset args
                log.info(f"Processing data for station {station_name} (id {station_id}), {step} ahead.")
                csvfile = get_csv_filename(station_id, step, reforecast, prefix = "euppens")
                if os.path.isfile(csvfile): continue # Skip if output file exists
                subset = {"station_id": station_id, "step": step}

                # -----------------------------------
                # Prepare observation data
                obs_subset = obs[['t2m']].loc[subset]
                # Skip the rest if the data output file exists already
                df_obs = obs_subset.rename({'t2m': 't2m_obs'}).to_dataframe()[["t2m_obs"]]

                # Fetching station meta
                station_meta[station_name] = get_station_meta(obs_subset.coords)

                # -----------------------------------
                # Prepare forecast data
                fcs_subset = fcs[['valid_time', 't2m']].loc[subset]
                df_fcs = fcs_subset[["t2m"]].to_dataframe()[["t2m"]].unstack('number')
                # drop multi-index on columns; rename columns
                df_fcs.columns = [f't2m_{x:02d}' for x in df_fcs.columns.droplevel()]
                df_fcs.index   = df_fcs.index.droplevel(1)

                # -----------------------------------
                # Extract valid time, append julian day (0-based; 0 = January 1th)
                vtime = fcs_subset[["valid_time"]].to_dataframe()[["valid_time"]]
                vtime["yday"] = vtime.valid_time.apply(lambda x: int(x.strftime("%j")) - 1)

                # -----------------------------------
                # Update date; only has an effect if we have reforecasts
                vtime  = modify_date(vtime)
                df_obs = modify_date(df_obs)
                df_fcs = modify_date(df_fcs)

                # -----------------------------------
                # Calculate ensemble mean and standard deviation (including control run)
                tmp_mean = df_fcs.mean(axis = 1).to_frame("ens_mean")
                tmp_std  = df_fcs.std(axis = 1).to_frame("ens_sd")

                # -----------------------------------
                # Combine valid time, observation, ensemble mean and standard deviation
                # as well as the individual forecasts
                data = pd.concat([vtime, df_obs, tmp_mean, tmp_std, df_fcs], axis = 1)

                # Delete objects 
                del tmp_mean, tmp_std, vtime, df_fcs, df_obs

                log.info(f"Writing final data set to {csvfile} now")
                data.to_csv(csvfile, index = False)
                del data

        log.info("Write station meta file")
        print(pd.DataFrame(station_meta))
        pd.DataFrame(station_meta).transpose().to_csv(station_meta_csv, index = False)

    log.info("\nThat's the end my friend.")

