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

log.basicConfig(level = log.INFO)

# -------------------------------------------------------------------
def get_data(cachefile):
    """get_data(cachefile)

    ... [tbd]
    """
    assert isinstance(cachefile, str), TypeError("argument 'cachefile' must be string")
    # Prepare data if needed
    if not os.path.isfile(cachefile):
        log.info(f"{cachefile} not existing: Downloading data")
        target_fcs = fsspec.get_mapper("https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_ensemble_forecasts_surface_germany.zarr")
        fcs = xr.open_zarr(target_fcs)

        log.info("Load and subset Obs")
        target_obs = fsspec.get_mapper("https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_forecasts_observations_surface_germany.zarr")
        obs = xr.open_zarr(target_obs)

        with open(cachefile, "wb") as fid:
            log.info(f"Saving data into {cachefile}")
            pickle.dump([fcs, obs], fid)

    # Read prepared data from pickle file
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
def get_csv_filename(station_id, step, prefix = "euppens"):
    """get_csv_filename(station_id, step)

    Params
    ------
    station_id : int
        Station identifier.
    step : str
        String of format 'X days HH:00:00' minutes. Will be parsed
        to calculate forecast hours (step in hours).
    prefix : str
        Prefix for the file name, defaults to 'euppens'.

    Return
    ------
    str : Name of the CSV file to store the final data set.
    """
    assert isinstance(station_id, int), TypeError("argument 'station_id' must be int")
    assert isinstance(step, str),       TypeError("argument 'step' must be str")
    assert isinstance(prefix, str),     TypeError("argument 'prefix' must be str")
    return f"{prefix}_{station_id}_{step_to_hours(step):03d}.csv"

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

# -------------------------------------------------------------------
# Main part of the Script
# -------------------------------------------------------------------
if __name__ == "__main__":

    # Stations and forecast steps (lead times) to process
    stations = {"Wasserkuppe": 5371, "Emden": 5839, "Oberstdorf": 3730}
    steps    = ["4 days 12:00:00", "5 days 00:00:00"]

    # Fetching station meta
    station_meta = []
    station_meta_csv = "eupp_station_meta.csv"

    cachefile = "_data.pickle" # Used to cache the data request

    # Loading data (uses cache file if existing)
    [fcs, obs] = get_data(cachefile)

    # Looping over all stations/steps
    for station_name,station_id in stations.items():
        for step in steps:

            # -----------------------------------
            # Define output file name and subset args
            log.info(f"Processing data for station {station_name} (id {station_id}), {step} ahead.")
            csvfile = get_csv_filename(station_id, step)
            subset = {"station_id": station_id, "step": step}

            # -----------------------------------
            # Prepare observation data
            obs_subset = obs[['t2m']].loc[subset]
            # Fetching meta information
            station_meta.append(get_station_meta(obs_subset.coords))
            # Skip the rest if the data output file exists already
            if os.path.isfile(csvfile): continue # Skip if output file exists
            df_obs = obs_subset.rename({'t2m': 't2m_obs'}).to_dataframe()[["t2m_obs"]]

            # -----------------------------------
            # Prepare forecast data
            fcs_subset = fcs[['valid_time', 't2m']].loc[subset]
            df_fcs = fcs_subset[['t2m']].to_dataframe()[["t2m"]].unstack('number')
            # drop multi-index on columns; rename columns
            df_fcs.columns = [f't2m_{x:02d}' for x in df_fcs.columns.droplevel()]
            df_fcs.index   = df_fcs.index.droplevel(1)

            # -----------------------------------
            # Calculate ensemble mean and standard deviation (including control run)
            tmp_mean = df_fcs.mean(axis = 1).to_frame("ens_mean")
            tmp_std  = df_fcs.std(axis = 1).to_frame("ens_sd")

            # -----------------------------------
            # Extract valid time, append julian day (0-based; 0 = January 1th)
            vtime = fcs_subset[["valid_time"]].to_dataframe()[["valid_time"]]
            vtime["yday"] = vtime.valid_time.apply(lambda x: int(x.strftime("%j")) - 1)

            # -----------------------------------
            # Combine valid time, observation, ensemble mean and standard deviation
            # as well as the individual forecasts
            data = pd.concat([vtime, df_obs, tmp_mean, tmp_std, df_fcs], axis = 1)
            del tmp_mean, tmp_std, vtime

            log.info(f"Writing final data set to {csvfile} now")
            data.to_csv(csvfile)

            del subset, csvfile, data, df_fcs, df_obs


    log.info("Write station meta file")
    pd.DataFrame(station_meta).to_csv(station_meta_csv, index = False)
    log.info("\nThat's the end my friend.")

