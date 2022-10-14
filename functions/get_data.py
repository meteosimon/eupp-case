#!/usr/bin/env python3
# -------------------------------------------------------------------
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

import sys
import os
import pickle
import fsspec
import xarray as xr

import logging as log
log.basicConfig(level = log.INFO)

# -------------------------------------------------------------------
def get_data(country, param, reforecast, cachedir = "_cache", do_cache = True):
    """get_data(cachefile)

    ... [tbd]
    """
    assert isinstance(country, str), TypeError("argument 'country' must be string")
    assert isinstance(param, str), TypeError("argument 'param' must be string")
    assert isinstance(reforecast, bool), TypeError("argument 'reforecast' must be bool")

    # Forecast type
    ftype = "reforecasts" if reforecast else "forecasts"

    # If we have caching on, make sure the output dir exists and generate output name
    cachefile = os.path.join(cachedir, f"_cached_{country.lower()}_{param}_{ftype}.pickle")
    if do_cache:
        if not os.path.isdir(cachedir):
            print(f"Creating cache directory '{cachedir}'")
            try:
                os.makedirs(cachedir)
            except Exception as e:
                raise Exception(e)
        print(f"Cachefile: {cachefile}")

    # If the country is 'swtizerland' this is in the restrictec area and only
    # available via EWC (cloud)
    if country in ["switzerland", "belgium"]:
        server_path = "/mnt/benchmark-training-dataset-zarr-restricted/mnt/benchmark-training-dataset-zarr-restricted/data/stations_data"
    else:
        server_path = "https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data"

    # Prepare data if needed
    if not do_cache or not os.path.isfile(cachefile):
        log.info(f"{cachefile} not existing: Downloading data")
        log.info("Load and subset Obs")

        # NOTE: Take care of not creating // in the URL, zarr does not like it at all

        # Reading forecasts
        target_file = f"{server_path}/stations_ensemble_{ftype}_surface_{country.lower()}.zarr"
        log.info(f"Reading: {target_file}")
        target_fcs = fsspec.get_mapper(target_file)
        del target_file
        fcs = xr.open_zarr(target_fcs, consolidated = True)[[param]]
        fcs_vars = fcs.var().variables

        # Reading observations
        target_file = f"{server_path}/stations_{ftype}_observations_surface_{country.lower()}.zarr"
        log.info(f"Reading: {target_file}")
        target_obs = fsspec.get_mapper(target_file)
        del target_file
        obs = xr.open_zarr(target_obs, consolidated = True)[[param]]
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
    log.info("Returning forecast data (fcs) and observations (obs) now")

    return [fcs, obs]


