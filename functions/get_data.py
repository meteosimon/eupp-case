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


