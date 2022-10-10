#!/usr/bin/env python3
# -------------------------------------------------------------------
# Authors: Thorsten Simon and Reto Stauffer
# Date: 2022-09-16
# -------------------------------------------------------------------

import os
import pandas as pd
import logging as log
log.basicConfig(level = log.INFO)

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
    log.info("Checking dimensions")
    fcs_req = ["model_altitude", "model_land_usage", "model_longitude", "station_id"]
    for k in fcs_req:
        if not k in fcs.coords: raise Exception(f"coord '{k}' missing in fcs")
    obs_req = ["altitude", "land_usage", "latitude", "longitude", "station_id", "station_name"]
    for k in obs_req:
        if not k in obs.coords: raise Exception(f"coord '{k}' missing in obs")

    # Check that we have the same stations in both datasets
    log.info("Loading station ids")
    obs_stnid = obs.coords.get("station_id").values
    fcs_stnid = fcs.coords.get("station_id").values
    if not all(obs_stnid == fcs_stnid):
        raise Exception("station_id not identical in both fcs and obs")

    # Fetching information
    log.info("Starting to extract the station metatdata")
    res = []


    # Test
    res = {}
    log.info(" - From obs first ...")
    #for k in obs_req: res[k] = obs.get(k).values
    for k in obs_req: res[k] = obs.coords[k].values
    log.info(" - From fcs second ...")
    #for k in fcs_req: res[k] = fcs.get(k).values
    for k in fcs_req: res[k] = fcs.coords[k].values

    log.info("- Finished, return data.frame")
    return pd.DataFrame.from_dict(res)






