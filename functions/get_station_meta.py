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

