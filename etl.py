import xarray as xr
import fsspec
import os
import pickle
import pandas as pd

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
    tmp = re.match("^([0-9]+)\sdays ([0-9])+:00:00$", STEP)
    assert isinstance(tmp, re.Match), ValueError("wrong format of object 'x'")
    return int(tmp[1]) * 24 + int(tmp[2])


def get_data(cachefile):
    """get_data(cachefile)

    ... [tbd]
    """
    assert isinstance(cachefile, str), TypeError("argument 'cachefile' must be string")
    # Prepare data if needed
    if not os.path.isfile(cachefile):
        print(f"{cachefile} not existing: Downloading data")
        target_fcs = fsspec.get_mapper("https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_ensemble_forecasts_surface_germany.zarr")
        fcs = xr.open_zarr(target_fcs)
        
        print("Load and subset Obs")
        target_obs = fsspec.get_mapper("https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_forecasts_observations_surface_germany.zarr")
        obs = xr.open_zarr(target_obs)
    
        with open(cachefile, "wb") as fid:
            print(f"Save data into {cachefile}")
            pickle.dump([fcs, obs], fid)
    
    # Read prepared data from pickle file
    with open(cachefile, "rb") as fid:
        print(f"Reading data from {cachefile}")
        [fcs, obs] = pickle.load(fid)

    return [fcs, obs]


# -------------------------------------------------------------------
# Main part of the Script
# -------------------------------------------------------------------
if __name__ == "__main__":

    # Configuration
    STATION_ID = 5371
    STEP   = "4 days 12:00:00"
    subset = {'station_id': STATION_ID, 'step': STEP}

    cachefile = "_data.pickle"
    csvfile   = f"data_{STATION_ID}_{step_to_hours(STEP):03d}.csv"
    print(f"Output file name: {csvfile}")

    # Loading data (uses cache file if existing)
    [fcs, obs] = get_data(cachefile)

    # -----------------------------------
    # Processing observation data
    obs_subset = obs[['t2m']].loc[subset]
    df_obs = obs_subset.rename({'t2m': 't2m_obs'}).to_dataframe()[["t2m_obs"]]


    #print("Merge Fx and Obs")
    #dsmerge = xr.combine_by_coords([obs_subset, fcs_subset], combine_attrs='drop_conflicts')
    #dfmerged = dsmerge.to_dataframe()


    # -----------------------------------
    # Processing forecast data
    fcs_subset = fcs[['valid_time', 't2m']].loc[subset]
    df_fcs = fcs_subset[['t2m']].to_dataframe()[["t2m"]].unstack('number')
    # drop multi-index on columns; rename columns
    df_fcs.columns = [f't2m_{x:02d}' for x in df_fcs.columns.droplevel()]
    df_fcs.index   = df_fcs.index.droplevel(1)

    # Calculate mean and standard deviation (including control run)
    tmp_mean = df_fcs.mean(axis = 1)
    tmp_std  = df_fcs.std(axis = 1)

    # Extract valid time
    vtime = fcs_subset[["valid_time"]].to_dataframe()[["valid_time"]]

    # Combine valid time, observation, ensemble mean and standard deviation
    # as well as the individual forecasts
    data = pd.concat([vtime, df_obs, tmp_mean, tmp_std, df_fcs], axis = 1)
    del tmp_mean, tmp_std

    print(f"Save final data set to {csvfile} now")
    data.to_csv(csvfile)








