import xarray as xr
import fsspec
import os
import pickle

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
    # Processing data
    # -----------------------------------
    fcs_subset = fcs[['valid_time', 't2m']].loc[subset]
    obs_subset = obs[['t2m']].loc[subset]
    obs_subset = obs_subset.rename({'t2m': 't2m_obs'})
    
    print("Merge Fx and Obs")
    dsmerge = xr.combine_by_coords([obs_subset, fcs_subset], combine_attrs='drop_conflicts')
    
    df_obs = dsmerge['t2m_obs'].to_dataframe()
    df_obs = df_obs[['t2m_obs']]
    
    dfmerged = dsmerge.to_dataframe()
    df = dfmerged[['t2m']].unstack('number')
    df.columns = [f't2m_{x:02d}' for x in df.columns.droplevel()] # drop multi-index on columns; rename columns
    df.index = df.index.droplevel(1) # drop multi-index on rows
    
    df_final = df_obs.merge(df, on='time')
    
    print("Store")
    df_final.to_csv(csvfile)
