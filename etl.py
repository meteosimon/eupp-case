import xarray as xr
import fsspec

STATION_ID = 5371
STEP = "4 days 12:00:00"
subset = {'station_id': STATION_ID, 'step': STEP}

print("Load and subset Fx")
target_fcs = fsspec.get_mapper("https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_ensemble_forecasts_surface_germany.zarr")
fcs = xr.open_zarr(target_fcs)
fcs_subset = fcs[['valid_time', 't2m']].loc[subset]

print("Load and subset Obs")
target_obs = fsspec.get_mapper("https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data/stations_forecasts_observations_surface_germany.zarr")
obs = xr.open_zarr(target_obs)
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
df_final.to_csv("test.csv")
