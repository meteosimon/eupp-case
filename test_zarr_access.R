library(stars)

ftype <- "forecasts"
baseurl <- "https://storage.ecmwf.europeanweather.cloud/eumetnet-postprocessing-benchmark-1st-phase-training-dataset/data/stations_data"

dsn <- sprintf('ZARR:"vsicurl/%s/stations_ensemble_%s_surface_germany.zarr"', baseurl, ftype)

zr <- read_mdim(dsn)
