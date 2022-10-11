

#' Loading Station Information from NetCDF file
#'
#' Designed for the ESSD benchmark NetCDF files. Extracts
#' station information from the NetCDF file.
#'
#' @param nc \code{ncdf4} object.
#'
#' @returns Returns a data.frame with all necessary information.
#' This is the \code{station_id} (which itself is a dimension of
#' the NetCDF file) as well as all variables which belong to this
#' dimension (only this; same length, one-dimensional).
#'
#' @author Reto Stauffer
#' @export
get_stationdata <- function(nc) {
    stopifnot(inherits(nc, "ncdf4"))
    tmp <- names(nc$var)[sapply(nc$var, function(x, n) x$ndims == 1 && x$varsize == n, n = nc$dim$station_id$len)]
    return(cbind(data.frame(station_id = ncvar_get(nc, "station_id")),
                 as.data.frame(lapply(setNames(tmp, tmp), function(dim, nc) ncvar_get(nc, dim), nc = nc))))
}

#' Loading Data From NetCDF File
#'
#' Extracting data for a specific station from NetCDF files.
#' Specifically designed for the NetCDF file of the ESSD
#' benchmark dataset.
#'
#' @param station_id integer, station identifier.
#' @param steps NULL or integer, forecast step in hours. If \code{NULL},
#'        all available steps will be extracted and returned (all in one data.frame).
#' @param dir string, defaults to \code{"."}. Path where the NetCDF
#'        files are located.
#' @param type string, either \code{"test"} or \code{"training"}.
#' @param varname string, defaults to \code{"t2m"}. Variable to be read.
#'
#' @return Either a \code{data.frame} or a \code{list}, depending on the function
#' arguments (see Details).
#'
#' @details Tries to locate the \code{ESSD_benchmark_<type>_data_observations.nc}
#' and \code{ESSD_benchmark_<type>_data_forecasts.nc} NetCDF file (naming fixed)
#' and extracts data for a specific station.
#' This is designed to simply train statistical models and do the predictions.
#'
#' The function allows to retrieve forecasts/observations for one single forecast step
#' (e.g., \code{step = 24}) or for multiple steps at a time (e.g., \code{steps = c(0, 12, 24)}).
#' If only one single step is requested, a data.frame will be returned. Else it depends on
#' the argument \code{returnclass}. Note that the variable \code{step} will not be in the
#' \code{data.frame} returned if only one single step is requested.
#'
#' @author Reto Stauffer
#' @export
get_data <- function(station_id, steps = NULL, dir = ".", type = "test", varname = "t2m", returnclass = "data.frame") {
    stopifnot(is.numeric(station_id), length(station_id) == 1L)
    stopifnot(is.null(steps) || is.numeric(steps))
    stopifnot(is.character(dir), length(dir) == 1L, dir.exists(dir))
    type        <- match.arg(type, c("test", "training"))
    returnclass <- match.arg(returnclass, c("data.frame", "list"))

    # Names of the netcdf files to be accessed
    tmp         <- "ESSD_benchmark_%s_data_%s.nc"
    ncfile_obs  <- file.path(dir, sprintf(tmp, type, "observations"))
    ncfile_fcst <- file.path(dir, sprintf(tmp, type, "forecasts"))
    if (!file.exists(ncfile_obs))  stop("cannot find file '", ncfile_obs, "'")
    if (!file.exists(ncfile_fcst)) stop("cannot find file '", ncfile_fcst, "'")

    require("ncdf4")
    nc_obs  <- nc_open(ncfile_obs)
    nc_fcst <- nc_open(ncfile_fcst)

    # Is the step valid?
    steps <- if (is.null(steps)) ncvar_get(nc_fcst, "step") else as.integer(steps)
    stopifnot(all(steps %in% ncvar_get(nc_obs,  "step")))
    stopifnot(all(steps %in% ncvar_get(nc_fcst, "step")))

    # Loading station information
    stninfo_obs  <- get_stationdata(nc_obs)
    stninfo_fcst <- get_stationdata(nc_fcst)
    stopifnot(station_id %in% stninfo_obs$station_id)
    stopifnot(station_id %in% stninfo_fcst$station_id)
    stninfo_obs  <- as.list(subset(stninfo_obs,  station_id == station_id))
    stninfo_fcst <- as.list(subset(stninfo_fcst, station_id == station_id))

    # Extractor helper function
    extractor <- function(nc, args, step, varname) {
        modify_year <- function(d, year) {
            d <- sapply(as.POSIXlt(d), function(x) { x$year <- x$year + year; return(as.POSIXct(x)) })
            return(as.POSIXct(unlist(d), origin = "1970-01-01"))
        }
        fn <- function(year = NULL) {
            if (!is.null(year)) {
                args$start["year"] <- which(ncvar_get(nc, "year") == year)
                args$count["year"] <- 1
            }
            args <- lapply(args, sort_args, nc = nc, varname = varname)              # Sorting arguments
            tmp  <- ncvar_get(nc, varname, start = args$start, count = args$count)   # Extracting values
            if (is.null(numbers)) {
                tmp <- setNames(data.frame(value = tmp), varname)
            } else {
                tmp <- setNames(as.data.frame(t(tmp)), sprintf("%s_m%02d", varname, numbers))
            }
            time <- if (is.null(year)) get_time(nc) else modify_year(get_time(nc), year - length(years) - 1)
            return(transform(tmp, date_valid = time + step * 3600)) # Create data.frame
        }

        # If this is a forecast file: add 'number' dimension (members)
        # Also create variable 'numbers' used for renaming (in fn())
        numbers <- if ("number" %in% names(nc$dim)) ncvar_get(nc, "number") else NULL
        if (!is.null(numbers)) { args$start["number"] <- 1; args$count["number"] <- -1 }

        # If we find 'years' these are reforecast data (or ob with the stupid reforecast structure)
        years <- if ("year" %in% names(nc$dim)) ncvar_get(nc, "year") else NULL
        res   <- if (is.null(years)) fn() else do.call(rbind, lapply(years, fn))

        return(res)
    }

    # Arguments for ncvar_get extraction ('year' for reforecasts will be added if needed)
    args <- list(start = c(station_id = which(ncvar_get(nc_obs, "station_id") == station_id), step  = -999, time = 1),
                 count = c(station_id = 1, step = 1, time = -1))

    res <- list()
    for (step in steps) {
        args$start["step"] <- which(ncvar_get(nc_obs, "step") == step)

        # Not reforecast data structure
        obs  <- extractor(nc_obs, args, step, varname)
        fcst <- extractor(nc_fcst, args, step, varname)
        res[[sprintf("step_%03d", step)]] <- transform(merge(obs, fcst, by = "date_valid", all = TRUE), step = step)
    }
    print(length(res))

    # If steps/res is only length 1:
    if (length(res) == 1L) {
        res <- subset(res[[1]], select = -step)
    } else if (returnclass == "data.frame") {
        res <- do.call(rbind, res)
        rownames(res) <- NULL
    }
    return(res)
}


#' Sort Arguments for ncvar_get
#'
#' Helper function to bring start/end into the correct order
#' following the definition of the dimensions of the variable
#' to be loaded later.
#'
#' @param nc netcdf4 object.
#' @param args named vector to be ordered.
#' @param varname string, variable name to be extracted later.
#'
#' @author Reto Stauffer
sort_args <- function(nc, args, varname) {
    dimnames <- names(nc$dim)[nc[[c("var", varname, "dimids")]] + 1]
    if (!all(dimnames %in% names(args))) {
        stop("args (", paste(names(args), collapse = ","), ") not matching dimensions[", varname, "](", paste(dimnames, collapse = ","), ")")
    }
    return(args[dimnames])
}



#' Get Time Information
#'
#' Tries to extract and decode the 'time' dimension.
#'
#' @param nc the NetCDF file.
#' @param varname string, defaults to 'time'.
#'
#' @returns Vector of class Date or POSIXct.
#'
#' @author Reto Stauffer
get_time <- function(nc, varname = "time") {
    stopifnot(inherits(nc, "ncdf4"))
    stopifnot(is.character(varname), length(varname) == 1L)

    # Decoding the unit
    get_origin <- function(nc, varname) {
        dimunit    <- nc[[c("dim", varname)]]$unit
        pattern <- "(?<=\\ssince\\s)[0-9]{4}-[0-9]{2}-[0-9]{2}$"
        origin  <- tryCatch(regmatches(dimunit, regexpr(pattern, dimunit, perl = TRUE)),
                            warning = function(w) stop("problems properly extracting time origin"),
                            error   = function(e) stop("problems properly extracting time origin"))
        unit    <- tryCatch(regmatches(dimunit, regexpr("^[a-z]+", dimunit, perl = TRUE)),
                            warning = function(w) stop("problems properly extracting time origin"),
                            error   = function(e) stop("problems properly extracting time origin"))
        unit <- match.arg(unit, c("days", "seconds"))
        return(list(origin = origin, unit = unit))
    }

    o   <- get_origin(nc, varname)
    val <- ncvar_get(nc, varname)
    FUN <- if(o$unit == "seconds") as.POSIXct else function(x, origin, tz, ...) as.POSIXct(as.Date(x, origin), tz = tz)
    return(FUN(val, origin = o$origin, tz = "UTC"))
}
