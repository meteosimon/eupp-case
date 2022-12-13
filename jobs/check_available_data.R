#!/usr/bin/env Rscript
# -------------------------------------------------------------------
# Checking for reason for broken models
#
# 2022-10-10, Reto
# -------------------------------------------------------------------

rm(list = objects())

# -------------------------------------------------------------------
# Identify models
# -------------------------------------------------------------------
rdsfiles <- basename(list.files("../results", recursive = TRUE))
models <- sort(unique(regmatches(rdsfiles, regexpr("^.*?(?=_)", rdsfiles, perl = TRUE))))
cat("Models found:", paste(models, collapse = ", "), "\n")

# -------------------------------------------------------------------
# Find files with small file size
# -------------------------------------------------------------------
get_file_size <- function(dir, model, sizelim = 500) {
    tmp  <- list.files(dir, recursive = TRUE, full.name = TRUE)
    tmp  <- tmp[grep(sprintf("^%s_", model), basename(tmp))]
    size <- file.size(tmp)

    # Only consider small files from here on
    tmp  <- tmp[size <= sizelim]
    size <- size[size <= sizelim]

    # Returns TRUE if the file contains an error message 'not enough data'
    check_error_msg <- function(f) {
        tmp <- tryCatch(readRDS(f), error = function(e) { cat("Problem reading ", f, "\n");  stop(e) })
        if (is.list(tmp) && "error" %in% names(tmp)) grepl("^not enough data", tmp$error) else FALSE
    }
    notenoughdata <- sapply(tmp, check_error_msg)

    # Extracting station information
    cat("Folder", dir, "- found", length(tmp), "files\n")
    tmp  <- basename(tmp) # For regexp; basename only
    country = regmatches(tmp, regexpr("[a-z]+(?=_[0-9])", tmp, perl = TRUE))
    if (is.null(model)) {
    	station = as.integer(regmatches(tmp, regexpr("[0-9]{3,}(?=(_t))", tmp, perl = TRUE)))
    } else {
    	station = as.integer(regmatches(tmp, regexpr("(?!=[a-z]_)[0-9]{3,}", tmp, perl = TRUE)))
    }
    step    = as.integer(regmatches(tmp, regexpr("(?!=_)[0-9]{3}(?=\\.)", tmp, perl = TRUE)))

    # Putting everything back together
    res <- data.frame(country = country, station = station, step = step, size = size, notenoughdata = notenoughdata)
    return(res)
}

# Check what CSV files we have.
sizeinfo <- setNames(lapply(models, function(m) get_file_size("../results", model = m)), models)
cat("\n\n")

lapply(sizeinfo, function(x) table(x$notenoughdata, useNA = "always"))


# -------------------------------------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------
stop("--- that's basically all we need ---")
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# -------------------------------------------------------------------

# Check what CSV files we have.
for (n in names(sizeinfo)) {
    cat("Sizeinfo for ", n, "\n")
    print(with(sizeinfo[[n]], table(station, country)))
    print(with(sizeinfo[[n]], table(country, step)))
}
print(lapply(sizeinfo, dim))
cat("\n\n")

# -------------------------------------------------------------------
# Identify models
# -------------------------------------------------------------------
get_available_data <- function(dir = "../euppens", prefix = "euppens", param ="t2m") {
    require("dplyr")
    library("parallel")
    tmp <- list.files(dir, recursive = TRUE, full.name = TRUE)
    idx <- grep(sprintf("^%s_%s_.*(training|test)_[0-9]{3}\\.csv$", prefix, param), basename(tmp))
    tmp <- tmp[idx]
    cat("Folder", dir, "- found", length(tmp), " test/training csv files files\n")

    country = regmatches(tmp, regexpr("[a-z]+(?=_[0-9])", tmp, perl = TRUE))
    station = as.integer(regmatches(tmp, regexpr("[0-9]{3,}(?=(_t))", tmp, perl = TRUE)))
    step    = as.integer(regmatches(tmp, regexpr("(?!=_)[0-9]{3}(?=\\.)", tmp, perl = TRUE)))

    # Extracting countries, station ID and step
    res <- data.frame(file = basename(tmp), country = country, station = station,
                      step = step, training = grepl("_training_", basename(tmp)))

    # Scoping the rest
    fn <- function(i) {
        data <- read.csv(tmp[i])[, c("valid_time", sprintf("%s_obs", param), "ens_mean", "ens_sd")]
        data <- transform(data, year = as.integer(format(as.POSIXct(valid_time, tz = "UTC") - res$step[i] * 3600, format = "%Y")))
        data <- data[apply(data, 1, function(x) sum(is.na(x)) == 0), ]
        if (NROW(data) > 0) {
            data <- aggregate(valid_time ~ year, data = data, FUN = length)
            rval <- c(as.list(res[i, ]), setNames(as.list(data$valid_time), data$year))
        } else {
            rval <- as.list(res[i, ])
        }
        return(rval)
    }
    tmp_data <- mclapply(seq_len(NROW(res)), fn)

    # Separate training and test; glue otgether
    res_training <- as.data.frame(bind_rows(tmp_data[res$training]))
    res_test     <- as.data.frame(bind_rows(tmp_data[!res$training]))
    return(list(training = res_training, test = res_test))
}

rdsfile <- "check_available_data_results.rds"
cat("Data availability ...\n")
if (!file.exists(rdsfile)) {
    cat(" - File does not exist; calculate availability (mclapply)\n")
    avail <- get_available_data()
    saveRDS(avail, rdsfile)
} else {
    cat(" - File exists; loading only ...\n")
    avail <- readRDS(rdsfile)
}

print(lapply(avail, dim))

# --------------------------------------------------------------------
# Find training data info for the models where the file size is too
# small, i.e., no model has been estimated at all.
# --------------------------------------------------------------------
find_stupid_ones <- function(x, training, years = NULL) {
        xID <- with(x, interaction(country, station, step))
        tID <- with(training, interaction(country, station, step))
        idx <- match(xID, tID)
        stopifnot(all(!is.na(idx))) # Missing entries(!!!!!)
        res <- training[idx, ]

        # Calculate sum of observations on 'years in years'
        if (!is.null(years)) {
            col_idx <- c(which(!grepl("^[0-9]{4}$", names(res))), which(names(res) %in% as.character(years)))
            res <- res[, col_idx]
        }
        col_idx <- grep("^[0-9]{4}$", names(res))
        res$sum <- apply(res[, col_idx], 1, function(x) sum(x, na.rm = TRUE))
        return(res)
}
x <- find_stupid_ones(sizeinfo[[1]], avail$training)
x3 <- find_stupid_ones(sizeinfo[[1]], avail$training, 2015:2017)
head(x3)
head(x)

plot(x3$sum, ylim = c(0, 3*365))
abline(h = 3*365, col = "gray")






