#!/usr/bin/env Rscript
# -------------------------------------------------------------------
# Simple rds/csv checker
#
# 2022-10-10, Reto
# -------------------------------------------------------------------


# Find RDS files
rdsfiles <- basename(list.files("../results", recursive = TRUE))
models <- sort(unique(regmatches(rdsfiles, regexpr("^.*?(?=_)", rdsfiles, perl = TRUE))))
cat("Models found:", paste(models, collapse = ", "), "\n")

get_file_info <- function(dir, model = NULL) {
    tmp <- basename(list.files(dir, recursive = TRUE))
    cat("Folder", dir, "- found", length(tmp), "files\n")
    if (!is.null(model)) {
       tmp <- tmp[grepl(paste("^", model, sep = ""), tmp)]
    }
    country = regmatches(tmp, regexpr("[a-z]+(?=_[0-9])", tmp, perl = TRUE))
    if (is.null(model)) {
    	station = as.integer(regmatches(tmp, regexpr("[0-9]{3,}(?=(_t))", tmp, perl = TRUE)))
    } else {
    	station = as.integer(regmatches(tmp, regexpr("(?!=[a-z]_)[0-9]{3,}", tmp, perl = TRUE)))
    }
    step    = as.integer(regmatches(tmp, regexpr("(?!=_)[0-9]{3}(?=\\.)", tmp, perl = TRUE)))

    # Extracting countries, station ID and step
    res <- list(country = country, station = station, step = step)
    res <- aggregate(step ~ location, data = transform(as.data.frame(res), location = interaction(country, station)), length)
    names(res)[length(res)] <- if (is.null(model)) "CSVs" else model
    return(res)
}

# Check what CSV files we have.
csvinfo <- get_file_info("../euppens")

# Now check what RDS files we have; split by 'models'
rdsinfo <- lapply(models, get_file_info, dir = "../results")

# Final result
res <- csvinfo
for (rec in rdsinfo) res <- merge(res, rec, all = TRUE)
print(res)

