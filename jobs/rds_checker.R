#!/usr/bin/env Rscript
# -------------------------------------------------------------------
# Simple rds/csv checker
#
# 2022-10-10, Reto
# -------------------------------------------------------------------


# Find RDS files
rdsfiles <- list.files("../euppens_rds")
models <- sort(unique(regmatches(rdsfiles, regexpr("^.*?(?=_)", rdsfiles, perl = TRUE))))
cat("Models found:", paste(models, collapse = ", "), "\n")

get_file_info <- function(dir, model = NULL) {
    tmp <- list.files(dir)
    if (!is.null(model)) {
       tmp <- tmp[grepl(paste("^", model, sep = ""), tmp)]
    }
    # Extracting countries, station ID and step
    res <- list(country = regmatches(tmp, regexpr("[a-z]+(?=_[0-9])", tmp, perl = TRUE)),
                station = as.integer(regmatches(tmp, regexpr("(?!=f_)[0-9]+(?=_[0-9]{3}\\.)", tmp, perl = TRUE))),
                step    = as.integer(regmatches(tmp, regexpr("(?!=_)[0-9]{3}(?=\\.)", tmp, perl = TRUE))))
    res <- aggregate(step ~ location, data = transform(as.data.frame(res), location = interaction(country, station)), length)
    names(res)[length(res)] <- if (is.null(model)) "CSVs" else model
    return(res)
}

# Check what CSV files we have.
csvinfo <- get_file_info("../euppens")

# Now check what RDS files we have; split by 'models'
rdsinfo <- lapply(models, get_file_info, dir = "../euppens_rds")

# Final result
res <- csvinfo
for (rec in rdsinfo) res <- merge(res, rec, all = TRUE)
print(res)

