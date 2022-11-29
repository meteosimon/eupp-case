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


for (i in 2:3) {
	cat("Column ", names(res)[i], "\n")
	print(table(res[, i], useNA = "always"))
}


# -------------------------------------------------------------------
# Detailed analysis
# -------------------------------------------------------------------
res_na <- subset(res, is.na(bamlss))

res_mi <- subset(res, (bamlss * 2) != CSVs)

extr_country_and_ID <- function(x) {
	stopifnot(is.data.frame(x))
	stopifnot("location" %in% names(x))
	x$country <- with(x, regmatches(location, regexpr("^.*(?=(\\.))", location, perl = TRUE)))
	x$ID <- as.integer(with(x, regmatches(location, regexpr("(?<=(\\.)).*$", location, perl = TRUE))))
	return(x)
}
res_na <- extr_country_and_ID(res_na)
res_mi <- extr_country_and_ID(res_mi)

print(res_na)
print(res_mi)


# Find missing files
find_missing <- function(country, ID, model, steps_expected = seq(0, 120, by = 6)) {
	files <- list.files(sprintf("../results/%s", model), recursive = TRUE)
	files <- files[grepl(sprintf("_%s_%d_", country, ID), files)]
	# Steps
	steps <- as.integer(regmatches(files, regexpr("^[0-9]+", files)))
	idx <- which(!steps_expected %in% steps)
	return(list(available = steps, missing = steps_expected[idx]))
}
foo <- rbind(res_na, res_mi)
for (i in seq_len(NROW(foo))) {
	tmp <- with(foo[i, ], find_missing(country, ID, "bamlss"))
	cat("For ", foo$country[i], " -- ", foo$ID[i], "      ", 
		"(", length(tmp$available), "/", length(tmp$available) + length(tmp$missing), ")\n")
	cat("    Available: ", paste(sprintf("%3d ", tmp$available), collapse = ""), "\n")
	cat("    Missing:   ", paste(sprintf("%3d ", tmp$missing), collapse = ""), "\n")
}






