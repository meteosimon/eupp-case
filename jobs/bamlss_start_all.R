#!/usr/bin/env Rscript


# Find all suitable CSV files
stopifnot(dir.exists("../euppens"))
csvfiles <- list.files("../euppens")
csvfiles <- csvfiles[grepl("euppens_t2m_[a-z]+_[0-9]+_[0-9]{3}\\.csv", csvfiles)]

# Extracting country and station ID
data <- data.frame(country = regmatches(csvfiles, regexpr("[a-z]+(?=_[0-9])", csvfiles, perl = TRUE)),
                   station = as.integer(regmatches(csvfiles, regexpr("(?!=f_)[0-9]+(?=_)", csvfiles, perl = TRUE))))
# Take unique country/station ID
data <- unique(data)

print(data)

# Start the job (array jobs)
for (i in seq_len(nrow(data))) {
    system(sprintf("sbatch bamlss_run.R -c %s -s %d", data$country[i], data$station[i]))
}

