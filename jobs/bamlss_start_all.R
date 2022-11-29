#!/usr/bin/env Rscript


# Find all suitable CSV files
stopifnot(dir.exists("../euppens"))
csvfiles <- list.files("../euppens")
csvfiles <- csvfiles[grepl("euppens_t2m_[a-z]+_[0-9]+_[a-z]+_[0-9]{3}\\.csv", csvfiles)]

# Extracting country and station ID
data <- data.frame(country = regmatches(csvfiles, regexpr("[a-z]+(?=_[0-9])", csvfiles, perl = TRUE)),
                   station = as.integer(regmatches(csvfiles, regexpr("(?!=f_)[0-9]+(?=_)", csvfiles, perl = TRUE))),
		   nrds = NA)

# Take unique country/station ID
data <- unique(data)

all_files <- list.files("../results/bamlss/", recursive = TRUE)
for (i in seq_len(NROW(data)))
    data$nrds[i] <- sum(grepl(sprintf("_%s_%d_", data$country[i], data$station[i]), all_files))

print(data)

# Start the job (array jobs)
for (i in seq_len(nrow(data))) {
    if (data$nrds[i] == 21) next
    #print(sprintf("sbatch bamlss_run.R -c %s -s %d", data$country[i], data$station[i]))
    system(sprintf("sbatch bamlss_run.R -c %s -s %d", data$country[i], data$station[i]))
}

