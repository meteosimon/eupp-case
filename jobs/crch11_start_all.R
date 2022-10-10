#!/usr/bin/env Rscript


stopifnot(dir.exists("../euppens"))

csvfiles <- list.files("../euppens")
csvfiles <- csvfiles[grepl("euppens_t2m_[a-z]+_[0-9]+_[0-9]{3}\\.csv", csvfiles)]
print(csvfiles)

# Extracting country and station ID
data <- data.frame(country = regmatches(csvfiles, regexpr("[a-z]+(?=_[0-9])", csvfiles, perl = TRUE)),
                   station = as.integer(regmatches(csvfiles, regexpr("(?!=f_)[0-9]+(?=_)", csvfiles, perl = TRUE))))
data <- unique(data)

print(data)

for (i in seq_len(nrow(data))) {
    system(sprintf("sbatch crch11_run.R -c %s -s %d", data$country[i], data$station[i]))
}

