#!/usr/bin/env Rscript


# Find all suitable CSV files
stopifnot(dir.exists("../euppens"))
csvfiles <- list.files("../euppens")
csvfiles <- csvfiles[grepl("euppens_t2m_[a-z]+_[0-9]+_[a-z]+_[0-9]{3}\\.csv", csvfiles)]

# Extracting country and station ID
data <- data.frame(country = regmatches(csvfiles, regexpr("[a-z]+(?=_[0-9])", csvfiles, perl = TRUE)),
                   station = as.integer(regmatches(csvfiles, regexpr("(?!=f_)[0-9]+(?=_)", csvfiles, perl = TRUE))),
		   nbamlss = NA,
		   ncrch   = NA)
# Take unique country/station ID
data <- unique(data)

count_files <- function(x, model) {
	all_files <- list.files(sprintf("../results/%s/", model), recursive = TRUE)
	x[[model]] <- NA
	for (i in seq_len(NROW(x)))
	    x[[model]][i] <- sum(grepl(sprintf("_%s_%d_", x$country[i], x$station[i]), all_files))
	return(x)
}
data <- count_files(data, "bamlss")
data <- count_files(data, "crch")

#print(data)
cat("In total there are", nrow(data), "jobs to start\n")

# Start the job (array jobs)
####for (m in c("bamlss", "crch")) {
for (m in c("bamlss")) {
    for (i in seq_len(nrow(data))) {
        if (data[[m]][i] == 21) next
        cmd <- sprintf("qsub SGE_jobhandler.sh %s %s %d", m, data$country[i], data$station[i])
        print(cmd)
        system(cmd)
stop('x')
    }
}
