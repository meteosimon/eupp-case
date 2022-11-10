#!/usr/bin/env Rscript
# ---------------------------------------------------------
#SBATCH --ntasks=1
#SBATCH --array=0,6,12,18,24,30,36,42,48,54,60,66,72,78,84,90,96,102,108,114,120
#SBATCH --output=_job%A-%j.out
#SBATCH --error=_job%A-%j.err
# ---------------------------------------------------------

# ---------------------------------------------------------
# Parsing input args; used to control the job
# ---------------------------------------------------------
library("argparse")
parser <- ArgumentParser(description = "Controls job execution")
parser$add_argument("-s", "--station", type = "integer",
        help = "ID of the station to be processed")
parser$add_argument("-c", "--country", type = "character",
        help = "Name of the country the station is in")
args <- parser$parse_args()
stopifnot(is.character(args$country))
stopifnot(is.integer(args$station), args$station > 0L)
args$country <- match.arg(tolower(args$country), c("germany", "austria", "france", "switzerland", "netherlands"))


# ---------------------------------------------------------
# The step to be processed is handled by SLURM
# ---------------------------------------------------------
step <- Sys.getenv("SLURM_ARRAY_TASK_ID")
# THIS IS FOR TESTING ONLY
step <- ifelse(nchar(step) == 0, 0, as.integer(step))

# ---------------------------------------------------------
# Generate the input and output file name
# - csvfile:   input file
# - rdsfile:   output file
# ---------------------------------------------------------
dir      <- file.path("..", "euppens")
outdir   <- file.path("..", "results", "crch11", sprintf("%03d", step))
if (!dir.exists(outdir)) dir.create(outdir, recursive = TRUE)
csvfiles <- setNames(file.path("..", "euppens", sprintf("euppens_t2m_%s_%d_%s_%03d.csv", args$country,
                                                args$station, c("training", "test"), step)), c("training", "test"))
rdsfile  <- file.path(outdir, sprintf("crch11_euppens_t2m_%s_%d_%03d.rds", args$country, args$station, step))


# If the ouput file exists we can stop here
if (file.exists(rdsfile)) cat("Output file", rdsfile, "exists - skip.\n")

# Else we will read the test and training data sets
for (f in csvfiles) stopifnot(file.exists(f))
train <- tryCatch(read.csv(csvfiles["training"]),
                  warning = function(w) warning(w),
                  error = function(e) stop("Problems reading", csvfiles["training"]))
test  <- tryCatch(read.csv(csvfiles["test"]),
                  warning = function(w) warning(w),
                  error = function(e) stop("Problems reading", csvfiles["test"]))

# Estimate model
rows_with_na <- function(x, cols = c("valid_time", "yday", "t2m_obs", "ens_mean", "ens_sd")) {
        which(rowSums(is.na(x[, cols])) > 0)
}

na_train <- rows_with_na(train)
na_test  <- rows_with_na(test)

# Less than 20% of the data? Skip!
if (length(na_train) > (nrow(train) * .2)) stop("Too many missing values in training data set")


library("crch")
mod <- crch(t2m_obs ~ ens_mean | log(ens_sd), data = train, link.scale = "log", dist = "gaussian")
result_train <- cbind(train, predict(mod, type = "parameter", newdata = train))
result_test  <- cbind(test,  predict(mod, type = "parameter", newdata = test))

result <- list(training = result_train,
               test     = result_test,
               model    = mod,
               packages = list(crch = packageVersion("crch")))
saveRDS(result, rdsfile)






