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
dir     <- file.path("..", "euppens")
csvfile <- file.path("..", "euppens", sprintf("euppens_t2m_%s_%d_%03d.csv", args$country, args$station, step))
rdsfile <- file.path("..", "euppens_rds", sprintf("crch11_euppens_t2m_%s_%d_%03d.rds", args$country, args$station, step))
if (!dir.exists("../euppens_rds")) dir.create("../euppens_rds")

# If the csvfile does not exist - ignore
# If the rdsfile does already exist - ignore as well
# Else we do the job
if (!file.exists(csvfile)) {
    stop("Input file", csvfile, "missing\n")
} else if (file.exists(rdsfile)) {
    cat("Output file", rdsfile, "exists - skip.\n")
} else {
    df <- read.csv(csvfile)
    df <- subset(df, select = c(valid_time, yday, t2m_obs, ens_mean, ens_sd))
    df <- subset(df, !is.na(t2m_obs) & !is.na(yday) & !is.na(ens_mean) & !is.na(ens_sd))
    print(head(df, n = 3))

    library("crch")
    mod <- crch(t2m_obs ~ ens_mean | log(ens_sd), data = df, link.scale = "log", dist = "gaussian")
    res <- cbind(df, predict(mod, type = "parameter"))
    print(head(res))

    result <- list(data = res, model = mod, packages = list(crch = packageVersion("crch")))
    saveRDS(result, rdsfile)
}






