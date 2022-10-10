#!/usr/bin/env Rscript
# ---------------------------------------------------------
#SBATCH --ntasks=1
#SBATCH --array=0,6,12,18,24,30,36,42,48,54,60,66,72,78,84,90,96,102,108,114,120
#SBATCH --output=_job%A-%j.out
#SBATCH --error=_job%A-%j.err
# ---------------------------------------------------------

library("argparse")
library("tidyverse")
library("bamlss")

# ---------------------------------------------------------
# Parsing input args; used to control the job
# ---------------------------------------------------------
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
step <- ifelse(nchar(step) == 0, 6, as.integer(step))

# ---------------------------------------------------------
# Generate the input and output file name
# - csvfile:   input file
# - rdsfile:   output file
# ---------------------------------------------------------
dir     <- file.path("..", "euppens")
csvfile <- file.path("..", "euppens", sprintf("euppens_t2m_%s_%d_%03d.csv", args$country, args$station, step))
rdsfile <- file.path("..", "euppens_rds", sprintf("bamlss_euppens_t2m_%s_%d_%03d.rds", args$country, args$station, step))
if (!dir.exists("../euppens_rds")) dir.create("../euppens_rds")

# If the csvfile does not exist - ignore
# If the rdsfile does already exist - ignore as well
# Else we do the job
if (!file.exists(csvfile)) {
    stop("Input file", csvfile, "missing\n")
} else if (file.exists(rdsfile)) {
    cat("Output file", rdsfile, "exists - skip.\n")
} else {
    # Reading training data set
    df <- subset(transform(read.csv(csvfile), log_ens_sd = log(ens_sd)),
                 select = c(valid_time, yday, t2m_obs, ens_mean, ens_sd, log_ens_sd))
    df <- subset(df, !is.na(t2m_obs) & !is.na(yday) & !is.na(ens_mean) & !is.na(log_ens_sd))

    # Estimating the model
    f <- list(
        t2m_obs ~ s(yday, bs = "cc") + s(yday, bs = "cc", by = ens_mean),
                ~ s(yday, bs = "cc") + s(yday, bs = "cc", by = log_ens_sd)
    )
    set.seed(args$station * 1e3 + step) # for reproducibility
    mod <- bamlss(f, data = df, verbose = FALSE, n.iter = 12000, burnin = 2000, thin = 10,
                  quiet = TRUE, light = TRUE)
    
    c95 <- function (x) {
        qx <- quantile(x, probs = c(0.025, 0.50, 0.975), na.rm = TRUE)
        return(setNames(c(qx[[1]], mean(x), qx[[3]]), c("lower", "mid", "upper")))
    }
    predict_effects <- function(mod) {
        # Setting up 'grid' (df) with effects to be computed
        args <- data.frame(term  = c("s(yday)", "s(yday,by=ens_mean)", "s(yday)", "s(yday,by=log_ens_sd)"),
                           param = rep(c("mu", "sigma"), each = 2),
                           coef  = paste("varying", rep(c("intercept", "coef"), times = 2), sep = "_"))
    
        # Compute effects
        res <- list()
        nd  <- data.frame(yday = 0:366, ens_mean = 1, log_ens_sd = 1)
        for (i in seq_len(NROW(args))) {
            res[[i]] <- predict(mod, nd, term = args$term[i], intercept = FALSE, type = "link", FUN = c95, model = args$param[i]) %>%
                        as_tibble() %>%
                        mutate(yday = nd$yday) %>%
                        pivot_longer(-yday, names_to = "prob") %>%
                        mutate(param = args$param[i], coef = args$coef[i])
        }
        return(bind_rows(res))
    }
    
    # Calculating estimated effects
    effects <- predict_effects(mod)
    res     <- cbind(df, as.data.frame(predict(mod)))
    result  <- list(data = res, effects = as.data.frame(effects), model = mod,
                    packages = list(bamlss = packageVersion("bamlss")))
    saveRDS(result, rdsfile)

}



