#!/usr/bin/env Rscript
# ---------------------------------------------------------
# Slurm config
#SBATCH --ntasks=1
#SBATCH --array=0,6,12,18,24,30,36,42,48,54,60,66,72,78,84,90,96,102,108,114,120
#SBATCH --output=_job%A-%j.out
#SBATCH --error=_job%A-%j.err
# ---------------------------------------------------------
# SGE config is done by another job outside
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

# Has an interactive mode for testing
args <- if (interactive()) list(station = 11312, country = "austria") else parser$parse_args()

stopifnot(is.character(args$country))
stopifnot(is.integer(args$station), args$station > 0L)
args$country <- match.arg(tolower(args$country), c("germany", "austria", "france", "switzerland", "netherlands"))


# ---------------------------------------------------------
# The step to be processed is handled by SLURM
# ---------------------------------------------------------
sgestep   <- Sys.getenv("SGE_TASK_ID")
slurmstep <- Sys.getenv("SLURM_ARRAY_TASK_ID")
if (!nchar(sgestep) == 0) {
    step <- (as.integer(sgestep) - 1) * 6
} else if (!nchar(slurmstep) == 0) {
    step <- as.integer(slurmstep)
} else {
    # JUST FOR TESTING
    step <- 6L
}
print(step)

# ---------------------------------------------------------
# Generate the input and output file name
# - csvfile:   input file
# - rdsfile:   output file
# ---------------------------------------------------------
dir      <- file.path("..", "euppens")
outdir   <- file.path("..", "results", "bamlss", sprintf("%03d", step))
if (!dir.exists(outdir)) dir.create(outdir, recursive = TRUE)
csvfiles <- setNames(file.path("..", "euppens", sprintf("euppens_t2m_%s_%d_%s_%03d.csv", args$country,
                                                args$station, c("training", "test"), step)), c("training", "test"))
rdsfile  <- file.path(outdir, sprintf("bamlss_euppens_t2m_%s_%d_%03d.rds", args$country, args$station, step))

# If the ouput file exists we can stop here
if (file.exists(rdsfile)) {
    cat("Output file", rdsfile, "exists - skip.\n")
} else {
    cat("Output file", rdsfile, "not yet on disc, estimate model ...\n")

    # Else we will read the test and training data sets
    for (f in csvfiles) stopifnot(file.exists(f))
    train <- tryCatch(read.csv(csvfiles["training"]),
                      warning = function(w) warning(w),
                      error = function(e) stop("Problems reading", csvfiles["training"]))
    test  <- tryCatch(read.csv(csvfiles["test"]),
                      warning = function(w) warning(w),
                      error = function(e) stop("Problems reading", csvfiles["test"]))
    
    # ---------------------------------------------------------
    # Missing values
    # ---------------------------------------------------------
    rows_with_na <- function(x, cols = c("valid_time", "yday", "t2m_obs", "ens_mean", "ens_sd")) rowSums(is.na(x[, cols])) > 0
    na_train     <- rows_with_na(train)
    na_test      <- rows_with_na(test)
    cat("Number of rows with missing values ", sum(na_train), " (traning) ", sum(na_test), " (test)\n")
    if (sum(na_train) > (nrow(train) * .2)) stop("Too many missing values in training data set")
    
    # ---------------------------------------------------------
    # Estimating the model
    # ---------------------------------------------------------
    train <- transform(train, log_ens_sd = log(ens_sd), ens_sd = NULL)
    test  <- transform(test,  log_ens_sd = log(ens_sd), ens_sd = NULL)
    
    # Model formula
    f <- list(
        t2m_obs ~ s(yday, bs = "cc") + s(yday, bs = "cc", by = ens_mean),
                ~ s(yday, bs = "cc") + s(yday, bs = "cc", by = log_ens_sd)
    )
    set.seed(args$station * 1e3 + step) # for reproducibility
    
    mod <- tryCatch(bamlss(f, data = train[!na_train, ],
                           verbose = FALSE, n.iter = 12000, burnin = 2000, thin = 10, quiet = TRUE, light = TRUE),
                    error = function(e) stop("Problems estimating the bamlss model: ", e))
    
    # ---------------------------------------------------------
    # Calculate effects as we would like to store them for now
    # ---------------------------------------------------------
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
    effects <- predict_effects(mod)
    
    
    # ---------------------------------------------------------
    # Make prediction (training and test)
    # ---------------------------------------------------------
    append_prediction <- function(x, na, mod) {
        tmp <- predict(mod, newdata = x[!na,])
        for (n in names(tmp)) {
                x[, n]    <- NA
                x[!na, n] <- tmp[[n]]
        }
        return(x)
    }
    res_train <- append_prediction(train, na_train, mod)
    res_test  <- append_prediction(test,  na_test,  mod)
        
    result  <- list(training = res_train,
                    test     = res_test,
                    model    = mod,
                    effects = as.data.frame(effects),
                    packages = list(bamlss = packageVersion("bamlss")))
    saveRDS(result, rdsfile)
} 
