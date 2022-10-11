

library("ncdf4") # Required later to load the data
library("argparse")

parser <- ArgumentParser(description = "Estimating ESSD benchmark models")
parser$add_argument("-s", "--station_id", type = "integer",
                    help = "Station ID, integer")
args <- parser$parse_args()
if (is.null(args$station_id)) { parser$print_help(); stop(2); }


# Single step
test  <- get_data(args$station_id, steps = 0, type = "test")
class(test)
head(test, n = 3)

# All steps in one data.frame
test  <- get_data(args$station_id, type = "test")
class(test)
head(test, n = 3)

idx <- grep("^t2m_m[0-9]{2}$", names(test))
test$ens_mean <- apply(test[, idx], 1, mean)
test$ens_sd   <- apply(test[, idx], 1, sd)


# --------------------------


library("ggplot2")
df <- transform(test, gg = factor(date_valid - step * 3600))
nlevels(df$gg)
ggplot(df) + geom_line(aes(x = step, y = t2m - t2m_m00, group = gg))

ggplot(test) + geom_point(aes(x = t2m, y = ens_mean)) + facet_wrap("step") +
    ggtitle("Test Data (50+1 member ENS)")

##################################################################

train  <- get_data(args$station_id, type = "train")
class(train)
head(train, n = 3)

idx <- grep("^t2m_m[0-9]{2}$", names(train))
train$ens_mean <- apply(train[, idx], 1, mean)
train$ens_sd   <- apply(train[, idx], 1, sd)

ggplot(test) + geom_point(aes(x = t2m, y = ens_mean)) + facet_wrap("step") +
    ggtitle("Training Data (10+1 member Hindcast)")

