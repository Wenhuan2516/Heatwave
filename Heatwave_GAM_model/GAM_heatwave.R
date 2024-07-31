library(mgcv)

### Yearly-county level
suicide_heatwave <- read.csv('~/Downloads/yearly_GAM_heatwave_state_1960_2019_trail2.csv')

#suicide_heatwave$summer_winter <- as.factor(suicide_heatwave$summer_winter)
model_group <- function(df) {
  model <- gam(suicide_rate ~ s(heatwave_count), data = df)
  return(model)
}

suicide_nonzero <- suicide_heatwave[suicide_heatwave$heatwave_count != 0, ]
suicide_under10 <- suicide_heatwave[suicide_heatwave$heatwave_count < 10, ]
suicide_over10 <- suicide_heatwave[suicide_heatwave$heatwave_count >= 10, ]
model_heatwave <- model_group(suicide_nonzero)
group <- 'heatwave'

plot(model_heatwave, select=1, rug=TRUE,main=paste("Effect of Heatwave on Suicide Rate - ", group),
     xlab = 'Yearly Heatwave Frequency', ylab = 'Change in Monthly Suicide Rate', ylim = c(-8, 8))
abline(h=0, col='red', lty =1)


### Yearly - State level
#suicide_heatwave <- read.csv('~/Downloads/yearly_GAM_heatwave_state_1960_2019_trail2.csv')
#suicide_heatwave <- read.csv('~/Downloads/county_to_month_heatwave_days_1960_2019.csv')
suicide_heatwave <- read.csv('~/Downloads/monthly_GAM_heatwave_days_state_1960_2019_final.csv')
#suicide_heatwave <- read.csv('~/Downloads/yearly_GAM_heatwave_days_state_1960_2019.csv')
library(dplyr)
# Assuming your dataframe is named df
#suicide_heatwave <- subset(suicide_heatwave, !is.na(heatwave_count))

#suicide_heatwave$summer_winter <- as.factor(suicide_heatwave$summer_winter)
model_group <- function(df) {
  model <- gam(suicide_rate ~ s(heatwave_days), data = df)
  return(model)
}

suicide_nonzero <- suicide_heatwave[suicide_heatwave$heatwave_days != 0, ]
heatwave_over2000 <- subset(suicide_heatwave, year >= 2000 & year <= 2019)
heatwave_under2000 <- subset(suicide_heatwave, year >= 1960 & year <= 1999)

#heatwave_southeast <- suicide_heatwave[suicide_heatwave$region == 'Sountheast', ]

model_heatwave <- model_group(heatwave_over2000)
group <- 'heatwave'

plot(model_heatwave, select=1, rug=TRUE,main=paste("Effect of Heatwave on Suicide Rate - ", group),
     xlab = 'monthly Heatwave days', ylab = 'Change in monthly Suicide Rate', ylim = c(-1, 1))
abline(h=0, col='red', lty =1)

count_min <- min(heatwave_over2000$heatwave_days)
print(count_min)
count_max <- max(heatwave_over2000$heatwave_days)
print(count_max)



data <- load("~/Downloads/PNW_JJA_maxima_with_covariates.RData")

# function to calculate percentage increase
calculate_slope_details <- function(x1, y1, y1_ci_upper, y1_ci_lower, x2, y2, y2_ci_upper, y2_ci_lower, base_rate) {
  # Calculate the slope
  slope <- (y2 - y1) / (x2 - x1)
  
  # Calculate the standard error of y1 and y2 using the CI upper and lower bounds
  y1_se <- (y1_ci_upper - y1_ci_lower) / (2 * 1.96)
  y2_se <- (y2_ci_upper - y2_ci_lower) / (2 * 1.96)
  
  # Propagate error for the slope using the standard error of y1 and y2
  slope_se <- sqrt(y1_se^2 + y2_se^2) / (x2 - x1)
  
  # Calculate the 95% CI for the slope
  slope_ci_upper <- slope + (1.96 * slope_se)
  slope_ci_lower <- slope - (1.96 * slope_se)
  
  # Calculate the percentage increase based on the base rate
  percentage_increase <- (slope / base_rate) * 100
  # Calculate the standard error of the percentage increase (propagating from the slope's SE)
  percentage_increase_se <- (slope_se / base_rate) * 100
  
  # Calculate the 95% CI for the percentage increase
  percentage_increase_ci_upper <- percentage_increase + (1.96 * percentage_increase_se)
  percentage_increase_ci_lower <- percentage_increase - (1.96 * percentage_increase_se)
  
  # Return a list containing the slope, CI for slope, percentage increase, and CI for percentage increase
  return(list(
    slope = slope,
    slope_ci_upper = slope_ci_upper,
    slope_ci_lower = slope_ci_lower,
    percentage_increase = percentage_increase,
    percentage_increase_ci_upper = percentage_increase_ci_upper,
    percentage_increase_ci_lower = percentage_increase_ci_lower
  ))
}

# add horizonal line
add_horizon_line <- function(estimated_y) {
  abline(h = estimated_y, col = "red", lty = 1)
}

# add vertical line
add_vertical_line <- function(estimated_x) {
  x_at_y0 <- estimated_x # This value is estimated from the plot
  abline(v = x_at_y0, col = "blue", lty = 2)
  #unit <- '°C'
  text(x = x_at_y0, y = 0, labels = paste("x =", x_at_y0), pos = 3)
}

model_heatwave <- model_group(heatwave_over2000)
group <- 'heatwave'

plot(model_heatwave, select=1, rug=TRUE,main=paste("Effect of Heatwave on Suicide Rate - ", group),
     xlab = 'monthly Heatwave days', ylab = 'Change in monthly Suicide Rate', ylim = c(-1, 1))
abline(h=0, col='red', lty =1)
#add_horizon_line(0.275)
#add_horizon_line(0.36)
#add_horizon_line(0.185)
#add_horizon_line(-0.01)
add_vertical_line(1)

print_base_rate <- function(df) {
  print(mean(df$suicide_rate))
  print(round(min(df$heatwave_days)))
  print(round(max(df$heatwave_days)))
}

print_base_rate(heatwave_over2000)

# The maan suicide rate of monthly state level is 1.30

calculate_slope_details(0, -0.01, -0.01, -0.01, 29, 0.275, 0.36, 0.185, 1.30)


# Load the mgcv package
library(mgcv)

# Simulate some data for demonstration
set.seed(123)
n <- 100
x <- runif(n, min=-10, max=30)
y <- sin(x) + rnorm(n)

# Fit the GAM model
gam_model <- gam(y ~ s(x))

# Define the x values for which you want predictions
x_values <- data.frame(heatwave_days = c(0, 10, 20))

# Predict the y values and standard errors at the specified x values
predictions <- predict(model_heatwave, newdata = x_values, se.fit = TRUE)

# Extract the fitted values and standard errors
fitted_values <- predictions$fit
standard_errors <- predictions$se.fit

# Calculate the confidence intervals (95% CI)
conf_intervals <- data.frame(
  x = x_values$x,
  fitted = fitted_values,
  lower_ci = fitted_values - 1.96 * standard_errors,
  upper_ci = fitted_values + 1.96 * standard_errors
)

print(conf_intervals)

plot(model_heatwave, select=1, rug=TRUE,main=paste("Effect of Heatwave on Suicide Rate - ", group),
     xlab = 'monthly Heatwave days', ylab = 'Change in monthly Suicide Rate', ylim = c(-1, 1))
abline(h=0, col='red', lty =1)
