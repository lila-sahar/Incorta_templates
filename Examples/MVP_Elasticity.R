#' ---
#' title: "MVP: Elasticity"
#' author: "Lila Sahar"
#' output: github_document
#' ---
#' 

# PRICE ELASTICITY TEMPLATE ----

# 1.1 LIBRARIES ----

## standard

library(sparklyr)
library(dplyr)
library(broom)


# 1.2 DATA ----

## spark connection

sc <- spark_connect(master = "local", version = "3.0.3")

## read data

cereal_movement_tbl <- spark_read_csv(sc,
                                      name = "cereal_movement_tbl",
                                      path = "movement_cereal.csv")

cereal_upc_tbl <- spark_read_csv(sc,
                                 name = "cereal_upc_tbl",
                                 path = "upc_cereal.csv")

date_tbl <- spark_read_csv(sc,
                           name = "date_tbl",
                           path = "date_tbl.csv")


# 2.0 PREPROCESS DATA ----

# 2.1 - Cleaning Tibbles ----

## union of tibbles

product_detail_tbl <- cereal_movement_tbl %>%
  inner_join(cereal_upc_tbl, by = "UPC") %>%
  inner_join(date_tbl, by = "WEEK") %>%
  filter(MOVE > 0,
         QTY > 0,
         PRICE > 0,
         OK == 1) %>%
  select(store = STORE,
         upc = UPC,
         description = DESCRIP,
         week = WEEK,
         start = START,
         end = END,
         year = YEAR,
         profit = PROFIT,
         move = MOVE,
         quantity = QTY,
         price = PRICE,
         ok = OK) %>%
  mutate(revenue = price * move / quantity,
         volume = move / quantity,
         cost = as.integer(revenue - profit)) %>%
  filter(cost > 0)

product_lookup_tbl <- product_detail_tbl %>%
  group_by(description) %>%
  summarize(distinct_year = n_distinct(year),
            sample_size = n()) %>%
  filter(distinct_year == 9,
         # this is variable
         sample_size > 100)

product_total_tbl <- product_detail_tbl %>%
  inner_join(product_lookup_tbl, by = "description") %>%
  write.csv(., file = "..//Processed//product_total_tbl.csv")


# 3.0 MACHINE LEARNING MODEL ----

# 3.1 - Exploratory Data Analysis ----

data_splits <- sdf_random_split(product_total_tbl,
                                training = 0.7,
                                testing = 0.3,
                                seed = 42)
product_train <- data_splits$training
product_test <- data_splits$testing


### Note: Putting a pin in cross validation

# vfolds <- sdf_random_split(product_train,
#                            weights = purrr::set_names(rep(0.1, 10), paste0("fold", 1:10)),
#                            seed = 42)

# analysis_set <- do.call(rbind, vfolds[2:10])
# assessment_set <- vfolds[[1]]

make_scale_cost <- function(analysis_data) {

  scale_values <- analysis_data %>%
    summarize(
      mean_cost = mean(cost, na.rm = TRUE),
      sd_cost = sd(cost, na.rm = TRUE)
    ) %>%
    collect()

  function(data) {

    data %>%
      mutate(scaled_cost = (cost - !!scale_values$mean_cost) / !!scale_values$sd_cost)
  }
}

scale_cost <- make_scale_cost(product_train)
train_set <- scale_cost(product_train)
validation_set <- scale_cost(product_test)

# 3.2 - Linear Regression ----

lr_1 <- ml_linear_regression(train_set, price ~ scaled_cost)

validation_summary <- ml_evaluate(lr_1, validation_set)

# validation_summary$root_mean_squared_error

### Using the total dataset

total_scale_cost <- make_scale_cost(product_total_tbl)
total_set <- total_scale_cost(product_total_tbl)

lr_2 <- ml_linear_regression(total_set, price ~ scaled_cost)

glance_lr <- glance(lr_2)
tidy_lr <- tidy(lr_2)
augment_lr <- augment(lr_2)
