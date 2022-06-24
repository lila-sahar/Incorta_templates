#' ---
#' title: "CLustering by Mapping"
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

# 3.0 PIPELINE ----

## K-Means Clustering: grouped by description

### import
grouped_df <- sdf_copy_to(sc, product_total_tbl, name = "brand_group_total_tbl", overwrite = TRUE) %>%
  group_by(description) %>%
  summarize(avg_price = mean(price),
            total_revenue = sum(revenue),
            transaction_count = n())

### pipeline
grouped_pipeline <- ml_pipeline(sc) %>%
  #ft_dplyr_transformer(tbl = grouped_df) %>%
  ft_vector_assembler(input_cols = c("avg_price", "total_revenue", "transaction_count"),
                                 output_col = "unscaled_features") %>%
  ft_standard_scaler(input_col = "unscaled_features",
                             output_col = "features") %>%
  ml_kmeans(k = 3)

### hyperparameter grid
grouped_grid <- list(
  kmeans = list(
    k = c(3, 4, 5, 6, 7, 8, 9, 10)
  )
)

### cross validator object
grouped_cv <- ml_cross_validator(sc,
                                 estimator = grouped_pipeline,
                                 estimator_param_maps = grouped_grid,
                                 evaluator = ml_clustering_evaluator(sc),
                                 num_folds = 3)

# ml_param_map(grouped_pipeline)

### single test
# ml_fit(grouped_pipeline, grouped_df)

#### this part is struggling with the group_by function
### train the model
grouped_cv_model <- ml_fit(grouped_cv, grouped_df)

### print the metrics
ml_validation_metrics(grouped_cv_model)

