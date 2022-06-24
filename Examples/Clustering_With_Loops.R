#' ---
#' title: "Clustering with Loops"
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

## K-Means Clustering - Transaction Data

### NOTE: Make these lists into a dataframe after the analysis
transaction_k_value <- c()
transaction_silhouette_coef <- c()
transaction_cluster_center <- c()

### when there is availability: we would like to calculate the standard deviation of
### the sample size of each cluster. another way to access if the cluster size is good.
# transaction_std_coef <- c()

### only one copy of the dataset
product_total <- sdf_copy_to(sc, product_total_tbl, name = "product_total", overwrite = TRUE) %>%
  select(description, move, volume, price, cost, revenue) %>%
  na.omit()

for (n_cluster in 2:3) {

  transaction_k_value[n_cluster] <- n_cluster

  product_total_kmeans <- product_total %>%
    ml_kmeans(formula = ~ price + move, k = n_cluster)

  product_total_kmeans_tbl <- ml_predict(product_total_kmeans, product_total)

  transaction_cluster_center <- c(transaction_cluster_center,
                                  product_total_kmeans)

  transaction_silhouette <- as.list(ml_compute_silhouette_measure(model = product_total_kmeans,
                                                                 dataset = product_total,
                                                                 distance_measure = c("squaredEuclidean", "cosine")))

  transaction_silhouette_coef <- c(transaction_silhouette_coef,
                                   transaction_silhouette)

  # this isn't counting data points within a cluster
  transaction_cluster_count <- product_total_kmeans_tbl %>%
    group_by(n_cluster) %>%
    tally()

  # transaction_std_coef <- c(transaction_std_coef,
  #                           transaction_cluster_count)

}


## K-Means Clustering - Grouped Data

### NOTE: Make these lists into a dataframe after the analysis
brand_k_value <- c()
brand_silhouette_coef <- c()
brand_cluster_center <- c()
# brand_std_coef <- c()

brand_group_total_tbl <- sdf_copy_to(sc, product_total_tbl, name = "brand_group_total_tbl", overwrite = TRUE) %>%
  group_by(description) %>%
  summarize(avg_price = mean(price),
            total_revenue = sum(revenue),
            transactional_count = n()) %>%
  na.omit()

for (n_cluster in 2:3) {

  brand_k_value[n_cluster] <- n_cluster

  brand_total_kmeans <- brand_group_total_tbl %>%
    ml_kmeans(formula = ~ avg_price + total_revenue + transactional_count, k = n_cluster) %>%
    na.omit()

  brand_total_kmeans_tbl <- ml_predict(brand_total_kmeans,
                                       brand_group_total_tbl) %>%
    select(description, prediction)

  ### this is rewritting values everytime it runs the amount of clusters
  brand_cluster_center <- c(brand_cluster_center,
                            brand_total_kmeans)

  brand_silhouette <- as.list(ml_compute_silhouette_measure(model = brand_total_kmeans,
                                                            dataset = brand_group_total_tbl,
                                                            distance_measure = c("squaredEuclidean", "cosine")))

  brand_silhouette_coef <- c(brand_silhouette_coef,
                             brand_silhouette)
}