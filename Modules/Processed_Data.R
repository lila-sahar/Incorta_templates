#' ---
#' title: "Processed Data: Elasticity"
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

