#' ---
#' title: "Test: Processed Data"
#' author: "Lila Sahar"
#' output: github_document
#' ---
#' 

source("..//..//Modules//Processed_Data.R")

product_detail_tbl <- get_union(cereal_movement_tbl)
print(product_detail_tbl)

product_lookup_tbl <- get_product(product_detail_tbl)
print(product_lookup_tbl)

product_total_tbl <- get_product_details(product_detail_tbl, product_lookup_tbl)
print(product_total_tbl)
