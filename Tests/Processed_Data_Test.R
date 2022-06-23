#' ---
#' title: "Test: Processed Data"
#' author: "Lila Sahar"
#' output: github_document
#' ---
#' 

source("..//..//Modules//Processed_Data.R")

get_union(cereal_movement_tbl)

get_product(get_union(cereal_movement_tbl))

get_product_details(get_union(cereal_movement_tbl), get_product(get_union(cereal_movement_tbl)))
