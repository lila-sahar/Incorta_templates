#' ---
#' title: "Spark Template: Elasticity"
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

## look-up table: cereal tibble

### currently does not work

### hard coded to not need rn

# cereal_movement_tbl %>%
  # mutate(description = case_when(
           # description == `~NUTRI GRAIN ALMOND` ~ "Nutri Grain Almond",
           # description == "APPLE CINNAMON CHEER" ~ "Apple Cinnamon Cheerios",
           # description == "BITE SIZE FRSTD MINI" ~ "Bite Size Frosted Mini-Wheats",
           # description == "CAP'N CRUNCH CHRISTM" ~ "Cap'n Crunch Christmas",
           # description == "CAPN CRUNCH CEREAL" ~ "Cap'n Crunch",
           # description == "CAPN CRUNCH JUMBO CR" ~ "Cap'n Crunch Jumbo",
           # description == "CAPTAIN CRUNCH JUMBO" ~ "Cap'n Crunch Jumbo",
           # description == "CHEERIOS" ~ "Cheerios",
           # description == "CINNAMON TOAST CRUNC" ~ "Cinnamon Toast Crunch",
           # description == "COCOA PUFFS" ~ "Cocoa Puffs",
           # description == "COOKIE CRISP CHOCOLA" ~ "Cookie Crisp",
           # description == "COUNT CHOCULA CEREAL" ~ "Count Chocula",
           # description == "CRISPY WHEATS & RAIS" ~ "Crispy Wheats & Raisins",
           # description == "DOM OAT HON RAISIN" ~ "Dominick's Oat Honey Raisin",
           # description == "DOM OAT HONEY" ~ "Dominick's Oat Honey",
           # description == "G.M. FIBER ONE" ~ "Fiber One",
           # description == "GENERAL MILLS KABOOM" ~ "Kaboom",
           # description == "GOLDEN GRAHAMS" ~ "Golden Grahams",
           # description == "HONEY BUNCHES OATS-A" ~ "Honey Bunches of Oats with Almonds",
           # description == "HONEY BUNCHES OATS R" ~ "Honey Bunches of Oats with Raisins",
           # description == "HONEY NUT CHEERIOS" ~ "Honey Nut Cheerios",
           # description == "KELL FROST MINI WHTS" ~ "Frosted Mini-Wheats",
           # description == "KELLOGG'S CORN POPS" ~ "Corn Pops",
           # description == "KELLOGG'S CRACKLIN O" ~ "Cracklin' Oat Bran",
           # description == "KELLOGG'S CRISPIX" ~ "Crispix",
           # description == "KELLOGG'S FROSTED FL" ~ "Frosted Flakes",
           # description == "KELLOGG'S RAISIN BRA" ~ "Raisin Bran",
           # description == "KELLOGG FROOT LOOPS" ~ "Froot Loops",
           # description == "KELLOGG FROSTED FLAK" ~ "Frosted Flakes",
           # description == "KELLOGGS ALL BRAN" ~ "All-Bran",
           # description == "KELLOGGS APPLE CINNA" ~ "Special K Apple Cinnamon",
           # description == "KELLOGGS APPLE JACKS" ~ "Apple Jacks",
           # description == "KELLOGGS BLUEBERRY S" ~ "Special K Blueberry",
           # description == "KELLOGGS COCOA KRISP" ~ "Cocoa Krispies",
           # description == "KELLOGGS CORN FLAKE" ~ "Corn Flakes",
           # description == "KELLOGGS CORN FLAKES" ~ "Corn Flakes",
           # description == "KELLOGGS CORN POPS" ~ "Corn Pops",
           # description == "KELLOGGS CRISPIX" ~ "Crispix",
           # description == "KELLOGGS FRUIT LOOPS" ~ "Froot Loops",
           # description == "KELLOGGS HONEY SMACK" ~ "Honey Smacks",
           # description == "KELLOGGS JUST RT FRT" ~ "Just Right",
           # description == "KELLOGGS NUT & HONEY" ~ "Nut & Honey Crunch",
           # description == "KELLOGGS NUTRI GRAIN" ~ "Nutri-Grain",
           # description == "KELLOGGS PRODUCT 19" ~ "Product 19",
           # description == "KELLOGGS RAISIN BRAN" ~ "Raisin Bran",
           # description == "KELLOGGS RICE KRISPI" ~ "Rice Krispies",
           # description == "KELLOGGS SPECIAL K" ~ "Special K",
           # description == "KELLOGGS STRAWBERRY" ~ "Special K Strawberry",
           # description == "KELLOGGS SUGAR FROST" ~ "Frosted Flakes",
           # description == "KELLOGGS VARIETY PAC" ~ "Kellogg's Variety Pack",
           # description == "KELLOGS RAISIN SQUAR" ~ "Raisin Squares",
           # description == "KIX" ~ "Kix",
           # description == "KLG JST RT FIBER NGT" ~ "Just Right",
           # description == "KLLGG SGR FRSTD MINI" ~ "Frosted Mini-Wheats",
           # description == "LUCKY CHARMS" ~ "Lucky Charms",
           # description == "NAB SPOON SIZE SHRED" ~ "Spoon Size Shredded Wheat",
           # description == "NABISCO SHREDDED WHE" ~ "Shredded Wheat",
           # description == "NABISCO WHEAT N BRAN" ~ "Wheat 'N Bran",
           # description == "OATMEAL RAISIN CRISP" ~ "Oatmeal Crisp",
           # description == "POPEYE PUFFED RICE" ~ "Popeye Puffed Rice",
           # description == "POPEYE PUFFED WHEAT" ~ "Popeye Puffed Wheat",
           # description == "POST ALPHA BITS" ~ "Alpha-Bits",
           # description == "POST COCOA PEBBLES" ~ "Cocoa PEBBLES",
           # description == "POST FRT&FIBRE DATE/" ~ "Fruit & Fibre",
           # description == "POST FRUITY PEBBLES" ~ "Fruity PEBBLES",
           # description == "POST GRAPE-NUTS CERE" ~ "Grape-Nuts",
           # description == "POST GRAPE NUTS" ~ "Grape-Nuts",
           # description == "POST GRAPENUT FLAKES" ~ "Grape-Nuts Flakes",
           # description == "POST HONEY COMB" ~ "Honeycomb",
           # description == "POST NATURAL RAISIN" ~ "Natural Raisin Bran",
           # description == "POST RAISIN BRAN" ~ "Post Raisin Bran",
           # description == "POST SUGAR CRISP" ~ "Sugar Crisp",
           # description == "POST/NATURAL BRAN FL" ~ "Natural Bran Flakes",
           # description == "QUAKER 100% CEREAL H" ~ "100% Natural Cereal",
           # description == "QUAKER 100% NATURAL" ~ "100% Natural Cereal",
           # description == "QUAKER LIFE CEREAL" ~ "Life",
           # description == "QUAKER LIFE CINNAMON" ~ "Life Cinnamon",
           # description == "QUAKER OAT SQUARES" ~ "Oatmeal Squares",
           # description == "QUAKER P.B. CAPTAIN" ~ "Peanut Butter Squares",
           # description == "QUAKER PUFFED RICE" ~ "Puffed Rice",
           # description == "QUAKER RTE OAT BRAN" ~ "Oat Bran",
           # description == "RALSTON CORN CHEX" ~ "Corn Chex",
           # description == "RALSTON RICE CHEX" ~ "Rice Chex",
           # description == "RALSTON/WHEAT CHEX" ~ "Wheat Chex",
           # description == "TOTAL RAISIN BRAN" ~ "Total Raisin Bran",
           # description == "SMORES CRUNCH CEREAL" ~ "S'mores Crunch",
           # description == "TOTAL" ~ "Total",
           # description == "TOTAL CORN FLAKES" ~ "Total Corn Flakes",
           # description == "TRIX" ~ "Trix",
           # description == "WHEATIES" ~ "Wheaties",
           # description == "WHOLE GRAIN TOTAL" ~ "Total Whole Grain",
           # TRUE ~ "Other"))

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
  inner_join(product_lookup_tbl, by = "description")


# 3.0 MACHINE LEARNING MODEL ----

# 3.1 - Exploratory Data Analysis ----

data_splits <- sdf_random_split(product_total_tbl,
                                training = 0.7,
                                testing = 0.3,
                                seed = 42)
product_train <- data_splits$training
product_test <- data_splits$testing


# Note: Putting a pin in cross validation

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

# Using the total dataset

total_scale_cost <- make_scale_cost(product_total_tbl)
total_set <- total_scale_cost(product_total_tbl)

lr_2 <- ml_linear_regression(total_set, price ~ scaled_cost)

glance_lr <- glance(lr_2)
tidy_lr <- tidy(lr_2)
augment_lr <- augment(lr_2)


# 4.0 PIPELINE ----

## K-Means Clustering

# fpa measures - total revenue, gross margin, average selling price (look at os example) & stand alone app
# don't choose values that are inter related

### for transaction data
### NOTE: Make these lists into a dataframe after the analysis
transaction_k_value <- c()

transaction_silhouette_coef <- c()

transaction_cluster_center <- c()

transaction_std_coef <- c()

for (n_cluster in 2:10) {
  
  transaction_k_value[n_cluster] <- n_cluster
  
  product_total_kmeans <- sdf_copy_to(sc, product_total_tbl, name = "product_total_tbl", overwrite = TRUE) %>%
    select(description, move, volume, price, cost, revenue) %>%
    ml_kmeans(formula = ~ price + move, k = n_cluster) %>%
    na.omit()
  
  transaction_cluster_center <- c(transaction_cluster_center,
                                  product_total_kmeans)
  
  # this works as a vector but I want it in list format
  transaction_silhouette <- c(transaction_silhouette, ml_compute_silhouette_measure(model = product_total_kmeans,
                                                 dataset = product_total_tbl,
                                                 distance_measure = c("squaredEuclidean", "cosine")))
  
  transaction_silhouette_coef <- c(transaction_silhouette_coef,
                                        transaction_silhouette)
  
  # doesn't like this
  #transaction_std_coef <- c(transaction_std_coef,
  #                          product_total_kmeans %>%
  #                            count())
    
}

# for groups of brands
### NOTE: Make these lists into a dataframe after the analysis
brand_k_value <- c()

brand_silhouette_coef <- c()

brand_cluster_center <- c()

brand_std_coef <- c()

brand_group_total_tbl <- product_total_tbl %>%
  group_by(description) %>%
  summarize(avg_price = mean(price),
            total_revenue = sum(revenue),
            transactional_count = n())
  

for (n_cluster in 2:10) {
  
  brand_k_value[n_cluster] <- n_cluster
  
  brand_total_kmeans <- sdf_copy_to(sc, brand_group_total_tbl, name = "brand_group_total_tbl", overwrite = TRUE) %>%
    ml_kmeans(formula = ~ avg_price + total_revenue + transactional_count, k = n_cluster) %>%
    na.omit()
  
  brand_cluster_center <- c(brand_cluster_center,
                                  brand_total_kmeans)
  
  # this works as a vector but I want it in list format
  brand_silhouette <- c(brand_silhouette, ml_compute_silhouette_measure(model = brand_total_kmeans,
                                                                                    dataset = brand_group_total_tbl,
                                                                                    distance_measure = c("squaredEuclidean", "cosine")))
  
  brand_silhouette_coef <- c(brand_silhouette_coef,
                                   brand_silhouette)
  
  # doesn't like this
  #brand_std_coef <- c(brand_std_coef,
  #                          brand_total_kmeans %>%
  #                            count())
  
}


## Define the pipeline
products_pipeline <- ml_pipeline(sc) %>%
  ft_dplyr_transformer(tbl = product_total_tbl) %>%
  ft_vector_assembler(input_cols = c("description", "cost"),
                      output_col = "features") %>%
  ft_standard_scaler(input_col = "features",
                     output_col = "features_scaled",
                     with_mean = TRUE) %>%
  ft_r_formula(price ~ cost) %>%
  ml_linear_regression()

fitted_pipeline <- ml_fit(products_pipeline,
                          data_splits$training)

predictions <- ml_transform(fitted_pipeline,
                            data_splits$testing)
  

### Convert the ml_pipeline() object --- import libraries, import csv, everything afterwards should be one pipeline
### Save the ml_pipeline object (batch processing)



