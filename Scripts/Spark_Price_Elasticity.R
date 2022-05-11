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

### Note: You still have two other files to read into the spark cluster.

# 2.0 PREPROCESS DATA ----

# 2.1 - Cleaning Tibbles ----

## look-up table: cereal tibble

### currently does not work

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
  select(store = STORE,
         upc = UPC,
         description = DESCRIP,
         week = WEEK,
         start = START,
         end = END,
         year = YEAR,
         move = MOVE,
         quantity = QTY,
         price = PRICE,
         ok = OK) %>%
  mutate(revenue = price * move / quantity,
         volume = move / quantity) %>%
  filter(move > 0,
         quantity > 0,
         price > 0,
         ok == 1)

product_lookup_tbl <- product_detail_tbl %>%
  group_by(description) %>%
  summarize(distinct_year = n_distinct(year),
            sample_size = n()) %>%
  filter(distinct_year == 9,
         # this is variable
         sample_size > 100)
  
product_total_tbl <- product_detail_tbl %>%
  inner_join(product_lookup_tbl, by = "description") %>%
  select(description, volume, price)
  

