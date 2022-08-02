# Created By: Lila W Sahar
# Created Date: 07/29/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" This script is made to take a Materialized View and use it for price optimization """ 
# ---------------------------------------------------------------------------

# Imports
from pyspark.sql.functions import mean
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# Data Load
processed_data = read('Price_Elasticity.Data_Processed')
lr_data = read('Price_Elasticity.LinearRegression')

# Grouping
processed_data = processed_data.groupBy('ProductID', 'ProductName') \
    .agg(mean('UnitPrice').alias('AvgPrice_1'), mean('OrderQuantity').alias('AvgQuantity'))

# Join
data = lr_data.join(processed_data, lr_data.AvgPrice == processed_data.AvgPrice_1) \
    .drop('AvgPrice_1')

save(data)

# take the output of the linear regression to map the output on a demand curve (quantity on price aka quantity with linear regression output)
# then map z = 1 where z is elasticity because that is when MR = MC
# this should give you the optimal price for a product