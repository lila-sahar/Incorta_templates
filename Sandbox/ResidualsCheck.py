# Created By: Lila W Sahar
# Created Date: 08/10/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" Motivation: Does the price optimization algorithm need 2SLS Regession? """ 
# ---------------------------------------------------------------------------

## Set Environment Variables ------------------------------------------------
import findspark
findspark.init()

# Imports -------------------------------------------------------------------
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import FloatType

# LOCAL TEST ----------------------------------------------------------------
spark = SparkSession.builder \
                    .master('local') \
                    .appName('ResidualsCheck') \
                    .getOrCreate()

# Data Load -----------------------------------------------------------------
path = '../Data/Processed/Data_Processed.csv'
processed_data = spark.read.option('header', True).csv(path)

# ---------------------------------------------------------------------------
""" Purpose: Understand the correlation between `Cost`, `Price`, and `Quantity` """ 
# ---------------------------------------------------------------------------

# Grouping ------------------------------------------------------------------
processed_data = processed_data.groupBy('ProductID', 'ProductName') \
    .agg(mean('UnitPrice').alias('AvgPrice'), mean('StandardCost').alias('AvgCost'), mean('OrderQuantity').alias('AvgQuantity'))

# Correlation ---------------------------------------------------------------
cor_price_cost = processed_data.stat.corr('AvgPrice', 'AvgCost')
## Value: 0.9875077376814002
## Hence, the relationship between price and cost is positive and relatively strong.

# Data Exploration - Graphs ---------------------------------------------------
processed_data_pd = processed_data.select('AvgPrice','AvgCost', 'AvgQuantity').toPandas()
plt.scatter(processed_data_pd.AvgPrice, processed_data_pd.AvgCost)
plt.xlabel('Avg Price')
plt.ylabel('Avg Cost')
plt.title('Relation of Average Cost and Average Price')
plt.show()
## Strong Positive Linear Correlation

# Hypothesis Testing - Chi Squared Test ---------------------------------------------
assembler_price_cost = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
output_price_cost = assembler_price_cost.transform(processed_data)

chisq_price_cost = ChiSquareTest.test(output_price_cost, 'Features', 'AvgCost')
chisq_price_cost.show()
## pValues: 0.2449236107707391
## degreesOfFreedom: 68904
## statistics: 69160.00000002132
### Looking like a weak relationship based on the p-value: 0.245 > 0.1

# Linear Regressions ----------------------------------------------------------------
## assembler_price_cost = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
## output_price_cost = assembler_price_cost.transform(processed_data)

train_data, test_data = output_price_cost.randomSplit([0.7,0.3])
train_data.describe().show()

price_cost_lr = LinearRegression(featuresCol = 'Features', labelCol = 'AvgCost')
price_cost_model = price_cost_lr.fit(train_data)
price_cost_results = price_cost_model.evaluate(test_data)

print('Rsquared Error :', price_cost_results.r2)
## Rsquared Error: 0.97142579175477
### 97% of the regression model covers most part of the variance of the values of the response variable
print('Mean Squared Error :', price_cost_results.meanSquaredError)
## Mean Squared Error: 9909.195865593128
print('Root Mean Square Deviation :', price_cost_results.rootMeanSquaredError)
## Root Mean Square Deviation: 99.54494394791294
print('Mean Absolute Error :', price_cost_results.meanAbsoluteError)
## Mean Absolute Error: 69.99321365968038
print('Explained Variance :', price_cost_results.explainedVariance)
## Explained Variance: 347954.87035416556

price_cost_predictions = price_cost_model.transform(test_data)
price_cost_predictions.show()

# Residuals --------------------------------------------
price_cost_predictions = price_cost_predictions.withColumn('Residuals', col('AvgCost') - col('prediction'))
price_cost_predictions.summary()

# Correlation between Residuals and Variables -----------------------------

cor_residual_price = price_cost_predictions.stat.corr('Residuals', 'AvgPrice')
## Value: -0.09379009875804613
## Hence, the relationship between residuals and average price is negative and weak.

price_cost_pd = price_cost_predictions.toPandas()

plt.scatter(price_cost_pd.Residuals, price_cost_pd.AvgQuantity)
plt.xlabel('Residuals')
plt.ylabel('Avg Quantity')
plt.title('Relation of Average Quantity and Residuals')
plt.show()
## No Clear Correlation

assembler_residuals_quantity = VectorAssembler(inputCols = ['Residuals'], outputCol = 'Residual_features')
output_residuals_quantity = assembler_residuals_quantity.transform(price_cost_predictions)

chisq_residuals_quantity = ChiSquareTest.test(output_residuals_quantity, 'Residual_features', 'AvgQuantity')
chisq_price_cost.show()
## pValues: 0.2449236107707391
## degreesOfFreedom: 68904
## statistics: 69160.00000002132
### Looking like a weak relationship based on the p-value: 0.245 > 0.1