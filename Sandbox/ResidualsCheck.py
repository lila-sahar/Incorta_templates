# Created By: Lila W Sahar
# Created Date: 08/04/2022
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

cor_price_quantity = processed_data.stat.corr('AvgPrice', 'AvgQuantity')
## Value: -0.20375785783624234
## Hence, the relationship between price and quantity is negative and relatively weak.

cor_cost_quantity = processed_data.stat.corr('AvgCost', 'AvgQuantity')
## Value: -0.18536013808736299
## Hence, the relationship between cost and quantity is negative and relatively weak.

# Data Exploration - Graphs ---------------------------------------------------
processed_data_pd = processed_data.select('AvgPrice','AvgCost', 'AvgQuantity').toPandas()
plt.scatter(processed_data_pd.AvgPrice, processed_data_pd.AvgCost)
plt.xlabel('Avg Price')
plt.ylabel('Avg Cost')
plt.title('Relation of Average Cost and Average Price')
plt.show()
## Strong Positive Linear Correlation

plt.scatter(processed_data_pd.AvgPrice, processed_data_pd.AvgQuantity)
plt.xlabel('Avg Price')
plt.ylabel('Avg Quantity')
plt.title('Relation of Average Quantity and Average Price')
plt.show()
## Weak Negative Correlation

plt.scatter(processed_data_pd.AvgCost, processed_data_pd.AvgQuantity)
plt.xlabel('Avg Cost')
plt.ylabel('Avg Quantity')
plt.title('Relation of Average Quantity and Average Cost')
plt.show()
## Weak Negative Correlation

# Hypothesis Testing - Chi Squared Test ---------------------------------------------
assembler_price_cost = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
output_price_cost = assembler_price_cost.transform(processed_data)

chisq_price_cost = ChiSquareTest.test(output_price_cost, 'Features', 'AvgCost')
chisq_price_cost.show()
## pValues: 0.2449236107707391
## degreesOfFreedom: 68904
## statistics: 69160.00000002132
### Looking like a weak relationship based on the p-value: 0.245 > 0.1

assembler_price_quantity = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
output_price_quantity = assembler_price_quantity.transform(processed_data)

chisq_price_quantity = ChiSquareTest.test(output_price_quantity, 'Features', 'AvgQuantity')
chisq_price_quantity.show()
## pValues: 0.2594186837716159
## degreesOfFreedom: 66000
## statistics: 66234.0000000017
### Looking like a weak relationship based on the p-value: 0.259 > 0.1

assembler_cost_quantity = VectorAssembler(inputCols = ['AvgCost'], outputCol = 'Features')
output_cost_quantity = assembler_cost_quantity.transform(processed_data)

chisq_cost_quantity = ChiSquareTest.test(output_cost_quantity, 'Features', 'AvgQuantity')
chisq_cost_quantity.show()
## pValues: 0.30284389779486
## degreesOfFreedom: 65250
## statistics: 65435.99999999638
### Looking like a weak relationship based on the p-value: 0.303 > 0.1

# Linear Regressions ----------------------------------------------------------------
## assembler_price_cost = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
## output_price_cost = assembler_price_cost.transform(processed_data)

final_price_cost = output_price_cost.select('Features','AvgCost')
train_data, test_data = final_price_cost.randomSplit([0.7,0.3])
train_data.describe().show()

price_cost_lr = LinearRegression(featuresCol = 'Features', labelCol = 'AvgCost')
price_cost_model = price_cost_lr.fit(train_data)
price_cost_results = price_cost_model.evaluate(test_data)

print('Rsquared Error :', price_cost_results.r2)
## Rsquared Error: 0.9769668026112462
### 97% of the regression model covers most part of the variance of the values of the response variable
print('Mean Squared Error :', price_cost_results.meanSquaredError)
## Mean Squared Error: 5258.389368068349
print('Root Mean Square Deviation :', price_cost_results.rootMeanSquaredError)
## Root Mean Square Deviation: 72.51475276154741
print('Mean Absolute Error :', price_cost_results.meanAbsoluteError)
## Mean Absolute Error: 51.88376154944492
print('Explained Variance :', price_cost_results.explainedVariance)
## Explained Variance: 217089.20163281556

unlabeled_data = test_data.select('Features')
price_cost_predictions = price_cost_model.transform(unlabeled_data)
price_cost_predictions.show()

# ------------------------------------------------------------------------------------
## assembler_price_quantity = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
## output_price_quantity = assembler_price_quantity.transform(processed_data)

final_price_quantity = output_price_quantity.select('Features','AvgQuantity')
train_data, test_data = final_price_quantity.randomSplit([0.7,0.3])
train_data.describe().show()

price_quantity_lr = LinearRegression(featuresCol = 'Features', labelCol = 'AvgQuantity')
price_quantity_model = price_quantity_lr.fit(train_data)
price_quantity_results = price_quantity_model.evaluate(test_data)

print('Rsquared Error :', price_quantity_results.r2)
## Rsquared Error: -0.13201066696908437
### -13% of the regression model covers part of the variance of the values of the response variable
print('Mean Squared Error :', price_quantity_results.meanSquaredError)
## Mean Squared Error: 0.6257054078271302
print('Root Mean Square Deviation :', price_quantity_results.rootMeanSquaredError)
## Root Mean Square Deviation: 0.7910154283116925
print('Mean Absolute Error :', price_quantity_results.meanAbsoluteError)
## Mean Absolute Error: 0.60940986683515
print('Explained Variance :', price_quantity_results.explainedVariance)
## Explained Variance: 0.12429722851567176

unlabeled_data = test_data.select('Features')
price_quantity_predictions = price_quantity_model.transform(unlabeled_data)
price_quantity_predictions.show()

# ------------------------------------------------------------------------------------
## assembler_cost_quantity = VectorAssembler(inputCols = ['AvgCost'], outputCol = 'Features')
## output_cost_quantity = assembler_cost_quantity.transform(processed_data)

final_cost_quantity = output_cost_quantity.select('Features','AvgQuantity')
train_data, test_data = final_cost_quantity.randomSplit([0.7,0.3])
train_data.describe().show()

cost_quantity_lr = LinearRegression(featuresCol = 'Features', labelCol = 'AvgQuantity')
cost_quantity_model = cost_quantity_lr.fit(train_data)
cost_quantity_results = cost_quantity_model.evaluate(test_data)

print('Rsquared Error :', cost_quantity_results.r2)
## Rsquared Error: 0.016764786649342556
### 1.68% of the regression model covers part of the variance of the values of the response variable
print('Mean Squared Error :', cost_quantity_results.meanSquaredError)
## Mean Squared Error: 1.2602815456215177
print('Root Mean Square Deviation :', cost_quantity_results.rootMeanSquaredError)
## Root Mean Square Deviation: 1.122622619414698
print('Mean Absolute Error :', cost_quantity_results.meanAbsoluteError)
## Mean Absolute Error: 0.623786246107052
print('Explained Variance :', cost_quantity_results.explainedVariance)
## Explained Variance: 0.03727458555214374

unlabeled_data = test_data.select('Features')
cost_quantity_predictions = cost_quantity_model.transform(unlabeled_data)
cost_quantity_predictions.show()

# Linear Regressions - Visuals --------------------------------------------

## Transform Data
firstelement = udf(lambda v:float(v[0]), FloatType())
### output_price_cost = output_price_cost.select(col('AvgCost'), col('AvgPrice'), firstelement('Features')) \
###     .withColumnRenamed('<lambda>(Features)', 'Features_2')

price_cost_predictions = price_cost_predictions.select(firstelement('Features'), col('prediction')) \
    .withColumnRenamed('<lambda>(Features)', 'Features_1') \
###     .join(output_price_cost, price_cost_predictions.Features_1 == output_price_cost.Features_2, 'left') \
###     .select('AvgCost', 'Features_1', 'prediction') \
    .withColumn('Residuals', col('Features_1') - col('prediction'))
    
price_cost_stats = price_cost_predictions.agg(mean('Residuals').alias('AvgResiduals'))
## AvgResiduals: 96.34890843538489

price_quantity_predictions = price_quantity_predictions.select(firstelement('Features'), col('prediction')) \
    .withColumnRenamed('<lambda>(Features)', 'Features_1') \
###     .join(output_price_cost, price_cost_predictions.Features_1 == output_price_cost.Features_2, 'left') \
###     .select('AvgCost', 'Features_1', 'prediction') \
    .withColumn('Residuals', col('Features_1') - col('prediction'))

price_quantity_stats = price_quantity_predictions.agg(mean('Residuals').alias('AvgResiduals'))

cost_quantity_predictions = cost_quantity_predictions.select(firstelement('Features'), col('prediction')) \
    .withColumnRenamed('<lambda>(Features)', 'Features_1') \
###     .join(output_price_cost, price_cost_predictions.Features_1 == output_price_cost.Features_2, 'left') \
###     .select('AvgCost', 'Features_1', 'prediction') \
    .withColumn('Residuals', col('Features_1') - col('prediction'))

cost_quantity_stats = price_quantity_predictions.agg(mean('Residuals').alias('AvgResiduals'))
