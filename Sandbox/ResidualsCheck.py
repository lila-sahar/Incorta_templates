# Created By: Lila W Sahar
# Created Date: 08/04/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" Motivation: Does the price optimization algorithm need 2SLS Regession? """ 
# ---------------------------------------------------------------------------

## Set Environment Variables
import findspark
findspark.init()

# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.regression import LinearRegression

# LOCAL TEST
spark = SparkSession.builder \
                    .master('local') \
                    .appName('ResidualsCheck') \
                    .getOrCreate()

# Data Load
path = '../Data/Processed/Data_Processed.csv'
processed_data = spark.read.option('header', True).csv(path)

# ---------------------------------------------------------------------------
""" Purpose: Understand the correlation between `Cost`, `Price`, and `Quantity` """ 
# ---------------------------------------------------------------------------

# Grouping
processed_data = processed_data.groupBy('ProductID', 'ProductName') \
    .agg(mean('UnitPrice').alias('AvgPrice'), mean('StandardCost').alias('AvgCost'), mean('OrderQuantity').alias('AvgQuantity'))

# Correlation
cor_price_cost = processed_data.stat.corr('AvgPrice', 'AvgCost')
## Value: 0.9875077376814002
## Hence, the relationship between price and cost is positive and relatively strong.

cor_price_quantity = processed_data.stat.corr('AvgPrice', 'AvgQuantity')
## Value: -0.20375785783624234
## Hence, the relationship between price and quantity is negative and relatively weak.

cor_cost_quantity = processed_data.stat.corr('AvgCost', 'AvgQuantity')
## Value: -0.18536013808736299
## Hence, the relationship between cost and quantity is negative and relatively weak.

# Data Exploration - Graphs
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

# Hypothesis Testing - Chi Squared Test
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

# Linear Regressions
## assembler_price_cost = VectorAssembler(inputCols = ['AvgPrice'], outputCol = 'Features')
## output_price_cost = assembler_price_cost.transform(processed_data)
price_cost_lr = LinearRegression(featuresCol = 'Features', labelCol = 'AvgCost')


