# Created By: Lila W Sahar
# Created Date: 08/11/2022
# version = '1.0'

# Imports ----
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from scipy import stats

# Data Load ----
processed_data = read('Price_Elasticity.LinearRegression')

# Creating Columns ----
processed_data = processed_data.withColumn('Residuals', col('UnitPrice') - col('Prediction'))

# Hypothesis Test ----
ttest_udf = udf(lambda x, y: stats.ttest_rel(x, y), FloatType())

pvalues = ttest_udf(processed_data.col('Residuals'), processed_data.col('UnitPrice'))

# If statements
pvalues[0] < 0.05
pvalues[1] < 0.05

