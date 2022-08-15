# Created By: Lila W Sahar
# Created Date: 08/10/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" This script is made to take a Materialized View and use it for a linear regression """ 
# ---------------------------------------------------------------------------

# Imports
from pyspark.sql.functions import mean
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# Data Load
processed_data = read('Price_Elasticity.Data_Processed') 
clustered_data = read('Price_Elasticity.ClusterProduct')

# # Grouping
# processed_data = processed_data.groupBy('ProductID', 'ProductName') \
#     .agg(mean('UnitPrice').alias('AvgPrice'), mean('StandardCost').alias('AvgCost'))

# Renaming Columns
clustered_data = clustered_data.withColumnRenamed('ProductID', 'ProductID_1') \
    .withColumnRenamed('ProductName', 'ProductName_1')

# Join
data = processed_data.join(clustered_data, (processed_data.ProductID == clustered_data.ProductID_1) & (processed_data.ProductName == clustered_data.ProductName_1)) \
    .drop('ProductID_1', 'ProductName_1')

# Transformation
assembler = VectorAssembler(inputCols = ['StandardCost', 'Cluster_1', 'Cluster_2', 'Cluster_3'], outputCol = 'Features')

output = assembler.transform(data)
finalized_data = output.select('Features', 'UnitPrice')

# Train-Test
train, test = finalized_data.randomSplit([0.75, 0.25])

# Model
lr = LinearRegression()\
    .setParams(featuresCol = 'Features', labelCol = 'UnitPrice')

# Model Fitting
lr = lr.fit(train)

# Export
pred_results = lr.evaluate(test)

result = pred_results.predictions
result = result.drop('Features').withColumnRenamed('prediction', 'Pred_UnitPrice')

save(result)