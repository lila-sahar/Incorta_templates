# Created By: Lila W Sahar
# Created Date: 07/26/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" This module is made to take a Materialized View and use it for a clustering algorithm """ 
# ---------------------------------------------------------------------------

# Imports
import pandas as pd

# from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, count, mean
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Data Load
training = read('Price_Elasticity.Data_Processed')

# Grouping
training = training.groupBy('ProductID', 'ProductName') \
    .agg(sum('Revenue').alias('TotalRevenue'), sum('Cost').alias('TotalCost'))

# Transformers
## Features
assembler = VectorAssembler(inputCols = ['TotalRevenue', 'TotalCost'], outputCol = 'unscaledFeatures')
scaler = StandardScaler(inputCol = 'unscaledFeatures', outputCol = 'features', withStd = True, withMean = False)

## outlier removal step before or after scaling

# model
kmeans = KMeans().setSeed(1)

# pipeline
pipeline = Pipeline(stages = [assembler, scaler, kmeans])

# tuning
paramGrid = ParamGridBuilder() \
    .addGrid(kmeans.k, list(range(3, 10))) \
    .build()

crossval = CrossValidator(
    estimator = pipeline,
    estimatorParamMaps = paramGrid,
    evaluator = ClusteringEvaluator(),
    numFolds = 3
)

# model fitting
cvModel = crossval.fit(training)

# predicting
prediction = cvModel.transform(training)

# cleaning output
output = prediction.drop('unscaledFeatures', 'features')

# # creation of dummy variable
# Clusters = output.select('prediction').distinct().rdd.flatMap(lambda x:x).collect()
# exprs = [F.when(F.col('prediction') == Clusters, 1).otherwise(0).alias(Cluster) for Cluster in Clusters]
# output.select('ProductID', 'ProductName', *exprs)


# show_dummy = prediction[['ProductID', 'ProductName', 'prediction']]
# vals_to_replace_cluster = {1: '0', 2: '1', 3: '2'}
# show_dummy['prediction'] = show_dummy['prediction'].map(vals_to_replace_cluster)

dummy_cluster = pd.get_dummies(output, columns = ['prediction'], prefix = 'Cluster')

output = dummy_cluster.withColumnRenamed('Cluster_2', 'Cluster_3') \
                      .withColumnRenamed('Cluster_1', 'Cluster_2') \
                      .withColumnRenamed('Cluster_0', 'Cluster_1')

# column_name = show_dummy.columns.values.tolist()
# column_name.remove('prediction')
# show_dummy = show_dummy[column_name].join(dummy_cluster)

save(output)

# this is subject to change depending on the MV