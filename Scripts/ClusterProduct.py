# Created By: Lila W Sahar
# Created Date: 07/26/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" This module is made to take a Materialized View and use it for a clustering algorithm """ 
# ---------------------------------------------------------------------------

# Imports
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, count, mean, first
from pyspark.ml.functions import vector_to_array
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler, OneHotEncoder
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Data Load
training = read('Price_Elasticity.Data_Processed')

# Grouping
training = training.groupBy('ProductID', 'ProductName') \
    .agg(sum('Revenue').alias('TotalRevenue'), sum('Cost').alias('TotalCost'))

# Transformers
assembler = VectorAssembler(inputCols = ['TotalRevenue', 'TotalCost'], outputCol = 'unscaledFeatures')
scaler = StandardScaler(inputCol = 'unscaledFeatures', outputCol = 'features', withStd = True, withMean = False)

## outlier removal step before or after scaling

# Model
kmeans = KMeans().setSeed(1)

# Pipeline
pipeline = Pipeline(stages = [assembler, scaler, kmeans])

# Tuning
paramGrid = ParamGridBuilder() \
    .addGrid(kmeans.k, list(range(3, 10))) \
    .build()

crossval = CrossValidator(
    estimator = pipeline,
    estimatorParamMaps = paramGrid,
    evaluator = ClusteringEvaluator(),
    numFolds = 3
)

# Model Fitting
cvModel = crossval.fit(training)

# Predicting
prediction = cvModel.transform(training)

# Cleaning Output
output = prediction.drop('unscaledFeatures', 'features')

# Feature: Dummy Variable
encoder = OneHotEncoder(
    inputCols = ['prediction'],
    outputCols = ['cluster_vec'],
    dropLast = False)
output_encoded = encoder.fit(output).transform(output)

df_col_onehot = output_encoded.select('*', vector_to_array('cluster_vec').alias('col_onehot'))
num_categories = len(df_col_onehot.first()['col_onehot'])
cols_expanded = [(F.col('col_onehot')[i]) for i in range(num_categories)]
df_cols_onehot = df_col_onehot.select('ProductID','ProductName', *cols_expanded)

result = df_cols_onehot.withColumnRenamed('col_onehot[0]', 'Cluster_1') \
    .withColumnRenamed('col_onehot[1]', 'Cluster_2') \
    .withColumnRenamed('col_onehot[2]', 'Cluster_3')

save(result)