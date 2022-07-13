# Created By: Lila W Sahar
# Created Date: 07/13/2022
# version = '1.0'

# ---------------------------------------------------------------------------
""" This module is made to take a Materialized View and use it for a clustering algorithm """ 
# ---------------------------------------------------------------------------

# Imports
from pyspark.sql.functions import col, sum, count, mean
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Data Load
training = read("Price_Elasticity.Data_Processed")

# Grouping
training = training.withColumn("OrderQuantity", col("OrderQuantity").cast("double")) \
    .groupBy("ProductID") \
    .agg(mean("UnitPrice").alias("AvgPrice"), sum("Revenue").alias("TotalRevenue"), count("*").alias("TransactionCount"))

# Transformers
assembler = VectorAssembler(inputCols = ["AvgPrice", "TotalRevenue", "TransactionCount"], outputCol = "unscaledFeatures")
scaler = StandardScaler(inputCol = "unscaledFeatures", outputCol = "features", withStd = True, withMean = False)

## outlier removal step before or after scaling
## afterwards: if the abs is less than a certain number than keep it

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

# export
output = prediction.drop("unscaledFeatures", "features").withColumnRenamed("prediction", "Cluster")
save(output)