#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------
# Created By  : Lila W Sahar
# Created Date: 06/29/2022
# version ='1.0'
# ---------------------------------------------------------------------------
""" This module is made to take a .csv and use it for a clustering algorithm """ 
# ---------------------------------------------------------------------------

# Imports
import findspark
findspark.init()

from numpy import mean
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, mean
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# LOCAL TEST
spark = SparkSession.builder \
                    .master("local") \
                    .appName('ClusterZip') \
                    .getOrCreate()

path = "../Data/Processed/product_total_tbl.csv"
training = spark.read.option("header", True).csv(path)

# grouping
training = training.groupBy("description") \
    .agg(mean("price").alias("avg_price"), sum("revenue").alias("total_revenue"), count("*").alias("transaction_count"))

# transformers
assembler = VectorAssembler(inputCols = ["avg_price", "total_revenue", "transaction_count"], outputCol = "unscaledFeatures")
scaler = StandardScaler(inputCol = "unscaledFeatures", outputCol = "features", withStd = True, withMean = False)

# outlier removal step before or after scaling
# afterwards: if the abs is less than a certain number than keep it

# model
kmeans = KMeans().setSeed(43)

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
# save(output)

# LOCAL TEST
output.show(10)
spark.stop()