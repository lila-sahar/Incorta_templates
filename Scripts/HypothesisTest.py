# Created By: Lila W Sahar
# Created Date: 08/16/2022
# version = '1.0'

# Function ----

def lr_func(var1, var2, var3, var4, indep):

    # Transformation
    assembler = VectorAssembler(inputCols = [var1, var2, var3, var4, indep], outputCol = 'Features')
    output = assembler.transform(data)
    finalized_data = output.select('Features', indep)

    # Train-Test
    train, test = finalized_data.randomSplit([0.75, 0.25])

    # Model
    lr = LinearRegression()\
        .setParams(featuresCol = 'Features', labelCol = indep)

    # Model Fitting
    lr = lr.fit(train)

    # Export
    pred_results = lr.evaluate(test)

    result = pred_results.predictions
    result = result.drop('Features').withColumnRenamed('prediction', 'Prediction')


# Imports ----
from pyspark.sql.functions import *
from scipy import stats
from pyspark.sql.types import FloatType, ArrayType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# Data Load ----
processed_data = read('Price_Elasticity.LinearRegression')

# Creating Columns ----
processed_data = processed_data.withColumn('Residuals', col('UnitPrice') - col('Pred_UnitPrice'))

# Hypothesis Test ----
ttest_udf = udf(lambda x, y: stats.ttest_rel(x, y), FloatType())

processed_data = processed_data.withColumn('test_stat', ttest_udf(col('Residuals'), col('UnitPrice')))

## If statements - Naive Model or 2SLS
### this means that the null hypthesis fails to be rejected
if p > 0.05:
    # Data Load
    cleaned_data = read('Price_Elasticity.Data_Processed') 
    clustered_data = read('Price_Elasticity.ClusterProduct')

    # Renaming Columns
    clustered_data = clustered_data.withColumnRenamed('ProductID', 'ProductID_1') \
        .withColumnRenamed('ProductName', 'ProductName_1')

    # Join
    data = cleaned_data.join(clustered_data, (cleaned_data.ProductID == clustered_data.ProductID_1) & (cleaned_data.ProductName == clustered_data.ProductName_1)) \
        .drop('ProductID_1', 'ProductName_1')
    
    # Transformation, Model, Model Fitting, Export
    naive_result = lr_func('UnitPrice', 'Cluster_1', 'Cluster_2', 'Cluster_3', 'OrderQuantity')

### this means that the null hypthesis was rejected
else: 
    
    # Transformation, Model, Model Fitting, Export
    two_sls_result = lr_func('Pred_UnitPrice', 'Cluster_1', 'Cluster_2', 'Cluster_3', 'OrderQuantity')

save(result)


