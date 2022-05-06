# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Implement a routine to "promote" your model at **Staging** in the registry to **Production** based on a boolean flag that you set in the code.
# MAGIC - Using wallet addresses from your **Staging** and **Production** model test data, compare the recommendations of the two models.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

import random
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec
from delta.tables import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from mlflow.tracking.client import MlflowClient
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC use G03_db;

# COMMAND ----------

dfm=spark.sql("SELECT * FROM g03_db.STAGING_MODEL_DETAILS").toPandas()

# COMMAND ----------

dfm.model_name[0]

# COMMAND ----------

df_gold=spark.sql("SELECT * FROM g03_db.GOLD_TABLE")

# COMMAND ----------

df_gold.display()

# COMMAND ----------

 client = MlflowClient()


# COMMAND ----------

dfm["model_name"]

# COMMAND ----------

client.transition_model_version_stage(
  name=dfm.model_name[0],
  version=dfm.model_version[0],
  stage='Production',
)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
