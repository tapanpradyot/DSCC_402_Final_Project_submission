# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Using the silver delta table(s) that were setup by your ETL module train and validate your token recommendation engine. Split, Fit, Score, Save
# MAGIC - Log all experiments using mlflow
# MAGIC - capture model parameters, signature, training/test metrics and artifacts
# MAGIC - Tune hyperparameters using an appropriate scaling mechanism for spark.  [Hyperopt/Spark Trials ](https://docs.databricks.com/_static/notebooks/hyperopt-spark-ml.html)
# MAGIC - Register your best model from the training run at **Staging**.

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
# MAGIC ## Your Code starts here...

# COMMAND ----------

# MAGIC %sql
# MAGIC USE G03_db;

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

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

df=spark.sql("SELECT WalletAddress,TokenAddress,first(image) as image,first(links) as links,first(symbol) as symbol,first(name) as name,first(((coingecko_score+developer_score+community_score)/3)) AS rating from g03_db.silverbal group by 1,2 LIMIT 200000").toPandas()
display(df)


# COMMAND ----------

df.describe()

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
df_new=df[['WalletAddress','TokenAddress','rating']]
le_from_address_encoder=LabelEncoder()
le_token_address_encoder=LabelEncoder()

df_new['WalletAddress_index']=le_from_address_encoder.fit_transform(df_new['WalletAddress'])
df_new['TokenAddress_index']=le_token_address_encoder.fit_transform(df_new['TokenAddress'])

df_spark=spark.createDataFrame(df_new)

# COMMAND ----------

(training,test)=df_spark.randomSplit([0.8, 0.2])
als=ALS(maxIter=5,regParam=0.09,rank=25,userCol="WalletAddress_index",itemCol="TokenAddress_index",ratingCol="rating",coldStartStrategy="drop",nonnegative=True)
model=als.fit(training)

# COMMAND ----------

evaluator=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
predictions=model.transform(test)
rmse=evaluator.evaluate(predictions)
print("RMSE="+str(rmse))
predictions.show()

# COMMAND ----------

user_recs=model.recommendForAllUsers(20).show(10)

# COMMAND ----------

import uuid
model_name=f'ETH_AS_{uuid.uuid4().hex[:10]}'
model_name

# COMMAND ----------

MY_EXPERIMENT = "/Users/" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get() + "/" + model_name
mlflow.set_experiment(MY_EXPERIMENT)
experiment = mlflow.get_experiment_by_name(MY_EXPERIMENT)



# COMMAND ----------

experiment


# COMMAND ----------

def train_ALS(regParam_opt, rank_opt):
    
    input_schema = Schema(
            [
                ColSpec('integer', 'WalletAddress_index'),
                ColSpec('integer', 'TokenAddress_index')
            ]
        )
    output_schema = Schema([ColSpec('double')])
    signature = ModelSignature(inputs=input_schema, 
                                   outputs=output_schema)
    
    with mlflow.start_run(nested=True) as run:
        als=ALS(maxIter=5,regParam=regParam_opt,rank=rank_opt,userCol="WalletAddress_index",itemCol="TokenAddress_index",
                ratingCol="rating",coldStartStrategy="drop",nonnegative=True)
        model=als.fit(training)

        # Define an evaluation metric and evaluate the model on the validation dataset.
        evaluator=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
        predictions_train=model.transform(training)
        rmse_train=evaluator.evaluate(predictions_train)
        
        predictions=model.transform(test)
        rmse=evaluator.evaluate(predictions)
        print("RMSE="+str(rmse))
        
        mlflow.set_tags({'group': '	G03', 'class': 'DSCC-402'})
        mlflow.log_params({'regParam':regParam_opt, 'rank': rank_opt})
        mlflow.spark.log_model(spark_model=model, signature=signature,
                               artifact_path='als-model', registered_model_name=model_name+str(rank_opt))
        
        mlflow.log_metric("RMSE_test", rmse)
        mlflow.log_metric("RMSE_train", rmse_train)
        runID=run.info.run_uuid
    
    return model, rmse, runID


# COMMAND ----------

#testing train_ALS function
initial_model, val_metric, runID = train_ALS(regParam_opt=0.09,rank_opt=10)
print(f"The ALS achieved RMSE of {val_metric} on the validation data, ran with ID {runID}")


# COMMAND ----------

#Using HyperOPT
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
 
def train_with_hyperopt(params):
  RegParam= int(params['regParam'])
  rank= int(params['rank'])
  model, rmse,RunId = train_ALS(RegParam, rank)
  loss = rmse
  return {'loss': loss, 'status': STATUS_OK}

# COMMAND ----------

space = {'regParam': hp.uniform('regParam', 0.1, 0.3),
        'rank': hp.uniform('rank', 4, 16)}

# COMMAND ----------

algo=tpe.suggest
 
with mlflow.start_run():
  best_params = fmin(
    fn=train_with_hyperopt,
    space=space,
    algo=algo,
    max_evals=8
  )

# COMMAND ----------

best_regParam = int(best_params['regParam'])
best_rank = int(best_params['rank'])
 
final_model, val_rmse,RunID = train_ALS(best_regParam,best_rank)

# COMMAND ----------



# COMMAND ----------

import mlflow.pyfunc

model_uri = "runs:/{run_id}/als-model".format(run_id= RunID)
model_details = mlflow.register_model(model_uri=model_uri, name=model_name)


#nn_pyfunc_model = mlflow.pyfunc.mlflow.pyfunc.load_model(model_uri=model_uri)
#type(nn_pyfunc_model)

#model_uri = "runs:/{run_id}/als-model".format(run_id= RunID)
# model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

 client = MlflowClient()
 model_version_details = client.get_model_version(name=model_name, version=1)
 model_version_details.status

# COMMAND ----------

 client.transition_model_version_stage(
   name=model_details.name,
   version=model_details.version,
   stage='Staging',
 )

# COMMAND ----------

# client.transition_model_version_stage(
#   name=model_details.name,
#   version=model_details.version,
#   stage='Production',
# )

# COMMAND ----------

model_details.name,model_details.version

# COMMAND ----------

data_model = [{"model_name": model_details.name,"model_version": model_details.version}
       ]                     
df_model = spark.createDataFrame(data_model)
type(df_model)
                     
                     

# COMMAND ----------

df_model.display()

# COMMAND ----------

df_model.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('G03_db.STAGING_MODEL_DETAILS')

# COMMAND ----------

evaluator=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
predictions=final_model.transform(test)
rmse=evaluator.evaluate(predictions)
print("RMSE="+str(rmse))
predictions.show()

# COMMAND ----------

address=[999900099]
df_test=pd.DataFrame(address,columns=['WalletAddress_index'])
df_spark1=spark.createDataFrame(df_test)
df_spark1.head()

# COMMAND ----------


users = df_spark1.select(final_model.getUserCol()).distinct().limit(1)
userSubsetRecs = model.recommendForUserSubset(users, 10)
display(userSubsetRecs)

# COMMAND ----------


display(users)

# COMMAND ----------

predictions.display()

# COMMAND ----------

df=spark.createDataFrame(df)

# COMMAND ----------

dfp = predictions.join(df, (df.WalletAddress == predictions.WalletAddress), 'inner')

# COMMAND ----------

dfp = dfp.toDF(*map(str, range(len(dfp.columns)))).drop("0","1","2").toDF(*dfp.columns[3:])

# COMMAND ----------

dfp.display()

# COMMAND ----------

dfp.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('G03_db.GOLD_TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA G03_db.GOLD_TABLE

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


