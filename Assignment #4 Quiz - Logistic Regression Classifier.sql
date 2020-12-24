-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Logistic Regression Classifier
-- MAGIC ## Module 4 Assignment
-- MAGIC 
-- MAGIC This final assignment is broken up into 2 parts:
-- MAGIC 1. Completing this Logistic Regression Classifier notebook
-- MAGIC   * Submitting question answers to Coursera
-- MAGIC   * Uploading notebook to Coursera for peer reviewing
-- MAGIC 2. Answering 3 free response questions on Coursera platform
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this notebook you:
-- MAGIC * Preprocess data for use in a machine learning model
-- MAGIC * Step through creating a sklearn logistic regression model for classification
-- MAGIC * Predict the `Call_Type_Group` for incidents in a SQL table
-- MAGIC 
-- MAGIC 
-- MAGIC For each **bold** question, input its answer in Coursera.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load the `/mnt/davis/fire-calls/fire-calls-clean.parquet` data as `fireCallsClean` table.

-- COMMAND ----------

-- TODO
USE DATABRICKS;
-- FILL IN
DROP TABLE IF EXISTS fireCallsClean;
CREATE TABLE fireCallsClean
USING Parquet 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-clean.parquet"
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check that your data is loaded in properly.

-- COMMAND ----------

SELECT * FROM fireCallsClean LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By the end of this assignment, we would like to train a logistic regression model to predict 2 of the most common `Call_Type_Group` given information from the rest of the table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Write a query to see what the different `Call_Type_Group` values are and their respective counts.
-- MAGIC 
-- MAGIC ### Question 1
-- MAGIC 
-- MAGIC **How many calls of `Call_Type_Group` "Fire"?**

-- COMMAND ----------

-- TODO
select `Call_Type_Group`, count(`Call_Type_Group`) as numberofcalls
from fireCallsClean
Group by `Call_Type_Group`

-- COMMAND ----------

select count(`Call_Type_Group`)
from fireCallsClean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's drop all the rows where `Call_Type_Group = null`. Since we don't have a lot of `Call_Type_Group` with the value `Alarm` and `Fire`, we will also drop these calls from the table. Call this new temporary view `fireCallsGroupCleaned`.

-- COMMAND ----------

-- TODO
create or replace temporary view fireCallsGroupCleaned

As 
select * 
from fireCallsClean 
where Call_Type_Group is not null and Call_Type_Group not in  ('Alarm', 'Fire')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check that every entry in `fireCallsGroupCleaned`  has a `Call_Type_Group` of either `Potentially Life-Threatening` or `Non Life-threatening`.

-- COMMAND ----------

-- TODO
select Call_Type_Group, count(*) from fireCallsGroupCleaned
group by Call_Type_Group

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 2
-- MAGIC 
-- MAGIC **How many rows are in `fireCallsGroupCleaned`?**

-- COMMAND ----------

select count(*) from fireCallsGroupCleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We probably don't need all the columns of `fireCallsGroupCleaned` to make our prediction. Select the following columns from `fireCallsGroupCleaned` and create a view called `fireCallsDF` so we can access this table in Python:
-- MAGIC 
-- MAGIC * "Call_Type"
-- MAGIC * "Fire_Prevention_District"
-- MAGIC * "Neighborhooods_-\_Analysis_Boundaries" 
-- MAGIC * "Number_of_Alarms"
-- MAGIC * "Original_Priority" 
-- MAGIC * "Unit_Type" 
-- MAGIC * "Battalion"
-- MAGIC * "Call_Type_Group"

-- COMMAND ----------

-- TODO
create or replace temporary view fireCallsDF
As 
select Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Number_of_Alarms, Original_Priority, Unit_Type, Battalion,Call_Type_Group 
from fireCallsGroupCleaned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Fill in the string SQL statement to load the `fireCallsDF` table you just created into python.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "true")
-- MAGIC df = sql("""select Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Number_of_Alarms, Original_Priority, Unit_Type, Battalion,Call_Type_Group 
-- MAGIC from fireCallsGroupCleaned""")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Logistic Regression Model in Sklearn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First we will convert the Spark DataFrame to pandas so we can use sklearn to preprocess the data into numbers so that it is compatible with the logistic regression algorithm with a [LabelEncoder](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelEncoder.html). 
-- MAGIC 
-- MAGIC Then we'll perform a train test split on our pandas DataFrame. Remember that the column we are trying to predict is the `Call_Type_Group`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.model_selection import train_test_split
-- MAGIC from sklearn.preprocessing import LabelEncoder
-- MAGIC 
-- MAGIC pdDF = df.toPandas()
-- MAGIC le = LabelEncoder()
-- MAGIC numerical_pdDF = pdDF.apply(le.fit_transform)
-- MAGIC 
-- MAGIC X = numerical_pdDF.drop("Call_Type_Group", axis=1)
-- MAGIC y = numerical_pdDF["Call_Type_Group"].values
-- MAGIC X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Look at our training data `X_train` which should only have numerical values now.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(X_train)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We'll create a pipeline with 2 steps. 
-- MAGIC 
-- MAGIC 0. [One Hot Encoding](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder): Converts our  features into vectorized features by creating a dummy column for each value in that category. 
-- MAGIC 
-- MAGIC 0. [Logistic Regression model](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html): Although the name includes "regression", it is used for classification by predicting the probability that the `Call Type Group` is one label and not the other.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.linear_model import LogisticRegression
-- MAGIC from sklearn.preprocessing import OneHotEncoder
-- MAGIC from sklearn.pipeline import Pipeline
-- MAGIC 
-- MAGIC ohe = ("ohe", OneHotEncoder(handle_unknown="ignore"))
-- MAGIC lr = ("lr", LogisticRegression())
-- MAGIC 
-- MAGIC pipeline = Pipeline(steps = [ohe, lr]).fit(X_train, y_train)
-- MAGIC y_pred = pipeline.predict(X_test)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following cell to see how well our model performed on test data (data that wasn't used to train the model)!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from sklearn.metrics import accuracy_score
-- MAGIC print(f"Accuracy of model: {accuracy_score(y_pred, y_test)}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 3
-- MAGIC 
-- MAGIC **What is the accuracy of our model on test data? Round to the nearest percent.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Save pipeline (with both stages) to disk.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC from mlflow.sklearn import save_model
-- MAGIC 
-- MAGIC model_path = "/dbfs/" + username + "/Call_Type_Group_lr"
-- MAGIC dbutils.fs.rm(username + "/Call_Type_Group_lr", recurse=True)
-- MAGIC save_model(pipeline, model_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## UDF

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now that we have created and trained a machine learning pipeline, we will use MLflow to register the `.predict` function of the sklearn pipeline as a UDF which we can use later to apply in parallel. Now we can refer to this with the name `predictUDF` in SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC from mlflow.pyfunc import spark_udf
-- MAGIC 
-- MAGIC predict = spark_udf(spark, model_path, result_type="int")
-- MAGIC spark.udf.register("predictUDF", predict)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a view called `testTable` of our test data `X_test` so that we can see this table in SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark_df = spark.createDataFrame(X_test)
-- MAGIC spark_df.createOrReplaceTempView("testTable")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a table called `predictions` using the `predictUDF` function we registered beforehand. Apply the `predictUDF` to every row of `testTable` in parallel so that each row of `testTable` has a `Call_Type_Group` prediction.

-- COMMAND ----------

-- TODO

USE DATABRICKS;
drop table if exists predictions;

create table predictions as (
select *, cast(predictUDF(Call_Type, Fire_Prevention_District, `Neighborhooods_-_Analysis_Boundaries`, Number_of_Alarms, Original_Priority, Unit_Type, Battalion) as double) as prediction
  FROM testTable
  --LIMIT 10000
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now take a look at the table and see what your model predicted for each call entry!

-- COMMAND ----------

SELECT * FROM predictions LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 4: 
-- MAGIC 
-- MAGIC **What 2 values are in the `prediction` column?**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Congrats on finishing your last assignment notebook! 
-- MAGIC 
-- MAGIC 
-- MAGIC Now you will have to upload this notebook to Coursera for peer reviewing.
-- MAGIC 1. Make sure that all your code will run without errors
-- MAGIC   * Check this by clicking the "Clear State & Run All" dropdown option at the top of your notebook
-- MAGIC   * ![](http://files.training.databricks.com/images/eLearning/ucdavis/clearstaterunall.png)
-- MAGIC 2. Click on the "Workspace" icon on the side bar
-- MAGIC 3. Next to the notebook you're working in right now, click on the dropdown arrow
-- MAGIC 4. In the dropdown, click on "Export" then "HTML"
-- MAGIC   * ![](http://files.training.databricks.com/images/eLearning/ucdavis/export.png)
-- MAGIC 5. On the Coursera platform, upload this HTML file to Week 4's Peer Review Assignment
-- MAGIC 
-- MAGIC Go back onto the Coursera platform for the free response portion of this assignment and for instructions on how to review your peer's work.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
