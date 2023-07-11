-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Engineering Data Pipelines
-- MAGIC ## Module 3 Assignment
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this assignment you:
-- MAGIC * Create a table with persistent data and a specified schema
-- MAGIC * Populate table with specific entries
-- MAGIC * Change partition number to compare query speeds
-- MAGIC 
-- MAGIC For each **bold** question, input its answer in Coursera.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC Create a table whose data will remain after you drop the table and after the cluster shuts down. Name this table `newTable` and specify the location to be at `/tmp/newTableLoc`
-- MAGIC 
-- MAGIC Set up the table to have the following schema:
-- MAGIC 
-- MAGIC ```
-- MAGIC `Address` STRING,
-- MAGIC `City` STRING,
-- MAGIC `Battalion` STRING,
-- MAGIC `Box` STRING,
-- MAGIC ```
-- MAGIC 
-- MAGIC Run the following cell first to remove any files stored at `/tmp/newTableLoc` before creating our table. Be sure to first re-run that cell each time you create `newTable`.
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This course was designed to work with Databricks Runtime 5.5 LTS ML, which uses Spark 2.4.  If you are running a later version of the Databricks Runtime, you might have to add an additional `STORED AS parquet` to your query [due to a bug.](https://issues.apache.org/jira/browse/SPARK-30436)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # removes files stored at '/tmp/newTableLoc'
-- MAGIC dbutils.fs.rm("/tmp/newTableLoc", True)   

-- COMMAND ----------

-- TODO 
DROP TABLE IF EXISTS newTable;
CREATE EXTERNAL TABLE newTable (
  `Address` STRING,
  `City` STRING,
  `Battalion` STRING,
  `Box` STRING
)
STORED AS parquet
LOCATION '/tmp/newTableLoc'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check that the data type of each column is what we want.
-- MAGIC 
-- MAGIC ### Question 1
-- MAGIC **What type of table is `newTable`? "EXTERNAL" or "MANAGED"?**
-- MAGIC ### Answer 1
-- MAGIC **EXTERNAL**
-- COMMAND ----------

DESCRIBE EXTENDED newTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following cell to read in the data stored at `/mnt/davis/fire-calls/fire-calls-truncated.json`. Check that the columns of the data are of the correct types (not all strings). 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  fireCallsJSON (
  `ALS Unit` boolean,
  `Address` string,
  `Available DtTm` string,
  `Battalion` string,
  `Box` string,
  `Call Date` string,
  `Call Final Disposition` string,
  `Call Number` long,
  `Call Type` string,
  `Call Type Group` string,
  `City` string,
  `Dispatch DtTm` string,
  `Entry DtTm` string,
  `Final Priority` long,
  `Fire Prevention District` string,
  `Hospital DtTm` string,
  `Incident Number` long,
  `Location` string,
  `Neighborhooods - Analysis Boundaries` string,
  `Number of Alarms` long,
  `On Scene DtTm` string,
  `Original Priority` string,
  `Priority` string,
  `Received DtTm` string,
  `Response DtTm` string,
  `RowID` string,
  `Station Area` string,
  `Supervisor District` string,
  `Transport DtTm` string,
  `Unit ID` string,
  `Unit Type` string,
  `Unit sequence in call dispatch` long,
  `Watch Date` string,
  `Zipcode of Incident` long
)
USING JSON 
OPTIONS (
    path "/mnt/davis/fire-calls/fire-calls-truncated.json"
);

DESCRIBE fireCallsJSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a look at the table to make sure it looks correct.

-- COMMAND ----------

SELECT * FROM fireCallsJSON LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's populate `newTable` with some of the rows from the `fireCallsJSON` table you just loaded. We only want to include fire calls whose `Final Priority` is `3`.

-- COMMAND ----------

-- TODO

INSERT INTO newTable
SELECT Address, City, Battalion, Box
FROM fireCallsJSON
WHERE `Final Priority`=3; 


-- COMMAND ----------

select * from newTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 2
-- MAGIC 
-- MAGIC **How many rows are in `newTable`? **
-- MAGIC ### Answer 2
-- MAGIC **191039**
  
-- COMMAND ----------

-- TODO
SELECT COUNT(*) FROM newTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Sort the rows of `newTable` by ascending `Battalion`.
-- MAGIC 
-- MAGIC ### Question 3
-- MAGIC 
-- MAGIC **What is the "Battalion" of the first entry in the sorted table?**
-- MAGIC ### Answer 3
-- MAGIC **B01**
  
-- COMMAND ----------

-- TODO
SELECT * FROM newTable
ORDER BY Battalion ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how this table is stored in our file system.
-- MAGIC 
-- MAGIC Note: You should have specified the location of the table to be `/tmp/newTableLoc` when you created it.

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/newTableLoc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC First run the following cell to check how many partitions are in this table. Did the number of partitions match the number of files our data was stored as?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's try increasing the number of partitions to 256. Create this as a new table and call it `newTablePartitioned`

-- COMMAND ----------

-- TODO
CREATE TABLE IF NOT EXISTS newTablePartitioned
AS
SELECT /*+ REPARTITION(256) */ *
FROM newTable


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's take a look at how this new table is stored.

-- COMMAND ----------

DESCRIBE EXTENDED newTablePartitioned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copy the location of the `newTablePartitioned` from the table above and take a look at the files stored at that location. Now how many parts is our data stored?

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/databricks.db/newtablepartitioned

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now sort the rows of `newTablePartitioned` by ascending `Battalion` and compare how long this query takes.
-- MAGIC 
-- MAGIC ### Question 4
-- MAGIC 
-- MAGIC **Was this query faster or slower on the table with increased partitions?**
-- MAGIC ### Answer 4
-- MAGIC **Slower**
  
-- COMMAND ----------

SELECT * FROM newTablePartitioned ORDER BY `Battalion`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following cell to see where the data of the original `newTable` is stored.

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/newTableLoc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now drop the table `newTable`.

-- COMMAND ----------

DROP TABLE newTable;

-- The following line should error!
-- SELECT * FROM newTable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 5
-- MAGIC 
-- MAGIC **Does the data stored within the table still exist at the original location (`dbfs:/tmp/newTableLoc`) after you dropped the table? (Answer "yes" or "no")**
-- MAGIC ### Answer 5
-- MAGIC **Yes**
-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/newTableLoc

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
