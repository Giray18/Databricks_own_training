# Databricks notebook source
# MAGIC %pip install pyspark
# MAGIC %pip install dlt
# MAGIC %pip install pandas
# MAGIC %pip install datetime
# MAGIC
# MAGIC from pyspark.sql import SparkSession, functions, types, SQLContext
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import explode
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.types import *
# MAGIC import pyspark.sql.functions as F
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.functions import udf, col
# MAGIC from datetime import datetime, timedelta
# MAGIC import pyspark.sql.functions as F
# MAGIC from datetime import datetime, timedelta
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import explode
# MAGIC from functools import reduce
# MAGIC from pyspark.sql.functions import sum,avg,max,count

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Start Notebook

# COMMAND ----------

print("Execution Started")
print(f"Notebook Context: {dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create max EH3 and execution started timestamps for the execution_logs table

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # max_EH3_timestamp = players_df.agg(F.max('EventEnqueuedUtcTime').alias('max_EH3_timestamp')).first().max_EH3_timestamp
# MAGIC exec_started_timestamp = datetime.now()
# MAGIC print(exec_started_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Mount Tables

# COMMAND ----------

# %run ./Mount_to_Tables_1

# COMMAND ----------

storageAccount = "training45"
container = "premier"
sasKey = "sp=rl&st=2023-08-06T12:17:32Z&se=2023-08-11T20:17:32Z&spr=https&sv=2022-11-02&sr=c&sig=EHz1FFk1BZVQgOh0yq3Yp2Pj6FRyWsJmsEAXXsv1FcM%3D"
mountPoint = "/mnt/mymountpointname"
 
 
# try {
#   dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
# } catch {
#   case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
# }
 
 
sourceString = f"wasbs://{container}@{storageAccount}.blob.core.windows.net/"
confKey = f"fs.azure.sas.{container}.{storageAccount}.blob.core.windows.net"
 
#  dbutils.fs.mount(
#   source: str,
#   mount_point: str,
#   encryption_type: Optional[str] = "",
#   extra_configs: Optional[dict[str:str]] = None
# )

dbutils.fs.unmount("/mnt/mymountpointname")

configs = {confKey: sasKey}
 
dbutils.fs.mount(
    source = sourceString,
    mount_point = mountPoint,
    extra_configs = configs
  )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Imports

# COMMAND ----------

import re
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from functools import reduce
from pyspark.sql.functions import sum,avg,max,count
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Create DF from Hive Metastore

# COMMAND ----------

players_df = spark.read.table('players_only')
 
if players_df.first() is None:
    dbutils.notebook.exit("data yooook!!")

# COMMAND ----------

unique_player_names = spark.read.table('hive_metastore.default.unique_player_names')
 
if unique_player_names.first() is None:
    dbutils.notebook.exit("data yooook!!")

# COMMAND ----------

df1 = spark.read.json("/mnt/mymountpointname/epl_2022_2023_07_02_2023.json",multiLine=True)

df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Team Startings List By Player
# MAGIC

# COMMAND ----------

import functools
from pyspark.sql.functions import explode
from pyspark.sql import DataFrame

column_names = list(df1.columns)

team_1_startings = [df1.select(explode(f"{i}.team1_startings")) for i in column_names]
team_2_startings = [df1.select(explode(f"{i}.team2_startings")) for i in column_names]

total_team_startings = team_1_startings + team_2_startings

df_output = functools.reduce(DataFrame.union, total_team_startings)


display(df_output)


# COMMAND ----------

df_output.write.format("delta").mode("Overwrite").saveAsTable("ALL_STARTING_PLAYERS") 

# CREATE OR REPLACE TABLE ALL_STARTING_PLAYERS
# USING DELTA
# AS SELECT * FROM df_output;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Players with starting lineups counts

# COMMAND ----------

all_starting_players_agg = spark.read.table('all_starting_players')

all_starting_players_agg_1 = all_starting_players_agg.select("col").groupBy("col").agg(count("*").alias("played_games")) \
    .orderBy("played_games",ascending = False)

all_starting_players_agg_1.write.mode("overwrite").format("delta").saveAsTable("all_starting_players_agg_2")  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Join 2 Tables 

# COMMAND ----------

players_only = spark.read.table('players_only')

all_starting_players_agg_2 = spark.read.table('all_starting_players_agg_2')

players_only_join = players_only.join(all_starting_players_agg_2,all_starting_players_agg_2.col == players_only.player_name,"left")

players_only_join_removed_nulls = players_only_join.filter((players_only_join.col.isNotNull()))

players_only_join_removed_nulls = players_only_join_removed_nulls.drop("col")

players_only_join_removed_nulls.createOrReplaceTempView("players_only_join_removed_nulls")

# # players_only_join = spark.createDataFrame(players_only_join, schema="_c0 LONG, player_name STRING,team STRING,birthday DATE,position STRING,col STRING, played_games LONG")

# # players_only_join.write.format("delta").saveAsTable("players_only_with_games_played")  

display(players_only_join_removed_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Get TOP 10 most played players

# COMMAND ----------

TOP10_MOST_PLAYED = spark.sql("SELECT player_name,played_games,row_number() OVER (ORDER BY played_games DESC) Play_Rank FROM players_only_join_removed_nulls  GROUP BY player_name, played_games LIMIT 10")

TOP10_MOST_PLAYED.write.format("delta").mode("Overwrite").saveAsTable("TOP10_MOST_PLAYED") 


# CREATE OR REPLACE TABLE TOP10_MOST_PLAYED
# USING DELTA
# AS SELECT player_name,played_games,row_number() OVER (ORDER BY played_games DESC) Play_Rank FROM players_only_join_removed_nulls  GROUP BY player_name, played_games LIMIT 10;

