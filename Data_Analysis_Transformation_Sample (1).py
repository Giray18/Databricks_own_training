# Databricks notebook source
from pyspark.sql import SparkSession, functions, types, SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import json
import pandas as pd
from pyspark.sql.functions import udf, col
import re
from datetime import datetime, timedelta

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

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs ls dbfs:/mnt/

# COMMAND ----------

#dbutils.fs.ls ("dbfs:/mnt/mymountpointname/")
# display(dbutils.fs.ls ("dbfs:/mnt/mymountpointname/"))

# display(dbutils.fs.ls ("dbfs:/mnt/mymountpointname/"))


# COMMAND ----------

df = spark.read.option("encoding", "UTF-8").format('csv').load(
  '/mnt/mymountpointname/player_info.csv',
  header=True,
  inferSchema=True
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("Players")

# COMMAND ----------

# from datetime import datetime, timedelta
# from pyspark.sql.functions import year
# from pyspark.sql.functions import *
# from pyspark.sql.functions import col
# TeamlyPlayers = df.select(year("birthday").alias("Born_Year")).groupBy("Born_Year").count().orderBy("Born_Year").sort(col("count").desc())

# display(TeamlyPlayers)

# COMMAND ----------

delta_table_path = "/playerscsv/players"
#df.write.format("delta").save(delta_table_path)
df.write.format("delta").mode("Overwrite").saveAsTable("players_only")  ## Simdi Hive Metastore a kaydoldu
#df.write.format("delta").option("path", "/mydata").saveAsTable("players_only_1")

# COMMAND ----------

df1 = spark.read.json("/mnt/mymountpointname/epl_2022_2023_07_02_2023.json",multiLine=True)

df1.printSchema()

# COMMAND ----------

df1.createOrReplaceTempView("EPL_RECORDS")

# COMMAND ----------

# %python
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import concat,col
# from functools import reduce
# from pyspark.sql import DataFrame
# # from pyspark.sql.DataFrame import *

# column_names = list(df1.columns)

# # starting_lineups
# # df5 = df1.select(explode("0.team1_startings"))

# player_startings = []
# for i in column_names:
#     df5 = df1.select(explode(f"{i}.team1_startings")).distinct()
#     df6 = df1.select(explode(f"{i}.team2_startings")).distinct()
#     player_startings.append(df5)
#     player_startings.append(df6)

# df_output = functools.reduce(DataFrame.union, player_startings)

# players_unique = df_output.distinct()


# display(players_unique)

# players_unique.select("col").alias("Born_Year")

# players_unique.write.format("delta").option("path", "/player_names").saveAsTable("Unique_Player_Names")

# COMMAND ----------

# %python
# from pyspark.sql.functions import dense_rank
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
# from pyspark.sql import SparkSession, functions, types, SQLContext
# from pyspark.sql.types import *
# import pyspark.sql.functions as F
# import json
# import pandas as pd
# from pyspark.sql.functions import udf, col
# import re
# from datetime import datetime, timedelta


# df7 = spark.read.table("Unique_Player_Names").withColumnRenamed("col","Player_name")

# df8 = spark.read.table("players_only")

# df10 = df7.join(df8,df7.Player_name == df8.player_name,"inner") #Sadece oynayan oyuncular

# df.createOrReplaceTempView("df11")

# df.createOrReplaceTempView("df10")

# df11 = spark.sql("SELECT team,count(*) FROM df11 GROUP BY team")

# df10 = spark.sql("SELECT * FROM df10")

# df10 = df10.join(df11,df10.team == df11.team,"left")

# display(df11)

# spark.sql("SELECT * FROM df11").show()

# COMMAND ----------

# %sql

# CREATE OR REPLACE TABLE TEAMS_WITH_STARTING_LINEUPS_2
# USING DELTA
# LOCATION '/data/sales/'
# AS SELECT team,count(*) AS sayim ,RANK() OVER (ORDER BY count(1) DESC) Sira FROM df11 GROUP BY team;

