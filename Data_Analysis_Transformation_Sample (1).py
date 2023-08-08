# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql import SparkSession, functions, types, SQLContext
# MAGIC from pyspark.sql.types import *
# MAGIC import pyspark.sql.functions as F
# MAGIC import json
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.functions import udf, col
# MAGIC import re
# MAGIC from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %scala
# MAGIC val storageAccount = "training45"
# MAGIC val container = "premier"
# MAGIC //val sasKey = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-04T22:30:50Z&st=2023-08-04T14:30:50Z&spr=https&sig=bqYO8DWF3Q6qWOJojN83uohdFX%2FylEj%2FciYL4EUmHGE%3D"
# MAGIC val sasKey = "sp=rl&st=2023-08-06T12:17:32Z&se=2023-08-11T20:17:32Z&spr=https&sv=2022-11-02&sr=c&sig=EHz1FFk1BZVQgOh0yq3Yp2Pj6FRyWsJmsEAXXsv1FcM%3D"
# MAGIC val mountPoint = s"/mnt/mymountpointname"
# MAGIC  
# MAGIC  
# MAGIC try {
# MAGIC   dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
# MAGIC } catch {
# MAGIC   case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
# MAGIC }
# MAGIC  
# MAGIC  
# MAGIC val sourceString = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
# MAGIC val confKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC try {
# MAGIC   dbutils.fs.mount(
# MAGIC     source = sourceString,
# MAGIC     mountPoint = mountPoint,
# MAGIC     extraConfigs = Map(confKey -> sasKey)
# MAGIC   )
# MAGIC }
# MAGIC catch {
# MAGIC   case e: Exception =>
# MAGIC     println(s"*** ERROR: Unable to mount $mountPoint. Run previous cells to unmount first")
# MAGIC }
# MAGIC  
# MAGIC
# MAGIC //fs ls /mnt/mymountpointname/
# MAGIC  
# MAGIC /*This will list all the files located at the container*/

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

# MAGIC %python
# MAGIC display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC #dbutils.fs.ls ("dbfs:/mnt/mymountpointname/")
# MAGIC # display(dbutils.fs.ls ("dbfs:/mnt/mymountpointname/"))
# MAGIC
# MAGIC display(dbutils.fs.ls ("dbfs:/mnt/mymountpointname/"))
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.option("encoding", "UTF-8").format('csv').load(
# MAGIC   '/mnt/mymountpointname/player_info.csv',
# MAGIC   header=True,
# MAGIC   inferSchema=True
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC df.createOrReplaceTempView("Players")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Players;

# COMMAND ----------

# MAGIC %python
# MAGIC from datetime import datetime, timedelta
# MAGIC from pyspark.sql.functions import year
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.functions import col
# MAGIC TeamlyPlayers = df.select(year("birthday").alias("Born_Year")).groupBy("Born_Year").count().orderBy("Born_Year").sort(col("count").desc())
# MAGIC
# MAGIC display(TeamlyPlayers)

# COMMAND ----------

# MAGIC %python
# MAGIC delta_table_path = "/playerscsv/players"
# MAGIC #df.write.format("delta").save(delta_table_path)
# MAGIC df.write.format("delta").mode("Overwrite").saveAsTable("players_only")  ## Simdi Hive Metastore a kaydoldu
# MAGIC #df.write.format("delta").option("path", "/mydata").saveAsTable("players_only_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE EXTENDED players
# MAGIC DESCRIBE HISTORY players_only   -- sadece tablolarda calisiyor HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED players

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC df1 = spark.read.json("/mnt/mymountpointname/epl_2022_2023_07_02_2023.json",multiLine=True)
# MAGIC
# MAGIC df1.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC df1.createOrReplaceTempView("EPL_RECORDS")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM EPL_RECORDS

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import explode
# MAGIC from pyspark.sql.functions import concat,col
# MAGIC from functools import reduce
# MAGIC from pyspark.sql import DataFrame
# MAGIC # from pyspark.sql.DataFrame import *
# MAGIC
# MAGIC column_names = list(df1.columns)
# MAGIC
# MAGIC # starting_lineups
# MAGIC # df5 = df1.select(explode("0.team1_startings"))
# MAGIC
# MAGIC player_startings = []
# MAGIC for i in column_names:
# MAGIC     df5 = df1.select(explode(f"{i}.team1_startings")).distinct()
# MAGIC     df6 = df1.select(explode(f"{i}.team2_startings")).distinct()
# MAGIC     player_startings.append(df5)
# MAGIC     player_startings.append(df6)
# MAGIC
# MAGIC df_output = functools.reduce(DataFrame.union, player_startings)
# MAGIC
# MAGIC players_unique = df_output.distinct()
# MAGIC
# MAGIC
# MAGIC display(players_unique)
# MAGIC
# MAGIC players_unique.select("col").alias("Born_Year")
# MAGIC
# MAGIC players_unique.write.format("delta").option("path", "/player_names").saveAsTable("Unique_Player_Names")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import dense_rank
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import row_number
# MAGIC from pyspark.sql import SparkSession, functions, types, SQLContext
# MAGIC from pyspark.sql.types import *
# MAGIC import pyspark.sql.functions as F
# MAGIC import json
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.functions import udf, col
# MAGIC import re
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC
# MAGIC df7 = spark.read.table("Unique_Player_Names").withColumnRenamed("col","Player_name")
# MAGIC
# MAGIC df8 = spark.read.table("players_only")
# MAGIC
# MAGIC df10 = df7.join(df8,df7.Player_name == df8.player_name,"inner") #Sadece oynayan oyuncular
# MAGIC
# MAGIC df.createOrReplaceTempView("df11")
# MAGIC
# MAGIC df.createOrReplaceTempView("df10")
# MAGIC
# MAGIC df11 = spark.sql("SELECT team,count(*) FROM df11 GROUP BY team")
# MAGIC
# MAGIC df10 = spark.sql("SELECT * FROM df10")
# MAGIC
# MAGIC df10 = df10.join(df11,df10.team == df11.team,"left")
# MAGIC
# MAGIC display(df11)
# MAGIC
# MAGIC spark.sql("SELECT * FROM df11").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE TEAMS_WITH_STARTING_LINEUPS_2
# MAGIC USING DELTA
# MAGIC LOCATION '/data/sales/'
# MAGIC AS SELECT team,count(*) AS sayim ,RANK() OVER (ORDER BY count(1) DESC) Sira FROM df11 GROUP BY team;
# MAGIC

# COMMAND ----------


