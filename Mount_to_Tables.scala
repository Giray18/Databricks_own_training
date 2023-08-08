// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ## Mount Tables

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import SparkSession, functions, types, SQLContext
// MAGIC from pyspark.sql.types import *
// MAGIC import pyspark.sql.functions as F
// MAGIC import json
// MAGIC import pandas as pd
// MAGIC from pyspark.sql.functions import udf, col
// MAGIC import re
// MAGIC from datetime import datetime, timedelta

// COMMAND ----------

// MAGIC %scala
// MAGIC val storageAccount = "training45"
// MAGIC val container = "premier"
// MAGIC //val sasKey = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-04T22:30:50Z&st=2023-08-04T14:30:50Z&spr=https&sig=bqYO8DWF3Q6qWOJojN83uohdFX%2FylEj%2FciYL4EUmHGE%3D"
// MAGIC val sasKey = "sp=rl&st=2023-08-06T12:17:32Z&se=2023-08-11T20:17:32Z&spr=https&sv=2022-11-02&sr=c&sig=EHz1FFk1BZVQgOh0yq3Yp2Pj6FRyWsJmsEAXXsv1FcM%3D"
// MAGIC val mountPoint = s"/mnt/mymountpointname"
// MAGIC  
// MAGIC  
// MAGIC try {
// MAGIC   dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
// MAGIC } catch {
// MAGIC   case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
// MAGIC }
// MAGIC  
// MAGIC  
// MAGIC val sourceString = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
// MAGIC val confKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"
// MAGIC  
// MAGIC  
// MAGIC  
// MAGIC try {
// MAGIC   dbutils.fs.mount(
// MAGIC     source = sourceString,
// MAGIC     mountPoint = mountPoint,
// MAGIC     extraConfigs = Map(confKey -> sasKey)
// MAGIC   )
// MAGIC }
// MAGIC catch {
// MAGIC   case e: Exception =>
// MAGIC     println(s"*** ERROR: Unable to mount $mountPoint. Run previous cells to unmount first")
// MAGIC }
