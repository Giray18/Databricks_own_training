# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Mount Tables

# COMMAND ----------

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
