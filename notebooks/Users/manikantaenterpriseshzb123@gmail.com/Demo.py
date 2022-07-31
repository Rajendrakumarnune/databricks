# Databricks notebook source
# MAGIC %md "This is my first command"

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %sh ls -all

# COMMAND ----------

# MAGIC %sh mkdir "logs/rajendra"

# COMMAND ----------

# MAGIC %sh ls logs

# COMMAND ----------

sum = 1+2
print(sum)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

containername = "adf"
storagename = "rajendradevstorage"
mountpoint = "/mnt/demo"
sas = "hBLG5I+JYO/PxAriGhpJG7wTy/TjDEMkE9k2IT3Rs2dfKEOK5AtLi5mqeGPiQDZS7EDediSCr2TE+AStFnY2pQ=="
try:
  
  dbutils.fs.mount( source = "wasbs://" + containername + "@" + storagename + ".blob.core.windows.net",
                mount_point = mountpoint,
                 extra_configs = {"fs.azure.account.key." + storagename + '.blob.core.windows.net': sas}
  )
except Exception as e:
  print("already mounted, please unmount using dbutils.fs.unmount")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/demo/AllEmployees.csv

# COMMAND ----------

# MAGIC %scala
# MAGIC val df1 =spark.read.option("header",true).csv("dbfs:/mnt/demo/AllEmployees.csv")
# MAGIC display(df1)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df2 = df1.select("name","age")
# MAGIC display(df2)

# COMMAND ----------

# MAGIC %scala
# MAGIC var df3 = df2.withColumnRenamed("name","Name")
# MAGIC display(df3.describe())
# MAGIC df3.createOrReplaceTempView("df3")

# COMMAND ----------

# MAGIC %scala
# MAGIC var df4 = df3.filter("age> 4")
# MAGIC display(df4)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df3

# COMMAND ----------

# MAGIC %scala
# MAGIC df2.write.option("header","true").format("csv").saveAsTable("databricks1.employees")
# MAGIC df2.write.option("header","true").format("csv").save("dbfs:/mnt/demo/databricks_|AllEmployees.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database databricks1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from databricks1.employees

# COMMAND ----------

