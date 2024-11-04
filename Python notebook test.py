# Databricks notebook source
# MAGIC %md
# MAGIC # Eerste Python notebook in Databricks voor CityTRAQ hackaton
# MAGIC
# MAGIC In deze notebook staat wat Python code om je op weg te helpen om aan de slag te gaan met Databricks en de RIVM/VMM datasets die we zullen gebruiken in de hackaton(s). 

# COMMAND ----------

import pandas as pd

hive_db = "hackaton"
hive_table = "hove_sodaq_agg_cleaned" # kleine dataset ~1maand
#hive_table = "sensor_data_rotterdam" # grote dataset ~3jaar

hive_full_table_link = f"{hive_db}.{hive_table}"


# COMMAND ----------

# MAGIC %md
# MAGIC # Inladen (referentie naar) tabel
# MAGIC

# COMMAND ----------

df = spark.table(hive_full_table_link)

# COMMAND ----------

# MAGIC %md
# MAGIC # Omzetten naar pandas

# COMMAND ----------

df_pd = df.toPandas()

# COMMAND ----------

df_pd.display()

# COMMAND ----------

df.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL queries zijn ook mogelijk

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in hackaton
