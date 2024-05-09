# Databricks notebook source
# MAGIC %run ./Utility/Helpers/EnvironmentVariableHelper

# COMMAND ----------

# MAGIC %run ./Utility/Helpers/AdlsHelper

# COMMAND ----------

# MAGIC %run ./Utility/Load

# COMMAND ----------

schema_name = databricks_schema_suf
catalog_name = databricks_catalog_name
stage_catalog_name = databricks_stage_catalog_name
storage_account_name = env_storage_account_name
raw_catalog_name = databricks_raw_catalog_name


# COMMAND ----------

dbutils.widgets.text('FILE_NAME','')
FILE_NAME = dbutils.widgets.get('FILE_NAME') 
output_list = []

# COMMAND ----------

# DBTITLE 1,Write data in storage account
try:
    data = [('New York','01'), ('Paris','03'), ('London','03'), ('Mumbai','04'),('Pune','09')]
    table_df = spark.createDataFrame(data, ['City','Code'])
    table_df.write.mode('overwrite').format('delta').save(f"abfss://datamovement@{storage_account_name}.dfs.core.windows.net/results/{FILE_NAME}.csv")
    output_list.append("write data into storage account-Success")
except Exception as e:
    output_list.append("write data into storage account-Failed: " + str(e))

# COMMAND ----------

# DBTITLE 1,Read data from storage account
try:
    read_data=spark.read.format('delta').load(f"abfss://datamovement@{storage_account_name}.dfs.core.windows.net/results/{FILE_NAME}.csv")
    display(read_data)
    output_list.append("read data from storage account-Success")
except Exception as e:
   output_list.append("read data from storage account-Failed: " + str(e))

# COMMAND ----------

# DBTITLE 1,Write dataframe in main catalog
# Check if the table already exists
try:
    if not spark.catalog.tableExists(f"{catalog_name}.{schema_name}.{FILE_NAME}_sampleTable"):
        table_df.write.format("delta").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
        spark.sql(f"AlTER table {catalog_name}.{schema_name}.{FILE_NAME}_sampleTable owner to {write_role}")
        output_list.append("write dataframe in main catalog-Success")
    
except Exception as e:
    output_list.append("write dataframe in main catalog-Failed: " + str(e))


# COMMAND ----------

# DBTITLE 1,Write dataframe in staging catalog
# Check if the table already exists
try:
    if not spark.catalog.tableExists(f"{stage_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable"):
        table_df.write.format("delta").option("mergeSchema", "true").saveAsTable(f"{stage_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
        spark.sql(f"AlTER table {stage_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable owner to {write_role}")
        output_list.append("write dataframe in staging catalog-Success")
except Exception as e:
   output_list.append("write dataframe in staging catalog-Failed: " + str(e))


# COMMAND ----------

# DBTITLE 1,Write data in raw catalog
# Check if the table already exists
try:
    if not spark.catalog.tableExists(f"{raw_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable"):
        table_df.write.format("delta").option("mergeSchema", "true").saveAsTable(f"{raw_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
        spark.sql(f"AlTER table {raw_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable owner to {write_role}")
        output_list.append("write dataframe in raw catalog-Success")
except Exception as e:
    output_list.append("write dataframe in raw catalog-Failed: " + str(e))


# COMMAND ----------

# DBTITLE 1,Update main catalog table
data = [("United States", "USA"), ("Canada", "CAN"), ("Australia", "AUS")]
try:
    upd_df = spark.createDataFrame(data, ["City", "CountryCode"])
    upd_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
    output_list.append("update main catalog table-Success")
except Exception as e:
    output_list.append("update main catalog table-Failed: " + str(e))


# COMMAND ----------

# DBTITLE 1,Update stage catalog table
data = [("United States", "USA"), ("Canada", "CAN"), ("Australia", "AUS")]
try:
    upd_stg_df = spark.createDataFrame(data, ["City", "CountryCode"])
    upd_stg_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{stage_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
    output_list.append("update stage catalog table-Success")
except Exception as e:
    output_list.append("update stage catalog table-Failed: " + str(e))


# COMMAND ----------

# DBTITLE 1,Update raw catalog table
data = [("United States", "USA"), ("Canada", "CAN"), ("Australia", "AUS")]
try:
    upd_raw_df = spark.createDataFrame(data, ["City", "CountryCode"])
    upd_raw_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{raw_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
    output_list.append("update raw catalog table-Success")
except Exception as e:
    output_list.append("update raw catalog table-Failed: " + str(e))


# COMMAND ----------

# DBTITLE 1,Load data to sql
try:
    load_df_to_sf_sql_db_spark(upd_stg_df,f'{FILE_NAME}_TestAdfDatabricksConenctivity')
    output_list.append("Load data into SQL-Success")
except Exception as e:
    output_list.append("Load data into SQL-Failed: "+ str(e))
     

# COMMAND ----------

# DBTITLE 1,Read data from sql
try:
    read_sql=read_sf_sql_tbl_to_df_spark(f'{FILE_NAME}_TestAdfDatabricksConenctivity')
    display(read_sql.limit(5))
    output_list.append("Read data from SQL-Success")
except Exception as e:
    output_list.append("Read data from SQL-Failed: " + str(e))

# COMMAND ----------

# DBTITLE 1,Drop table
try:
    spark.sql(f"DROP TABLE {stage_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
    spark.sql(f"DROP TABLE {catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
    spark.sql(f"DROP TABLE {raw_catalog_name}.{schema_name}.{FILE_NAME}_sampleTable")
    output_list.append("drop tables from all catalogs-Success")
except Exception as e:
    output_list.append("drop tables from all catalogs-Failed: "+ str(e))


# COMMAND ----------

# # Delete table from SQL
# drop_table = spark.read.format('jdbc')\
#     .option("url", sf_url)\
#     .option("database", sf_db_name)\
#     .option("user", sf_user)\
#     .option("password", sf_password)\
#     .option('query', f"DELETE TABLE {FILE_NAME}_TestAdfDatabricksConenctivity")\
#     .load()
# print("Success: Table is dropeed")



# COMMAND ----------

# DBTITLE 1,Existing notebook
final_output = ', '.join(output_list)
dbutils.notebook.exit(final_output)
