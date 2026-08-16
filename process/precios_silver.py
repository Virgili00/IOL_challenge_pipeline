# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import json

# COMMAND ----------

# DBTITLE 1,Widget parameters
dbutils.widgets.text("process_datetime", "")
dbutils.widgets.text("runId", "")

process_datetime = dbutils.widgets.get("process_datetime")
runId = dbutils.widgets.get("runId")

# COMMAND ----------

# DBTITLE 1,Filtrar batch incremental
# MAGIC %sql
# MAGIC -- Filtrar cotizaciones del batch incremental
# MAGIC CREATE OR REPLACE TEMPORARY VIEW news_cotizaciones AS
# MAGIC SELECT 
# MAGIC     simbolo_base,
# MAGIC     fecha_cotizacion,
# MAGIC     precio_mercado,
# MAGIC     error_msg
# MAGIC FROM bronze.cotizaciones_mercado
# MAGIC WHERE fecha_cotizacion >= DATE(:process_datetime)

# COMMAND ----------

# MAGIC %md
# MAGIC Conteo de novedades

# COMMAND ----------

records_read = spark.sql(f"""
    SELECT count(1) FROM news_cotizaciones
""")
if records_read.first()[0] == 0:
    result = {
        "status": "SUCCESS",
        "records_read": 0,
        "records_written": 0,
        "error_message": None,
        "table_name": "precios_silver",
        "layer": "SILVER"
    }
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cotizaciones_enriquecidas AS
# MAGIC SELECT 
# MAGIC   simbolo_base,
# MAGIC   fecha_cotizacion,
# MAGIC   precio_mercado,
# MAGIC   COALESCE(precio_mercado, precio) AS precio_final,
# MAGIC   CASE 
# MAGIC     WHEN precio_mercado IS NOT NULL THEN 'API'
# MAGIC     WHEN precio_transaccion IS NOT NULL THEN 'TRANSACTION_PRICE'
# MAGIC     ELSE 'MISSING'
# MAGIC   END AS fuente_precio,
# MAGIC   error_msg
# MAGIC FROM news_cotizaciones

# COMMAND ----------

# DBTITLE 1,MERGE a silver.cotizaciones
try :
    merge_cotizaciones = spark.sql(f"""
    MERGE INTO silver.cotizaciones AS target
    USING cotizaciones_enriquecidas AS source
    ON target.simbolo_base = source.simbolo_base 
    AND target.fecha_cotizacion = source.fecha_cotizacion
    WHEN MATCHED THEN 
    UPDATE SET 
        target.precio_mercado = source.precio_mercado,
        target.precio_final = source.precio_final,
        target.fuente_precio = source.fuente_precio,
        target.error_msg = source.error_msg
    WHEN NOT MATCHED THEN 
    INSERT (
        simbolo_base,
        fecha_cotizacion,
        precio_mercado,
        precio_final,
        fuente_precio,
        es_outlier,
        error_msg
    ) VALUES (
        source.simbolo_base,
        source.fecha_cotizacion,
        source.precio_mercado,
        source.precio_final,
        source.es_outlier,
        source.error_msg
    )"""
)
except Exception as e:
    result = {
        "status": "ERROR",
        "records_read": records_read.first()[0],
        "records_written": 0,
        "error_message": e,
        "table_name": "precios_silver.cotizaciones"
    }
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

result = {
    "status": "SUCCESS",
    "records_read": records_read.first()[0],
    "records_written": merge_cotizaciones.first()[0],
    "error_message": None,
    "table_name": "precios_silver.cotizaciones"
}
dbutils.notebook.exit(json.dumps(result))