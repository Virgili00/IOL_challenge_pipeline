# Databricks notebook source
# DBTITLE 1,CREATE bronze.cotizaciones_mercado
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.cotizaciones_mercado (
# MAGIC     simbolo_base STRING COMMENT 'Símbolo normalizado sin sufijos',
# MAGIC     fecha_cotizacion DATE COMMENT 'Fecha de la cotización',
# MAGIC     precio_mercado DOUBLE COMMENT 'Precio de cierre del mercado (NULL si no disponible)',
# MAGIC     error_msg STRING COMMENT 'Mensaje de error si la cotización falló',
# MAGIC     fecha_auditoria TIMESTAMP COMMENT 'Fecha y hora de ingesta'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Cotizaciones de mercado obtenidas de Yahoo Finance API. Contiene NULLs explícitos cuando el API no retorna datos para un símbolo/fecha.'

# COMMAND ----------

