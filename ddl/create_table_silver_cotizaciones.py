# Databricks notebook source
# DBTITLE 1,CREATE silver.cotizaciones
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.cotizaciones (
# MAGIC     simbolo_base STRING COMMENT 'Símbolo normalizado sin sufijos',
# MAGIC     fecha_cotizacion DATE COMMENT 'Fecha de la cotización',
# MAGIC     precio_mercado DOUBLE COMMENT 'Precio del API de Yahoo Finance (puede ser NULL)',
# MAGIC     precio_final DOUBLE COMMENT 'Precio consolidado: API o fallback a precio de transacción',
# MAGIC     fuente_precio STRING COMMENT 'Fuente del precio: API, TRANSACTION_PRICE, MISSING',
# MAGIC
# MAGIC     error_msg STRING COMMENT 'Mensaje de error si la cotización del API falló'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Cotizaciones enriquecidas con fallback automático al precio de transacción cuando el API falla. Validadas para outliers.'

# COMMAND ----------

