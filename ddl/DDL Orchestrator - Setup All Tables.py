# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Orchestrator DDL
# MAGIC %md
# MAGIC # Orchestrator DDL - IOL Challenge Pipeline
# MAGIC
# MAGIC Ejecuta todos los DDLs necesario para el pipeline
# MAGIC modificar el base path

# COMMAND ----------

# DBTITLE 1,Configuración
import json
from datetime import datetime

# Directorio base
base_path = "/Users/fedevirgili00@gmail.com/IOL_challenge_pipeline/ddl"

# Resultados de ejecución
results = []
start_time = datetime.now()

# COMMAND ----------

# DBTITLE 1,1. Crear Schemas
print("PASO 1: Creando schemas (bronze, silver, gold)")
try:
    result = dbutils.notebook.run(
        f"{base_path}/create_schemas",
        timeout_seconds=300
    )
    results.append({"notebook": "create_schemas", "status": "SUCCESS", "result": result})
    print("Schemas creados exitosamente")
except Exception as e:
    results.append({"notebook": "create_schemas", "status": "ERROR", "error": str(e)})
    print(f"Error: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,2. Crear Tablas de Auditoría
print("PASO 2: Creando tablas de auditoría")
try:
    result = dbutils.notebook.run(
        f"{base_path}/create_tables_audits",
        timeout_seconds=300
    )
    results.append({"notebook": "create_tables_audits", "status": "SUCCESS", "result": result})
    print("Tablas de auditoría creadas exitosamente")
except Exception as e:
    results.append({"notebook": "create_tables_audits", "status": "ERROR", "error": str(e)})
    print(f"Error: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,3. Crear Tablas Bronze
print("PASO 3: Creando tablas Bronze")

# bronze.transacciones
try:
    result = dbutils.notebook.run(
        f"{base_path}/create_table_bronze_transacciones",
        timeout_seconds=300
    )
    results.append({"notebook": "create_table_bronze_transacciones", "status": "SUCCESS", "result": result})
    print("✓ bronze.transacciones creada")
except Exception as e:
    results.append({"notebook": "create_table_bronze_transacciones", "status": "ERROR", "error": str(e)})
    print(f"✗ Error: {e}")
    raise

# bronze.cotizaciones_mercado
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.cotizaciones_mercado (
            simbolo_base STRING NOT NULL,
            fecha_cotizacion STRING NOT NULL,
            precio_mercado DOUBLE,
            error_msg STRING
        )
        USING DELTA
        COMMENT 'Cotizaciones de mercado obtenidas desde Yahoo Finance API'
    """)
    results.append({"notebook": "create_table_bronze_cotizaciones", "status": "SUCCESS", "result": "Created"})
    print("bronze.cotizaciones_mercado creada")
except Exception as e:
    results.append({"notebook": "create_table_bronze_cotizaciones", "status": "ERROR", "error": str(e)})
    print(f"Error: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,4. Crear Tablas Silver
print("\n" + "=" * 80)
print("PASO 4: Creando tablas Silver")
print("=" * 80)

# silver.transacciones
try:
    result = dbutils.notebook.run(
        f"{base_path}/create_table_silver_transacciones",
        timeout_seconds=300
    )
    results.append({"notebook": "create_table_silver_transacciones", "status": "SUCCESS", "result": result})
    print("silver.transacciones creada")
except Exception as e:
    results.append({"notebook": "create_table_silver_transacciones", "status": "ERROR", "error": str(e)})
    print(f"Error: {e}")
    raise

# silver.cotizaciones
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.cotizaciones (
            simbolo_base STRING NOT NULL,
            fecha_cotizacion STRING NOT NULL,
            precio_mercado DOUBLE,
            precio_final DOUBLE NOT NULL,
            fuente_precio STRING NOT NULL,
            es_outlier BOOLEAN,
            error_msg STRING
        )
        USING DELTA
        COMMENT 'Cotizaciones consolidadas con fallback y detección de outliers'
    """)
    results.append({"notebook": "create_table_silver_cotizaciones", "status": "SUCCESS", "result": "Created"})
    print("silver.cotizaciones creada")
except Exception as e:
    results.append({"notebook": "create_table_silver_cotizaciones", "status": "ERROR", "error": str(e)})
    print(f"Error: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,5. Crear Dimensiones Gold
print("PASO 5: Creando dimensiones Gold")
dimensiones = [
    "create_table_gold_dim_cliente",
    "create_table_gold_dim_fecha",
    "create_table_gold_dim_instrumento",
    "create_table_gold_dim_origen"
]

for dim in dimensiones:
    try:
        result = dbutils.notebook.run(
            f"{base_path}/{dim}",
            timeout_seconds=300
        )
        results.append({"notebook": dim, "status": "SUCCESS", "result": result})
        print(f"{dim.replace('create_table_', '')} creada")
    except Exception as e:
        results.append({"notebook": dim, "status": "ERROR", "error": str(e)})
        print(f"✗ Error en {dim}: {e}")
        raise

# COMMAND ----------

# DBTITLE 1,6. Crear Tabla de Hechos Gold
print("PASO 6: Creando tabla de hechos Gold")
try:
    result = dbutils.notebook.run(
        f"{base_path}/create_table_gold_fact_transacciones",
        timeout_seconds=300
    )
    results.append({"notebook": "create_table_gold_fact_transacciones", "status": "SUCCESS", "result": result})
    print("gold.fact_transacciones creada")
except Exception as e:
    results.append({"notebook": "create_table_gold_fact_transacciones", "status": "ERROR", "error": str(e)})
    print(f"Error: {e}")
    raise