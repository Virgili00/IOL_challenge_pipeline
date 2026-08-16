# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Orquestador Pipeline IOL
# MAGIC %md
# MAGIC #Orquestador Pipeline IOL - Arquitectura Medallion
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuración y Parámetros
from datetime import datetime
import time

dbutils.widgets.text("process_datetime", "2026-08-16T19:54:26.830+00:00", "Process DateTime (ISO 8601)")
dbutils.widgets.text("runId", "", "Run ID (opcional)")

process_datetime = dbutils.widgets.get("process_datetime")
runId = dbutils.widgets.get("runId") or f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


# COMMAND ----------


if not process_datetime:
    raise ValueError("process_datetime es requerido. Formato: 2026-01-01T00:00:00Z")

print(f"Iniciando pipeline - Process DateTime: {process_datetime} - Run ID: {runId}")

# COMMAND ----------

# DBTITLE 1,Funciones Helper
import json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

def registrar_auditoria(runId, process_datetime, layer, table_name, notebook_path, 
                        status, duration_seconds, records_read, records_written, error_msg=None):
    """
    Registra la ejecución en la tabla de auditoría completa.
    
    Args:
        runId: ID de la ejecución del pipeline
        process_datetime: DateTime de procesamiento
        layer: Capa (BRONZE, SILVER, GOLD)
        table_name: Tabla procesada
        notebook_path: Path del notebook ejecutado
        status: SUCCESS o FAILED
        duration_seconds: Duración en segundos
        records_read: Registros leídos
        records_written: Registros escritos
        error_msg: Mensaje de error (None si exitoso)
    """
    try:
        execution_id = f"{runId}_{layer}_{table_name}_{int(time.time())}".replace(" ", "_")
        
        # Definir schema explícito para evitar errores de inferencia de tipos
        schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("process_datetime", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("layer", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("notebook_path", StringType(), False),
            StructField("status", StringType(), False),
            StructField("duration_seconds", DoubleType(), False),
            StructField("records_read", IntegerType(), False),
            StructField("records_written", IntegerType(), False),
            StructField("error_message", StringType(), True),
            StructField("error_detail", StringType(), True)
        ])
        
        audit_df = spark.createDataFrame([(
            execution_id,
            runId,
            process_datetime,
            datetime.now(),
            layer,
            table_name,
            notebook_path,
            status,
            round(duration_seconds, 2),
            records_read,
            records_written,
            error_msg[:500] if error_msg else None,
            error_msg if error_msg else None
        )], schema)
        
        audit_df.write.mode("append").saveAsTable(f"audit.pipeline_executions")
        print(f"✓ Registrado en auditoría: {execution_id}")
        
    except Exception as audit_error:
        print(f"✗ No se pudo registrar en auditoría: {str(audit_error)}")


def ejecutar_notebook(ruta, timeout_seconds=3600, parametros=None, layer="", table_name=""):
    """
    Ejecuta un notebook y retorna el resultado parseado.
    Los notebooks deben devolver JSON con:
    {
        "status": "SUCCESS|FAILED",
        "records_read": int,
        "records_written": int,
        "error_message": str|None,
        "table_name": str,
        "layer": str
    }
    
    Args:
        ruta: Path del notebook
        timeout_seconds: Timeout en segundos
        parametros: Dict con parámetros a pasar
        layer: Capa del pipeline (BRONZE, SILVER, GOLD)
        table_name: Nombre de la tabla siendo procesada
    
    Returns:
        dict con status, duration_seconds, records_read, records_written, error
    """
    inicio = time.time()
    params = parametros or {}
    
    try:
        print(f"Ejecutando: {ruta}")
        print(f"Parámetros: {params}")
        
        resultado_json = dbutils.notebook.run(
            ruta, 
            timeout_seconds, 
            params
        )
        
        duracion = time.time() - inicio
        
        try:
            resultado = json.loads(resultado_json)
            records_read = resultado.get("records_read", 0)
            records_written = resultado.get("records_written", 0)
            
            print(f"✓ Completado en {duracion:.2f}s")
            print(f"   Registros leídos: {records_read:,}")
            print(f"   Registros escritos: {records_written:,}")
            
            # Registrar ejecución exitosa en auditoría
            registrar_auditoria(
                runId=runId,
                process_datetime=process_datetime,
                layer=layer,
                table_name=resultado.get("table_name", table_name),
                notebook_path=ruta,
                status="SUCCESS",
                duration_seconds=duracion,
                records_read=records_read,
                records_written=records_written,
                error_msg=None
            )
            
            return {
                "status": "success",
                "duration_seconds": duracion,
                "records_read": records_read,
                "records_written": records_written,
                "table_name": resultado.get("table_name", table_name),
                "layer": resultado.get("layer", layer),
                "error": None
            }
        except json.JSONDecodeError:
            # Compatibilidad con notebooks que devuelven strings simples
            print(f"✓ Completado en {duracion:.2f}s (respuesta no-JSON: {resultado_json})")
            
            # Registrar en auditoría (sin métricas detalladas)
            registrar_auditoria(
                runId=runId,
                process_datetime=process_datetime,
                layer=layer,
                table_name=table_name,
                notebook_path=ruta,
                status="SUCCESS",
                duration_seconds=duracion,
                records_read=0,
                records_written=0,
                error_msg=None
            )
            
            return {
                "status": "success",
                "duration_seconds": duracion,
                "records_read": 0,
                "records_written": 0,
                "table_name": table_name,
                "layer": layer,
                "error": None,
                "raw_result": resultado_json
            }
        
    except Exception as e:
        duracion = time.time() - inicio
        error_msg = str(e)
        print(f"✗ Falló después de {duracion:.2f}s")
        print(f"   Error: {error_msg}")
        
        # Registrar ejecución fallida en auditoría completa
        registrar_auditoria(
            runId=runId,
            process_datetime=process_datetime,
            layer=layer,
            table_name=table_name,
            notebook_path=ruta,
            status="FAILED",
            duration_seconds=duracion,
            records_read=0,
            records_written=0,
            error_msg=error_msg
        )
        

        return {
            "status": "failed",
            "duration_seconds": duracion,
            "records_read": 0,
            "records_written": 0,
            "table_name": table_name,
            "layer": layer,
            "error": error_msg
        }

def imprimir_resumen(resultados):
    """
    Imprime resumen de ejecución con métricas de registros.
    """
    print("\n" + "="*80)
    print("📊 RESUMEN DE EJECUCIÓN DEL PIPELINE")
    print("="*80)
    
    total_duracion = sum(r["duration_seconds"] for r in resultados.values())
    exitosos = sum(1 for r in resultados.values() if r["status"] == "success")
    fallidos = sum(1 for r in resultados.values() if r["status"] == "failed")
    total_read = sum(r.get("records_read", 0) for r in resultados.values())
    total_written = sum(r.get("records_written", 0) for r in resultados.values())
    
    print(f"\nDuración total: {total_duracion:.2f}s ({total_duracion/60:.2f} min)")
    print(f"Exitosos: {exitosos}")
    print(f"Fallidos: {fallidos}")
    print(f"Total registros leídos: {total_read:,}")
    print(f"Total registros escritos: {total_written:,}")
    print("Detalle por notebook:")
    
    for nombre, resultado in resultados.items():
        icono = "success" if resultado["status"] == "success" else "failed"
        duracion = resultado['duration_seconds']
        read = resultado.get('records_read', 0)
        written = resultado.get('records_written', 0)
        
        print(f"\n{icono} {nombre}")
        print(f"   Tiempo: {duracion:.2f}s")
        if read > 0 or written > 0:
            print(f"Leídos: {read:,} | Escritos: {written:,}")
        
        if resultado["error"]:
            print(f" Error: {resultado['error'][:100]}...")

# COMMAND ----------

# DBTITLE 1,Ejecutar Bronze transacciones
resultados = {}
parametros_bronze = {
    "process_datetime": process_datetime,
    "runId": runId
}

print("BRONZE LAYER - TRANSACCIONES")


# 1. Transacciones Bronze
resultados["transacciones_bronze"] = ejecutar_notebook(
    "./process/transacciones_bronze",
    timeout_seconds=1800,
    parametros=parametros_bronze,
    layer="BRONZE",
    table_name="transacciones_bronze"
)

# Verificar si Bronze Transacciones falló
if resultados["transacciones_bronze"]["status"] == "failed":
    raise Exception("Bronze transacciones falló. Abortando pipeline.")

print("\nBronze transacciones completado")

# COMMAND ----------

# DBTITLE 1,Ejecutar Silver Transacciones
parametros_silver = {
    "process_datetime": process_datetime,
    "runId": runId,
}

print("SILVER LAYER - TRANSACCIONES")

# 2. Transacciones Silver
resultados["transacciones_silver"] = ejecutar_notebook(
    "./process/transacciones_silver",
    timeout_seconds=1800,
    parametros=parametros_silver,
    layer="SILVER",
    table_name="transacciones_silver"
)

# Verificar si Transacciones Silver falló
if resultados["transacciones_silver"]["status"] == "failed":
    raise Exception("Transacciones Silver falló. Abortando pipeline.")

print("\nTransacciones Silver completado")


# COMMAND ----------

# DBTITLE 1,Precios Pipeline
print("BRONZE LAYER - PRECIOS (lee de transacciones_silver)")

resultados["precios_bronze"] = ejecutar_notebook(
    "./process/precios_bronze",
    timeout_seconds=7200, #porque la bajada tarda
    parametros=parametros_bronze,
    layer="BRONZE",
    table_name="precios_bronze"
)

if resultados["precios_bronze"]["status"] == "failed":
    raise Exception("Precios Bronze falló. Abortando pipeline.")

print("\nPrecios Bronze completado")


print("SILVER LAYER - PRECIOS")

resultados["precios_silver"] = ejecutar_notebook(
    "./process/precios_silver",
    timeout_seconds=1800,
    parametros=parametros_silver,
    layer="SILVER",
    table_name="precios_silver"
)

if resultados["precios_silver"]["status"] == "failed":
    raise Exception("Precios Silver falló. Abortando pipeline.")

print("\nPrecios Silver completado")

# COMMAND ----------

# DBTITLE 1,Test Silver
print("TESTS - SILVER LAYER")

parametros_test_silver = {
    "process_datetime": process_datetime,
    "runId": runId,
    "schema": "silver"
}

resultados["test_silver"] = ejecutar_notebook(
    "./tests/test_silver_layer",
    timeout_seconds=600,
    parametros=parametros_test_silver,
    layer="SILVER",
    table_name="test_silver_layer"
)

if resultados["test_silver"]["status"] == "failed":
    raise Exception("Tests de Silver fallaron. Abortando pipeline.")

print("\nTests de Silver completados exitosamente")

# COMMAND ----------

# DBTITLE 1,Ejecutar Refined
parametros_refined = {
    "process_datetime": process_datetime,
    "runId": runId
}


print("GOLD/REFINED LAYER")

# 5. Transacciones Refined (Star Schema)
resultados["transacciones_gold"] = ejecutar_notebook(
    "./process/transacciones_gold",
    timeout_seconds=1800,
    parametros=parametros_refined,
    layer="GOLD",
    table_name="transacciones_gold"
)

# Verificar si Refined falló
if resultados["transacciones_refined"]["status"] == "failed":
    raise Exception("Refined layer falló. Abortando pipeline.")

print("\nRefined layer completado")



# COMMAND ----------

# DBTITLE 1,Resumen Final
# Imprimir resumen completo
imprimir_resumen(resultados)

# Verificar éxito total
if all(r["status"] == "success" for r in resultados.values()):
    print("\nPIPELINE COMPLETADO EXITOSAMENTE")
    dbutils.notebook.exit("SUCCESS")
else:
    print("\nPIPELINE COMPLETADO CON ERRORES")
    dbutils.notebook.exit("FAILED")