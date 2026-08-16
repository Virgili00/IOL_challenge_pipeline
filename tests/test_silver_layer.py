# Databricks notebook source
# DBTITLE 1,Tests de Calidad - Silver Layer
# MAGIC %md
# MAGIC # Tests de Calidad de Datos - Silver Layer
# MAGIC
# MAGIC Validaciones automáticas para las tablas:
# MAGIC - silver.transacciones
# MAGIC - silver.cotizaciones
# MAGIC
# MAGIC ## Tipos de validaciones:
# MAGIC 1. **Completitud**: Campos obligatorios sin nulls
# MAGIC 2. **Unicidad**: PKs únicas
# MAGIC 3. **Integridad**: Rangos válidos, tipos correctos
# MAGIC 4. **Freshness**: Datos actualizados
# MAGIC 5. **Consistencia**: Relaciones entre tablas

# COMMAND ----------

# DBTITLE 1,Configuración
import json
from datetime import datetime

# Parámetros
dbutils.widgets.text("process_datetime", "")
dbutils.widgets.text("runId", "")
dbutils.widgets.text("schema", "silver")

process_datetime = dbutils.widgets.get("process_datetime")
runId = dbutils.widgets.get("runId")
schema = dbutils.widgets.get("schema")

# Contadores
test_failures = []
test_warnings = []
total_tests = 0

# COMMAND ----------

# DBTITLE 1,Función Helper de Validación
def run_test(test_name, query_failed_records, table_name, severity="CRITICAL"):
    """
    Ejecuta un test de calidad de datos y registra en audit.test_results.
    
    Args:
        test_name: Nombre descriptivo del test
        query_failed_records: Query SQL que retorna (record_id, failure_reason) de registros fallidos
        table_name: Nombre de la tabla siendo testeada
        severity: CRITICAL o WARNING
    
    Returns:
        True si el test pasa, False si falla
    """
    global total_tests, test_failures, test_warnings
    total_tests += 1
    
    test_id = f"{total_tests}_{runId}"
    test_status = "PASS"
    records_failed = 0
    test_message = ""
    
    try:
        # Ejecutar query que retorna registros fallidos
        failed_df = spark.sql(query_failed_records)
        records_failed = failed_df.count()
        
        if records_failed > 0:
            test_status = "FAIL" if severity == "CRITICAL" else "WARNING"
            test_message = f"{records_failed:,} registros fallaron"
            
            # Insertar registros fallidos en audit.test_failed_records (limitar a 10000)
            failed_df.limit(10000).createOrReplaceTempView("temp_failed_records")
            spark.sql(f"""
                INSERT INTO audit.test_failed_records
                SELECT 
                    '{test_id}' as test_id,
                    '{runId}' as run_id,
                    '{table_name}' as table_name,
                    '{test_name.replace("'", "\\'")[:200]}' as test_name,
                    record_id,
                    failure_reason,
                    CURRENT_TIMESTAMP() as created_at
                FROM temp_failed_records
            """)
            
            if severity == "CRITICAL":
                test_failures.append(f"{test_name}: {test_message}")
                print(f"✗ FAIL: {test_name}: {test_message}")
            else:
                test_warnings.append(f"{test_name}: {test_message}")
                print(f"⚠ WARNING: {test_name}: {test_message}")
        else:
            test_message = "Validación exitosa"
            print(f"✓ PASS: {test_name}")
    
    except Exception as e:
        test_status = "ERROR"
        test_message = str(e)[:500]
        test_failures.append(f"{test_name}: ERROR - {test_message}")
        print(f"ERROR: {test_name}: {test_message}")
    
    # Insertar registro en audit.test_results
    try:
        spark.sql(f"""
            INSERT INTO audit.test_results VALUES (
                '{test_id}',
                CAST('{process_datetime}' AS TIMESTAMP),
                '{runId}',
                'SILVER',
                '{table_name}',
                '{test_name.replace("'", "\\'")[:200]}',
                '{test_status}',
                '{test_message.replace("'", "\\'")[:500]}',
                0,
                {records_failed},
                CURRENT_TIMESTAMP()
            )
        """)
    except Exception as audit_error:
        print(f"No se pudo registrar en audit: {audit_error}")
    
    return test_status == "PASS"

# COMMAND ----------

# DBTITLE 1,Tests silver.transacciones
# MAGIC %md
# MAGIC ## Tests: silver.transacciones

# COMMAND ----------

# DBTITLE 1,Test 1: Completitud - Campos obligatorios
print("COMPLETITUD: Campos obligatorios")

# PK no puede ser null
run_test(
    "id_transaccion NO NULL",
    f"""
    SELECT 
        COALESCE(CAST(id_transaccion AS STRING), 'NULL_ID') as record_id,
        'id_transaccion es NULL' as failure_reason
    FROM {schema}.transacciones 
    WHERE id_transaccion IS NULL
    """,
    "silver.transacciones",
    "CRITICAL"
)

# Campos de negocio críticos
run_test(
    "fecha NO NULL",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        'fecha es NULL' as failure_reason
    FROM {schema}.transacciones 
    WHERE fecha IS NULL
    """,
    "silver.transacciones",
    "CRITICAL"
)

run_test(
    "id_cliente NO NULL",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        'id_cliente es NULL' as failure_reason
    FROM {schema}.transacciones 
    WHERE id_cliente IS NULL
    """,
    "silver.transacciones",
    "CRITICAL"
)

run_test(
    "simbolo_base NO NULL",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        'simbolo_base es NULL o vacío' as failure_reason
    FROM {schema}.transacciones 
    WHERE simbolo_base IS NULL OR simbolo_base = ''
    """,
    "silver.transacciones",
    "CRITICAL"
)

run_test(
    "cantidad NO NULL",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        'cantidad es NULL' as failure_reason
    FROM {schema}.transacciones 
    WHERE cantidad IS NULL
    """,
    "silver.transacciones",
    "CRITICAL"
)

run_test(
    "precio NO NULL",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        'precio es NULL' as failure_reason
    FROM {schema}.transacciones 
    WHERE precio IS NULL
    """,
    "silver.transacciones",
    "CRITICAL"
)

# COMMAND ----------

# DBTITLE 1,Test 2: Unicidad de PK
print("UNICIDAD: Primary Key")

run_test(
    "id_transaccion es único",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('id_transaccion duplicado: ', CAST(cnt AS STRING), ' veces') as failure_reason
    FROM (
        SELECT id_transaccion, COUNT(*) as cnt
        FROM {schema}.transacciones
        GROUP BY id_transaccion
        HAVING cnt > 1
    )
    """,
    "silver.transacciones",
    "CRITICAL"
)

# COMMAND ----------

# DBTITLE 1,Test 3: Integridad de datos - Rangos válidos
print("INTEGRIDAD: Rangos válidos")

# Cantidades positivas
run_test(
    "cantidad > 0",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('cantidad inválida: ', CAST(cantidad AS STRING)) as failure_reason
    FROM {schema}.transacciones 
    WHERE cantidad <= 0
    """,
    "silver.transacciones",
    "CRITICAL"
)

# Precios positivos
run_test(
    "precio > 0",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('precio inválido: ', CAST(precio AS STRING)) as failure_reason
    FROM {schema}.transacciones 
    WHERE precio <= 0
    """,
    "silver.transacciones",
    "CRITICAL"
)

# Fechas válidas (no futuras)
run_test(
    "fecha no es futura",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('fecha futura: ', CAST(fecha AS STRING)) as failure_reason
    FROM {schema}.transacciones 
    WHERE fecha > CURRENT_DATE()
    """,
    "silver.transacciones",
    "CRITICAL"
)

# Tipo de transacción válido
run_test(
    "tipo_transaccion válido",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('tipo_transaccion inválido: ', tipo_transaccion) as failure_reason
    FROM {schema}.transacciones 
    WHERE tipo_transaccion NOT IN ('COMPRA', 'VENTA', 'Compra', 'Venta')
    """,
    "silver.transacciones",
    "WARNING"
)

# Moneda válida
run_test(
    "moneda válida",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('moneda inválida: ', moneda) as failure_reason
    FROM {schema}.transacciones 
    WHERE moneda NOT IN ('ARS', 'USD')
    """,
    "silver.transacciones",
    "WARNING"
)

# COMMAND ----------

# DBTITLE 1,Test 3.1: Montos extremos y outliers
print("MONTOS EXTREMOS Y OUTLIERS")

# Transacciones con montos extremadamente altos (> 1 millón USD)
run_test(
    "monto_total < 1M USD",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('monto extremo: ', 
               CAST(ROUND(cantidad * precio, 2) AS STRING), 
               ' ', moneda) as failure_reason
    FROM {schema}.transacciones 
    WHERE (cantidad * precio) > 1000000 
      AND moneda = 'USD'
    """,
    "silver.transacciones",
    "WARNING"
)

# Transacciones con montos extremadamente altos en ARS (> 100M ARS)
run_test(
    "monto_total < 100M ARS",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('monto extremo ARS: ', 
               CAST(ROUND(cantidad * precio, 2) AS STRING)) as failure_reason
    FROM {schema}.transacciones 
    WHERE (cantidad * precio) > 100000000 
      AND moneda = 'ARS'
    """,
    "silver.transacciones",
    "WARNING"
)

# Precios unitarios extremos (> 100K USD por unidad)
run_test(
    "precio unitario < 100K USD",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('precio unitario extremo: ', 
               CAST(precio AS STRING), 
               ' USD para ', simbolo_base) as failure_reason
    FROM {schema}.transacciones 
    WHERE precio > 100000 
      AND moneda = 'USD'
    """,
    "silver.transacciones",
    "WARNING"
)

# Cantidades extremas (> 1 millón de unidades)
run_test(
    "cantidad < 1M unidades",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('cantidad extrema: ', 
               CAST(cantidad AS STRING), 
               ' unidades de ', simbolo_base) as failure_reason
    FROM {schema}.transacciones 
    WHERE cantidad > 1000000
    """,
    "silver.transacciones",
    "WARNING"
)

# COMMAND ----------

# DBTITLE 1,Test 4: Consistencia de enriquecimiento
print("CONSISTENCIA: Enriquecimiento de datos")

# Validar normalización de símbolos
run_test(
    "simbolo_base normalizado (sin sufijos D/C)",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('simbolo_base con sufijo: ', simbolo_base) as failure_reason
    FROM {schema}.transacciones 
    WHERE simbolo_base LIKE '%D' OR simbolo_base LIKE '%C'
    """,
    "silver.transacciones",
    "CRITICAL"
)

# Validar tipo_instrumento asignado
run_test(
    "tipo_instrumento asignado",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('tipo_instrumento NULL para simbolo: ', simbolo_base) as failure_reason
    FROM {schema}.transacciones 
    WHERE tipo_instrumento IS NULL
    """,
    "silver.transacciones",
    "WARNING"
)

# Validar variante_liquidacion
run_test(
    "variante_liquidacion válida",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        CONCAT('variante_liquidacion inválida: ', variante_liquidacion) as failure_reason
    FROM {schema}.transacciones 
    WHERE variante_liquidacion NOT IN ('MEP', 'CABLE', 'STANDARD')
    """,
    "silver.transacciones",
    "WARNING"
)

# Validar flag es_dia_habil
run_test(
    "es_dia_habil es booleano",
    f"""
    SELECT 
        CAST(id_transaccion AS STRING) as record_id,
        'es_dia_habil no es booleano' as failure_reason
    FROM {schema}.transacciones 
    WHERE es_dia_habil NOT IN (TRUE, FALSE)
    """,
    "silver.transacciones",
    "CRITICAL"
)

# COMMAND ----------

# DBTITLE 1,Tests silver.cotizaciones
# MAGIC %md
# MAGIC ## Tests: silver.cotizaciones

# COMMAND ----------

# DBTITLE 1,Test 6: Completitud cotizaciones
print("\n=== COMPLETITUD: silver.cotizaciones ===")

# Verificar que la tabla existe y tiene datos
try:
    count_cotizaciones = spark.sql(f"SELECT COUNT(*) FROM {schema}.cotizaciones").first()[0]
    print(f"Total registros en cotizaciones: {count_cotizaciones:,}")
    
    if count_cotizaciones > 0:
        run_test(
            "simbolo_base NO NULL en cotizaciones",
            f"""
            SELECT 
                COALESCE(CONCAT(simbolo_base, '_', CAST(fecha_cotizacion AS STRING)), 'NULL_ID') as record_id,
                'simbolo_base es NULL' as failure_reason
            FROM {schema}.cotizaciones 
            WHERE simbolo_base IS NULL
            """,
            "silver.cotizaciones",
            "CRITICAL"
        )
        
        run_test(
            "fecha_cotizacion NO NULL",
            f"""
            SELECT 
                CONCAT(simbolo_base, '_NULL_DATE') as record_id,
                'fecha_cotizacion es NULL' as failure_reason
            FROM {schema}.cotizaciones 
            WHERE fecha_cotizacion IS NULL
            """,
            "silver.cotizaciones",
            "CRITICAL"
        )
        
        run_test(
            "precio_final NO NULL y > 0",
            f"""
            SELECT 
                CONCAT(simbolo_base, '_', CAST(fecha_cotizacion AS STRING)) as record_id,
                CONCAT('precio_final inválido: ', CAST(precio_final AS STRING)) as failure_reason
            FROM {schema}.cotizaciones 
            WHERE precio_final IS NULL OR precio_final <= 0
            """,
            "silver.cotizaciones",
            "CRITICAL"
        )
        
        # Unicidad: Un símbolo + fecha debe tener solo un precio
        run_test(
            "Unicidad simbolo_base + fecha_cotizacion",
            f"""
            SELECT 
                CONCAT(simbolo_base, '_', CAST(fecha_cotizacion AS STRING)) as record_id,
                CONCAT('Duplicado: ', CAST(cnt AS STRING), ' registros') as failure_reason
            FROM (
                SELECT simbolo_base, fecha_cotizacion, COUNT(*) as cnt
                FROM {schema}.cotizaciones
                GROUP BY simbolo_base, fecha_cotizacion
                HAVING cnt > 1
            )
            """,
            "silver.cotizaciones",
            "CRITICAL"
        )
    else:
        print("⚠ WARNING: Tabla cotizaciones está vacía")
        test_warnings.append("silver.cotizaciones está vacía")
except Exception as e:
    print(f"⚠ WARNING: No se pudo acceder a silver.cotizaciones - {str(e)}")
    test_warnings.append(f"silver.cotizaciones no accesible: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Resumen y Exit
# Exit con JSON
result = {
    "status": status,
    "result": result_flag,
    "records_read": total_tests,
    "records_written": 0,
    "error_message": "; ".join(test_failures) if test_failures else None,
    "table_name": "test_silver_layer",
    "layer": "SILVER",
    "total_tests": total_tests,
    "failures": len(test_failures),
    "warnings": len(test_warnings)
}

dbutils.notebook.exit(json.dumps(result))