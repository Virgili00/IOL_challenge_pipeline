-- Databricks notebook source
-- DBTITLE 1,Inicialización de varables
-- MAGIC %python
-- MAGIC dbutils.widgets.text("process_datetime", "")
-- MAGIC dbutils.widgets.text("runId", "")
-- MAGIC
-- MAGIC process_datetime = dbutils.widgets.get("process_datetime")
-- MAGIC runId = dbutils.widgets.get("runId")
-- MAGIC records_read = 0
-- MAGIC written_records = 0
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Vistas de novedades

-- COMMAND ----------

-- DBTITLE 1,news_dim_fecha
CREATE OR REPLACE TEMPORARY VIEW news_dim_fecha AS 
SELECT DISTINCT
    DATE(fecha) AS fecha,
    YEAR(fecha) AS anio,
    MONTH(fecha) AS mes,
    QUARTER(fecha) AS trimestre,
    DAYOFMONTH(fecha) AS dia,
    DAYOFWEEK(fecha) AS dia_semana,
    WEEKOFYEAR(fecha) AS semana_anio,
    CASE WHEN DAYOFWEEK(fecha) IN (1, 7) THEN FALSE ELSE TRUE END AS es_dia_habil,
    DATE_FORMAT(fecha, 'MMMM') AS nombre_mes,
    DATE_FORMAT(fecha, 'EEEE') AS nombre_dia
  FROM silver.transacciones
  WHERE fecha_auditoria >= :process_datetime

-- COMMAND ----------

-- DBTITLE 1,news_instrumento
CREATE OR REPLACE TEMPORARY VIEW news_dim_instrumento AS 
SELECT DISTINCT
    simbolo_base,
    simbolo_original,
    tipo_instrumento,
    variante_liquidacion,
    descripcion_titulo
  FROM silver.transacciones
  WHERE fecha_auditoria >= :process_datetime

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW news_dim_clientes AS
SELECT 
    id_cliente,
    COUNT(DISTINCT DATE(fecha)) AS dias_activos,
    COUNT(*) AS total_transacciones,
    SUM(cantidad * precio) AS volumen_total,
    MIN(fecha) AS primera_transaccion,
    MAX(fecha) AS ultima_transaccion
FROM silver.transacciones
GROUP BY id_cliente

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW news_dim_origen AS
SELECT 
    DISTINCT origen
FROM silver.transacciones
WHERE fecha_auditoria >= :process_datetime

-- COMMAND ----------

-- MAGIC %python
-- MAGIC news_list = ["news_dim_fecha","news_dim_instrumento","news_dim_clientes","news_dim_origen"]
-- MAGIC for view_name in news_list:  
-- MAGIC     records_read += spark.sql(f"""select COUNT(1) FROM {view_name}""").first()[0]
-- MAGIC
-- MAGIC if records_read < 0:
-- MAGIC     result = {
-- MAGIC     "status": "SUCCESS",
-- MAGIC     "records_read": records_read.first()[0],
-- MAGIC     "records_written": written_records.first()[0],
-- MAGIC     "error_message": None,
-- MAGIC     "table_name": "precios_silver.cotizaciones"
-- MAGIC     }
-- MAGIC     dbutils.notebook.exit(json.dumps(result))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Merge dimensiones

-- COMMAND ----------

-- DBTITLE 1,dim_fecha
-- dim_fecha: Calendario completo con atributos temporales
MERGE INTO gold.dim_fecha AS target
USING news_dim_fecha AS source
ON target.fecha = source.fecha
WHEN NOT MATCHED THEN INSERT (
  fecha, anio, mes, trimestre, dia, dia_semana, semana_anio, 
  es_dia_habil, nombre_mes, nombre_dia
) VALUES (
  source.fecha, source.anio, source.mes, source.trimestre, source.dia, 
  source.dia_semana, source.semana_anio, source.es_dia_habil, 
  source.nombre_mes, source.nombre_dia
)

-- COMMAND ----------

-- DBTITLE 1,dim_instrumento
-- dim_instrumento: Símbolos con tipo y variante de liquidación
MERGE INTO gold.dim_instrumento AS target
USING news_dim_instrumento AS source
ON target.simbolo_base = source.simbolo_base 
   AND target.variante_liquidacion = source.variante_liquidacion
WHEN NOT MATCHED THEN INSERT (
  simbolo_base, simbolo_original, tipo_instrumento, 
  variante_liquidacion, descripcion_titulo
) VALUES (
  source.simbolo_base, source.simbolo_original, source.tipo_instrumento,
  source.variante_liquidacion, source.descripcion_titulo
)

-- COMMAND ----------

-- DBTITLE 1,dim_cliente
-- dim_cliente: Clientes con métricas agregadas
MERGE INTO gold.dim_cliente AS target
USING news_dim_clientes AS source
ON target.id_cliente = source.id_cliente
WHEN MATCHED THEN UPDATE SET
  target.dias_activos = source.dias_activos,
  target.total_transacciones = source.total_transacciones,
  target.volumen_total = source.volumen_total,
  target.primera_transaccion = source.primera_transaccion,
  target.ultima_transaccion = source.ultima_transaccion
WHEN NOT MATCHED THEN INSERT (
  id_cliente, dias_activos, total_transacciones, volumen_total,
  primera_transaccion, ultima_transaccion
) VALUES (
  source.id_cliente, source.dias_activos, source.total_transacciones,
  source.volumen_total, source.primera_transaccion, source.ultima_transaccion
)

-- COMMAND ----------

-- DBTITLE 1,dim_origen
-- dim_origen: Canales de transacción
MERGE INTO gold.dim_origen AS target
USING
  news_dim_origen AS source
ON target.origen = source.origen
WHEN NOT MATCHED THEN INSERT (origen) VALUES (source.origen)

-- COMMAND ----------

-- DBTITLE 1,Sección: Fact Table
-- MAGIC %md
-- MAGIC ### Paso 2: Construir Fact Table
-- MAGIC Unir transacciones con cotizaciones y enriquecer con surrogate keys de las dimensiones.

-- COMMAND ----------

-- DBTITLE 1,Staging: Unión transacciones + cotizaciones
CREATE OR REPLACE TEMPORARY VIEW transacciones_enriquecidas AS
SELECT 
  t.id_transaccion,
  t.fecha,
  t.tipo_transaccion,
  t.id_cliente,
  t.simbolo_base,
  t.variante_liquidacion,
  t.origen,
  t.cantidad,
  t.precio AS precio_transaccion,
  t.moneda,
  c.precio_final AS precio_mercado,
  c.fuente_precio,
  t.fecha_auditoria
FROM silver.transacciones t
LEFT JOIN silver.cotizaciones c 
  ON t.simbolo_base = c.simbolo_base 
  AND DATE(t.fecha) = c.fecha_cotizacion
WHERE t.fecha_auditoria >= :process_datetime

-- COMMAND ----------

-- DBTITLE 1,MERGE a fact_transacciones
-- Insertar a fact_transacciones con surrogate keys
MERGE INTO gold.fact_transacciones AS target
USING (
  SELECT 
    t.id_transaccion,
    df.fecha_sk,
    di.instrumento_sk,
    dc.cliente_sk,
    do.origen_sk,
    t.tipo_transaccion,
    t.cantidad,
    t.precio_transaccion,
    t.precio_mercado,
    t.cantidad * t.precio_transaccion AS importe,
    t.moneda,
    t.fuente_precio,
    t.fecha_auditoria
  FROM transacciones_enriquecidas t
  INNER JOIN gold.dim_fecha df 
    ON DATE(t.fecha) = df.fecha
  INNER JOIN gold.dim_instrumento di 
    ON t.simbolo_base = di.simbolo_base 
    AND t.variante_liquidacion = di.variante_liquidacion
  INNER JOIN gold.dim_cliente dc 
    ON t.id_cliente = dc.id_cliente
  INNER JOIN gold.dim_origen do 
    ON t.origen = do.origen
) AS source
ON target.id_transaccion = source.id_transaccion
WHEN NOT MATCHED THEN INSERT (
  id_transaccion, fecha_sk, instrumento_sk, cliente_sk, origen_sk,
  tipo_transaccion, cantidad, precio_transaccion, precio_mercado_final,
  importe, moneda, fuente_precio, es_outlier, fecha_auditoria
) VALUES (
  source.id_transaccion, source.fecha_sk, source.instrumento_sk, 
  source.cliente_sk, source.origen_sk, source.tipo_transaccion,
  source.cantidad, source.precio_transaccion, source.precio_mercado_final,
  source.importe, source.moneda, source.fuente_precio, source.es_cotizacion_outlier,
  source.fecha_auditoria
)