# Procesamiento de Datos con Apache Spark y Kafka

Este repositorio contiene la implementación de soluciones de Big Data para el procesamiento de grandes volúmenes de datos en modalidades de lote (batch) y tiempo real (streaming). El proyecto utiliza datos históricos de la Tasa de Cambio Representativa del Mercado (TRM) de Colombia.

## Tecnologías Utilizadas

- **Apache Spark**: Procesamiento distribuido mediante RDDs y DataFrames.
- **Apache Kafka**: Gestión de mensajería y flujos de datos en tiempo real.
- **PySpark**: Interfaz de Python para el procesamiento de datos masivos.
- **Spark Streaming**: Micro-batching para el análisis de datos dinámicos.
- **Hadoop**: Entorno de almacenamiento y soporte de infraestructura.

## Componentes del Proyecto

### 1. Procesamiento en Batch

Implementación de aplicaciones Spark para el análisis de conjuntos de datos históricos provenientes de fuentes externas. Se incluyen procesos de:

- Ingesta y limpieza de datos masivos.
- Transformación y análisis exploratorio (EDA).
- Generación de resultados estadísticos y visualizaciones.

### 2. Procesamiento en Tiempo Real

Configuración de una arquitectura de productor-consumidor para el tratamiento de datos en movimiento:

- **Productor (Kafka)**: Lectura y envío de datos históricos de TRM simulando un flujo continuo en tiempo real.
- **Consumidor (Spark Streaming)**: Procesamiento de datos mediante ventanas de tiempo de 30 segundos para cálculos agregados.
- **Monitoreo**: Uso de la interfaz web de Spark (puerto 4040) para el seguimiento de tareas y etapas del procesamiento.

## Instrucciones de Ejecución

Para el correcto funcionamiento del sistema de streaming, se deben seguir estos pasos dentro de la máquina virtual configurada:

1. **Inicio de servicios**: Ejecutar ZooKeeper y el servidor de Kafka.
2. **Configuración del Topic**: Crear el canal de comunicación necesario para los mensajes.
   ```bash
   kafka-topics.sh --create --topic trm_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
3. **Ejecución del Productor**:
   ```bash
   python3 kafka_producer.py
   ```
4. **Ejecución del Consumidor de Spark** (en otra terminal):
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
   ```

---

## Dataset: Tasa de Cambio Representativa del Mercado (TRM)

### Descripción General del Dataset

El dataset utilizado es **"Tasa de Cambio Representativa del Mercado – Histórico"** proporcionado por la Superintendencia Financiera de Colombia y disponible en el portal de datos abiertos del gobierno colombiano:
https://www.datos.gov.co/Econom-a-y-Finanzas/Tasa-de-Cambio-Representativa-del-Mercado-Historic/mcec-87by/about_data

### Características del Dataset

| Característica | Valor |
|---|---|
| **Origen** | Superintendencia Financiera de Colombia |
| **Actualización** | Diaria (última: 15 de abril de 2026) |
| **Período de cobertura** | 1994 - 2026 (más de 30 años) |
| **Total de registros** | ~11,000+ registros diarios |
| **Vistas** | 1,56 millones |
| **Descargas** | 487.000 |
| **Formato** | CSV |
| **Columnas** | VIGENCIADESDE, VIGENCIAHASTA, VALOR, UNIDAD |

### ¿Qué es la TRM?

La **Tasa de Cambio Representativa del Mercado (TRM)** es el promedio ponderado de las operaciones de compra y venta de contado de dólares estadounidenses a cambio de pesos colombianos, calculadas y certificadas diariamente por la Superintendencia Financiera de Colombia.

### Importancia Económica de la TRM

La TRM es fundamental para:
- **Comercio exterior**: Define precios de importaciones y exportaciones
- **Deuda pública**: Afecta el valor de obligaciones en moneda extranjera
- **Decisiones de inversión**: Guía inversiones nacionales e internacionales
- **Política económica**: Refleja la salud fiscal y cambiaria del país
- **Sector empresarial**: Impacta márgenes de ganancia y competitividad

### ¿Por qué este dataset es ideal para Big Data?

1. **Volumen considerable**: Más de 30 años de datos = ~11,000+ registros históricos
2. **Actualización continua**: Se recibe un nuevo registro diariamente (ideal para streaming)
3. **Naturaleza dual**: Permite demostrar tanto procesamiento batch como en tiempo real
4. **Calidad garantizada**: Proviene de fuente oficial confiable (Superintendencia Financiera)
5. **Análisis significativos**: Permite cálculos de tendencias, volatilidad, anomalías y correlaciones
6. **Caso de uso real**: Simula escenarios reales empresariales

---

## Análisis Detallado de los Códigos

### `kafka_producer.py` - Productor de Eventos TRM

#### Propósito General

Este script lee datos históricos de TRM desde un archivo CSV y los envía a Kafka, **simulando un flujo continuo de datos en tiempo real**. Actúa como el origen de datos (source) en la arquitectura productor-consumidor.

#### Funcionalidades Principales

**1. Lectura y Limpieza de Datos (Líneas 30-48)**
```python
# Lee archivo con manejo robusto de codificaciones
try:
    df = pd.read_csv(CSV_FILE, encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(CSV_FILE, encoding='latin-1')

# Normaliza columnas y las renombra para claridad
df.columns = [col.strip().upper() for col in df.columns]
df.rename(columns={
    'VIGENCIADESDE': 'Fecha',
    'VALOR': 'Valor_TRM',
    'UNIDAD': 'Unidad'
}, inplace=True)
```

**Tareas que realiza:**
- Lee CSV con codificación UTF-8 o Latin-1
- Limpia nombres de columnas (espacios, mayúsculas)
- Renombra campos para mejor interpretación
- Ordena datos cronológicamente
- Elimina registros incompletos

**2. Transformación de Valores (Líneas 11-22)**

La función `limpiar_valor()` convierte strings monetarios a números:
```python
def limpiar_valor(valor_str):
    # Elimina caracteres: $, espacios
    s = str(valor_str).replace('$', '').replace(' ', '').strip()
    
    # Maneja formatos: 1.234,56 → 1234.56
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    
    return float(s)
```

**Ejemplos de transformación:**
- `"$ 4.250,75"` → `4250.75`
- `"3.000,00"` → `3000.00`
- `"$ 1.823,50"` → `1823.50`

**3. Cálculo de Indicadores Financieros (Líneas 57-79)**

Para **cada registro**, se calcula:

**a) Variación Porcentual:**
```python
variacion = ((valor_actual - valor_anterior) / valor_anterior) * 100
```

**b) Tendencia (clasificación de cambios):**
```python
if variacion > 1.0:       tendencia = 'DEVALUACION_FUERTE'      # Peso se debilita mucho
elif variacion > 0:       tendencia = 'DEVALUACION_LEVE'        # Peso se debilita poco
elif variacion < -1.0:    tendencia = 'REVALUACION_FUERTE'      # Peso se fortalece mucho
elif variacion < 0:       tendencia = 'REVALUACION_LEVE'        # Peso se fortalece poco
else:                     tendencia = 'ESTABLE'                 # Sin cambio
```

**c) Alerta (identificación de valores extremos):**
```python
if row['Valor_TRM'] >= 4500:  alerta = 'ALERTA_TRM_MUY_ALTA'        # Dólar muy caro
elif row['Valor_TRM'] <= 1000: alerta = 'ALERTA_HISTORICA_BAJA'     # Dólar muy barato
else:                         alerta = 'NORMAL'                    # Valor dentro de rango
```

**4. Envío a Kafka (Líneas 83-88)**

```python
evento = {
    'Fecha': row['Fecha'].strftime('%Y-%m-%d'),
    'Valor_TRM': round(float(row['Valor_TRM']), 2),
    'Unidad': str(row['Unidad']),
    'Variacion_Pct': variacion,
    'Tendencia': tendencia,
    'Alerta': alerta,
    'Timestamp_Envio': time.time()
}

producer.send(KAFKA_TOPIC, value=evento)
time.sleep(SLEEP_TIME_SECONDS)  # 0.5 segundos entre eventos
```

#### Datos que el Productor Analiza

| Campo | Análisis Realizado |
|-------|-------------------|
| **Valor_TRM** | Limpieza de formato, conversión a número |
| **Variación_Pct** | Cálculo diario de cambio porcentual |
| **Tendencia** | Clasificación: devaluación/revaluación |
| **Alerta** | Detección de valores extremos |
| **Timestamp** | Marca temporal para auditoría |

#### Salida del Productor (Consola)

```
============================================================
   RESUMEN TRM - DATOS CORREGIDOS
============================================================
  Primer registro (1994): $800.00
  Último registro (2026): $4.250,75
============================================================
Enviado | 1994-01-03 | TRM: $    800.00 | Var:   +0.00% | ESTABLE
Enviado | 1994-01-04 | TRM: $    810.50 | Var:   +1.31% | DEVALUACION_FUERTE
Enviado | 1994-01-05 | TRM: $    808.75 | Var:   -0.22% | REVALUACION_LEVE
...
Enviado | 2026-04-15 | TRM: $  4.250,75 | Var:   -0.54% | REVALUACION_LEVE
```

---

### `spark_streaming_consumer.py` - Consumidor de Spark Streaming

#### Propósito General

Este script recibe eventos de TRM desde Kafka **en tiempo real**, los procesa mediante **micro-batching** y genera estadísticas agregadas en ventanas de 30 segundos. Actúa como el consumidor (sink) en la arquitectura.

#### Funcionalidades Principales

**1. Configuración de Spark y Conexión a Kafka (Líneas 22-34)**

```python
spark = SparkSession.builder \
    .appName("TRM_Streaming_Consumer") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trm_stream") \
    .option("startingOffsets", "latest") \
    .load()
```

**Configuración:**
- Se conecta al broker Kafka en `localhost:9092`
- Se suscribe al topic `trm_stream`
- Comienza desde los últimos eventos (`latest`)
- Carga datos en streaming (no batch)

**2. Parsing y Validación de Datos (Líneas 12-49)**

Define un esquema JSON strict para validar eventos:
```python
json_schema = StructType([
    StructField("Fecha", StringType(), True),
    StructField("Valor_TRM", DoubleType(), True),
    StructField("Unidad", StringType(), True),
    StructField("Variacion_Pct", DoubleType(), True),
    StructField("Tendencia", StringType(), True),
    StructField("Alerta", StringType(), True),
    StructField("Timestamp_Envio", DoubleType(), True),
])
```

Transforma datos recibidos:
```python
parsed_df = kafka_df \
    .select(col("value").cast("string")) \
    .select(from_json(col("json_data"), json_schema).alias("data")) \
    .select("data.*") \
    .withColumn("Fecha_TRM", to_timestamp(col("Fecha"), "yyyy-MM-dd")) \
    .withColumn("Tiempo_Procesado", current_timestamp())
```

**3. Enriquecimiento de Datos con Clasificación (Líneas 42-48)**

Añade columna que clasifica la TRM por rangos de valor:

```python
.withColumn("Clasificacion_Valor",
    when(col("Valor_TRM") >= 4500, "TRM_MUY_ALTA")
    .when(col("Valor_TRM") >= 4000, "TRM_ALTA")
    .when(col("Valor_TRM") >= 3000, "TRM_MEDIA")
    .when(col("Valor_TRM") >= 2000, "TRM_BAJA")
    .otherwise("TRM_HISTORICA")
)
```

| Rango de TRM | Clasificación |
|---|---|
| ≥ 4.500 | TRM_MUY_ALTA (dólar muy caro) |
| 4.000 - 4.499 | TRM_ALTA |
| 3.000 - 3.999 | TRM_MEDIA |
| 2.000 - 2.999 | TRM_BAJA |
| < 2.000 | TRM_HISTORICA |

**4. Agregación en Ventanas de Tiempo (Líneas 50-62)**

Agrupa datos en **ventanas deslizantes de 30 segundos**:

```python
stats_df = parsed_df.groupBy(
    window(col("Tiempo_Procesado", "30 seconds"),  # Ventana temporal
    col("Tendencia"),                               # Agrupar por tipo de cambio
    col("Clasificacion_Valor")                      # Agrupar por rango
).agg(
    count("Valor_TRM").alias("Total_Registros"),
    round(avg("Valor_TRM"), 2).alias("TRM_Promedio"),
    round(min("Valor_TRM"), 2).alias("TRM_Minima"),
    round(max("Valor_TRM"), 2).alias("TRM_Maxima"),
    round(avg("Variacion_Pct"), 4).alias("Variacion_Promedio_Pct")
)
```

#### Métricas Calculadas por el Consumidor

| Métrica | Descripción | Utilidad |
|---------|-------------|----------|
| **Total_Registros** | Cantidad de eventos TRM en ventana de 30s | Medir volumen de datos |
| **TRM_Promedio** | Promedio de valores TRM | Tendencia central |
| **TRM_Minima** | Valor mínimo en la ventana | Piso de precio |
| **TRM_Maxima** | Valor máximo en la ventana | Techo de precio |
| **Variacion_Promedio_Pct** | Promedio de cambios % | Volatilidad del período |

**5. Salida en Consola (Líneas 64-70)**

```python
query = stats_df \
    .writeStream \
    .outputMode("complete") \  # Muestra todos los registros de la ventana
    .format("console") \       # Salida en consola
    .option("truncate", "false") \  # No trunca columnas
    .option("numRows", 50) \   # Muestra hasta 50 filas
    .start()

query.awaitTermination()  # Espera indefinidamente
```

#### Ejemplo de Salida del Consumidor

```
+------------------------------------------+-----------------------+---------------------+----------------+-----+-----+-----+-----------+
|window                                    |Tendencia              |Clasificacion_Valor  |Total_Registros|Prom |Min  |Max  |Var_Pct    |
+------------------------------------------+-----------------------+---------------------+----------------+-----+-----+-----+-----------+
|2026-04-15 10:30:00 - 2026-04-15 10:30:30|DEVALUACION_LEVE       |TRM_MEDIA            |45              |3250 |3245 |3260 |+0.0234    |
|2026-04-15 10:30:00 - 2026-04-15 10:30:30|ESTABLE                |TRM_ALTA             |28              |4100 |4090 |4110 |+0.0001    |
|2026-04-15 10:30:00 - 2026-04-15 10:30:30|REVALUACION_FUERTE     |TRM_BAJA             |12              |2150 |2140 |2160 |-1.5432    |
+------------------------------------------+-----------------------+---------------------+----------------+-----+-----+-----+-----------+

+------------------------------------------+-----------------------+---------------------+----------------+-----+-----+-----+-----------+
|window                                    |Tendencia              |Clasificacion_Valor  |Total_Registros|Prom |Min  |Max  |Var_Pct    |
+------------------------------------------+-----------------------+---------------------+----------------+-----+-----+-----+-----------+
|2026-04-15 10:31:00 - 2026-04-15 10:31:30|DEVALUACION_FUERTE     |TRM_MUY_ALTA         |55              |4525 |4510 |4540 |+1.2345    |
|2026-04-15 10:31:00 - 2026-04-15 10:31:30|DEVALUACION_LEVE       |TRM_MEDIA            |32              |3280 |3270 |3295 |+0.3210    |
+------------------------------------------+-----------------------+---------------------+----------------+-----+-----+-----+-----------+
```

---

## Flujo Completo de Datos del Sistema

```
┌─────────────────────┐
│ trm_historico.csv   │
│ (1994-2026)         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────┐
│ Kafka Producer (kafka_producer.py)  │
│                                     │
│ ✓ Limpia datos                      │
│ ✓ Calcula variaciones               │
│ ✓ Clasifica tendencias              │
│ ✓ Detecta alertas                   │
│ ✓ Serializa a JSON                  │
│ ✓ Envía a Kafka (0.5s/evento)       │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│ Kafka Topic: trm_stream             │
│ (Cola de mensajes en tiempo real)   │
└──────────┬──────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────┐
│ Spark Streaming Consumer                 │
│ (spark_streaming_consumer.py)            │
│                                          │
│ ✓ Lee eventos de Kafka                   │
│ ✓ Parsea JSON                            │
│ ✓ Valida esquema                         │
│ ✓ Agrupa en ventanas de 30 segundos      │
│ ✓ Calcula estadísticas                   │
│ ✓ Clasifica por rangos                   │
└──────────┬───────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│ Salida en Consola (Real-time)        │
│                                      │
│ Métricas actualizadas cada 30s:      │
│ • Promedio TRM                       │
│ • Mínimo/Máximo                      │
│ • Tendencias                         │
│ • Volatilidad                        │
└──────────────────────────────────────┘
```

---

## Resumen de Análisis Realizado

### Por el Productor
- ✅ Limpieza de formatos monetarios
- ✅ Detección de variaciones diarias
- ✅ Clasificación de tendencias (devaluación/revaluación)
- ✅ Alerta de valores extremos
- ✅ Simulación de flujo en tiempo real

### Por el Consumidor
- ✅ Parsing y validación de eventos JSON
- ✅ Agregación en ventanas de 30 segundos
- ✅ Cálculo de promedios, mínimos y máximos
- ✅ Análisis de variabilidad (volatilidad)
- ✅ Clasificación multidimensional (tendencia + rango)
- ✅ Visualización en tiempo real

---

**Autor**: Javier Alexander Garcia Mariño

**Curso**: Big Data - UNAD

**Dataset**: Tasa de Cambio Representativa del Mercado – Histórico

**Fuente**: Superintendencia Financiera de Colombia