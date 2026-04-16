# Procesamiento de Datos con Apache Spark y Kafka

Este repositorio contiene la implementación de soluciones de Big Data para el procesamiento de grandes volúmenes de datos en modalidades de lote (batch) y tiempo real (streaming). El proyecto integra herramientas del ecosistema Hadoop para construir una infraestructura capaz de soportar análisis de datos eficientes.

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

- **Productor (Kafka)**: Simulación de flujo continuo de datos de sensores (temperatura y humedad).
- **Consumidor (Spark Streaming)**: Procesamiento de datos mediante ventanas de tiempo de 1 minuto para cálculos agregados.
- **Monitoreo**: Uso de la interfaz web de Spark (puerto 4040) para el seguimiento de tareas y etapas del procesamiento.

## Instrucciones de Ejecución

Para el correcto funcionamiento del sistema de streaming, se deben seguir estos pasos dentro de la máquina virtual configurada:

1. **Inicio de servicios**: Ejecutar ZooKeeper y el servidor de Kafka.
2. **Configuración del Topic**: Crear el canal de comunicación necesario para los mensajes.
3. **Ejecución del Productor**:
   ```bash
   python3 kafka_producer.py
   ```
4. **Ejecución del Consumidor de Spark**:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
   ```

---

**Autor**: Javier Alexander Garcia Mariño

**Curso**: Big Data - UNAD