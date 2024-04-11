# Part 2: Apache Iceberg in CDE

* [Una Breve Introducción a Apache Iceberg](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_02_iceberg.md#una-breve-introducci%C3%B3n-a-apache-iceberg)
* [Lab 1: Trabajar con Iceberg en Sesiones CDE](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_02_iceberg.md#lab-1-trabajar-con-iceberg-en-sesiones-cde)
* [Resumen](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_02_iceberg.md#resumen)
* [Enlaces y Recursos Útiles](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_02_iceberg.md#enlaces-y-recursos-%C3%BAtiles)

### Una Breve Introducción a Apache Iceberg

Apache Iceberg es un formato de tabla abierto de alto rendimiento y nativo para organizar conjuntos de datos analíticos a escala de petabytes en un sistema de archivos o almacenamiento de objetos en la nube. Combinado con Cloudera Data Platform (CDP), los usuarios pueden construir una arquitectura de data lakehouse abierta para analítica multifuncional y desplegar pipelines de extremo a extremo a gran escala.

Iceberg ofrece muchas ventajas. En primer lugar, es un formato de tabla que proporciona numerosas ventajas, incluida la capacidad de estandarizar varios formatos de datos en un sistema de gestión de datos uniforme, un Lakehouse. En el Lakehouse Iceberg, los datos pueden ser consultados con diferentes motores de cómputo, incluidos Apache Impala, Apache Flink, Apache Spark y otros.

Además, Iceberg simplifica análisis de datos y pipelines de ingeniería de datos con características como particionado y evolución de esquemas, viaje en el tiempo (time travel) y captura de datos de cambios (Change Data Capture). En CDE, puedes usar Spark para consultar tablas Iceberg de forma interactiva a través de Sesiones o en pipelines por lotes a través de Jobs.

### Lab 1: Trabajar con Iceberg en Sesiones CDE

En la Parte 1 utilizamos Sesiones CDE para explorar dos conjuntos de datos y prototipar código para una aplicación Spark de detección de fraudes. En este breve laboratorio cargaremos un nuevo lote de transacciones desde Cloud Storage y aprovecharemos las capacidades del Lakehouse Iceberg para probar el SQL Merge Into con Spark.

##### Migrar una Tabla de Spark a una Tabla Iceberg

Inicia una nueva Sesión CDE o reutiliza la Sesión que creaste en la Parte 1 si aún está en ejecución.

```
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

storageLocation = "s3a://bacpoccdp/data"
username = "user005"
```

Por default, una tabla Spark creada en Cloudera Data Engineering (CDE) se registra en el Hive Metastore como una tabla externa. Los datos se almacenan en Cloud Storage en uno de varios formatos (parquet, csv, avro, etc.) y su ubicación es rastreada por el Hive Metastore (HMS).

Cuando se adopta Iceberg por primera vez, las tablas pueden ser copiadas o migradas al formato Iceberg. Una copia implica la recomputación de todo el conjunto de datos en una tabla Iceberg, mientras que una migración solo implica la creación de metadatos de Iceberg en la capa de Metadatos de Iceberg.

```
## Migrate First Batch Table to Iceberg Table Format
spark.sql("ALTER TABLE {}.FIRST_BATCH_TABLE UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
spark.sql("CALL spark_catalog.system.migrate('{}.FIRST_BATCH_TABLE')".format(username))
```

El Metadata Layer de Iceberg proporciona tres capas de archivos de metadatos cuyo propósito es rastrear las tablas Iceberg a medida que son modificadas. La Metadata Layer consta de Archivos de Metadatos (Metadata Files), Listas de Manifiestos (Manifest Lists) y Archivos de Manifiestos (Manifest Files).

El Metadata Layer en su conjunto almacena una versión de cada conjunto de datos proporcionando referencias a diferentes versiones de los datos. Entre otras ventajas, esto habilita capacidades de "Time Travel" en las tablas Iceberg, es decir, la capacidad de consultar datos como de una instantánea o marca de tiempo específica.

En el siguiente ejemplo, cargarás un nuevo lote de transacciones desde Cloud Storage. Luego lo insertarás en la tabla de transacciones históricas a través de la declaración SQL Merge Into, la cual no está disponible en Spark SQL a menos que se use el formato de tabla Iceberg. Finalmente, consultaremos las tablas de metadatos de Iceberg y consultaremos los datos en su versión PRE-INSERT con un comando simple de Spark SQL.

Al igual que en la Parte 1, simplemente copia y pega los siguientes fragmentos de código en celdas del cuaderno de Sesiones de CDE.

```
# PRE-INSERT TIMESTAMP
from datetime import datetime
now = datetime.now()
timestamp = datetime.timestamp(now)

print("PRE-INSERT TIMESTAMP: ", timestamp)
```

```
# PRE-INSERT COUNT
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}.FIRST_BATCH_TABLE".format(username)).show()
```
```
# LOAD SECOND BATCH
secondBatchDf = spark.read.json("{0}/logistics/secondbatch/{1}/iotfleet".format(storageLocation, username))
secondBatchDf = secondBatchDf.withColumn("event_ts", secondBatchDf["event_ts"].cast('timestamp'))
secondBatchDf.printSchema()
secondBatchDf.createOrReplaceTempView("SECOND_BATCH_TEMP_VIEW".format(username))
```

```
# APPEND SECOND BATCH
secondBatchDf.writeTo("spark_catalog.{}.FIRST_BATCH_TABLE".format(username)).append()
```

```
# POST-INSERT COUNT
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}.FIRST_BATCH_TABLE".format(username)).show()
```

```
# QUERY ICEBERG METADATA HISTORY TABLE
spark.sql("SELECT * FROM spark_catalog.{}.FIRST_BATCH_TABLE.history".format(username)).show(20, False)
```

```
# QUERY ICEBERG METADATA SNAPSHOTS TABLE
spark.sql("SELECT * FROM spark_catalog.{}.FIRST_BATCH_TABLE.snapshots".format(username)).show(20, False)
```

```
# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}.FIRST_BATCH_TABLE".format(username))

# POST TIME TRAVEL COUNT
print(df.count())
```

### Resumen

El Open Data Lakehouse en CDP simplifica la analítica avanzada sobre todos los datos con una plataforma unificada para datos estructurados y no estructurados, y servicios de datos integrados para habilitar cualquier caso de uso de analítica, desde ML y BI hasta analítica de streaming y en tiempo real. Apache Iceberg es el componente clave del lakehouse abierto.

Apache Iceberg es un formato de tabla abierto diseñado para cargas de trabajo analíticas a gran escala. Admite evolución de esquemas, particionamiento oculto, evolución del diseño de particiones y "time travel" (viaje en el tiempo). Cada cambio en la tabla crea una instantánea Iceberg, lo que ayuda a resolver problemas de concurrencia y permite a los lectores escanear un estado estable de la tabla en cada momento.

Iceberg se adapta bien a una variedad de casos de uso, incluyendo analítica de lakehouse, pipelines de ingeniería de datos y cumplimiento normativo con aspectos específicos de regulaciones como GDPR (Reglamento General de Protección de Datos) y CCPA (Ley de Privacidad del Consumidor de California) que requieren la capacidad de eliminar datos de clientes bajo solicitud.

Los Clústeres Virtuales de CDE brindan soporte nativo para Iceberg. Los usuarios pueden ejecutar cargas de trabajo de Spark e interactuar con sus tablas Iceberg a través de declaraciones SQL. La Capa de Metadatos de Iceberg rastrea las versiones de las tablas Iceberg a través de Instantáneas (Snapshots) y proporciona Tablas de Metadatos con información de instantáneas y otros datos útiles. En este laboratorio, utilizamos Iceberg para acceder al conjunto de datos de transacciones con tarjeta de crédito en una marca de tiempo específica.

### Enlaces y Recursos Útiles

Si tienes curiosidad por aprender más sobre las características mencionadas anteriormente en el contexto de casos de uso más avanzados, visita las siguientes referencias:

1. **Documentación de Cloudera Data Platform (CDP)**:
   - Explora la documentación oficial de Cloudera Data Platform para obtener detalles completos sobre las funcionalidades de lakehouse y Iceberg: [Documentación de Cloudera Data Platform](https://docs.cloudera.com/cdp/latest/index.html)

2. **Documentación de Apache Iceberg**:
   - Sumérgete en la documentación oficial de Apache Iceberg para profundizar en sus capacidades y características avanzadas: [Documentación de Apache Iceberg](https://iceberg.apache.org/documentation/)

3. **Blogs y Artículos de Cloudera**:
   - Encuentra artículos interesantes y blogs sobre Iceberg y tecnologías relacionadas en el sitio web de Cloudera: [Blogs de Cloudera](https://blog.cloudera.com/)

4. **Foros y Comunidad de Cloudera**:
   - Únete a la comunidad de Cloudera para hacer preguntas, compartir conocimientos y aprender de otros profesionales en el campo de la analítica de datos: [Comunidad de Cloudera](https://community.cloudera.com/)

Estos recursos te ayudarán a profundizar en el uso de Iceberg, las capacidades del lakehouse abierto en CDP y cómo aprovechar al máximo estas tecnologías en casos de uso avanzados.
