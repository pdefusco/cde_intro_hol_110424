# Part 3: Apache Airflow in CDE

* [Una Breve Introducción a Airflow](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_03_airflow.md#una-breve-introducci%C3%B3n-a-airflow)
* [Lab 1: Orquestar Pipeline de Spark con Airflow](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_03_airflow.md#lab-1-orquestar-pipeline-de-spark-con-airflow)
* [Resumen](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_03_airflow.md#resumen)
* [Useful Links and Resources](https://github.com/pdefusco/cde_intro_hol_110424/blob/main/step_by_step_guides/espanol/part_03_airflow.md#enlaces-y-recursos-%C3%BAtiles)

### Una Breve Introducción a Airflow

Apache Airflow es una plataforma para crear, programar y ejecutar pipelines de ingeniería de datos. Es ampliamente utilizado por la comunidad para crear flujos de trabajo dinámicos y robustos para casos de uso de ingeniería de datos por lotes.

La principal característica de los flujos de trabajo de Airflow es que todos están definidos en código Python. El código Python que define el flujo de trabajo se almacena como una colección de tareas de Airflow organizadas en un DAG (grafo acíclico dirigido). Las tareas están definidas por operadores integrados y módulos de Airflow. Los operadores son clases Python que pueden ser instanciadas para realizar acciones predefinidas y parametrizadas.

CDE incorpora Apache Airflow a nivel de Clúster Virtual de CDE. Se despliega automáticamente para el usuario de CDE durante la creación del Clúster Virtual de CDE y no requiere mantenimiento por parte del administrador de CDE. Además de los operadores principales, CDE admite el CDEJobRunOperator y el CDWOperator para ejecutar trabajos de Spark y consultas de Data Warehousing.

### Lab 1: Orquestar Pipeline de Spark con Airflow

En este laboratorio construirás un pipeline de trabajos de Spark para cargar un nuevo lote de transacciones, unirlo con datos PII de clientes y crear un informe de clientes que probablemente sean víctimas de fraude con tarjetas de crédito.

A alto nivel, el flujo de trabajo será similar a las Partes 1 y 2 donde creaste dos tablas y cargaste un nuevo lote de transacciones. Sin embargo, hay dos diferencias:

1. El flujo de trabajo aprovechará todas las características utilizadas hasta este punto, pero en conjunto. Por ejemplo, se utilizará Iceberg Time Travel para crear un informe incremental que incluya solo las actualizaciones dentro del último lote en lugar de todo el conjunto de datos históricos.
2. Todo el flujo de trabajo será orquestado por Airflow. Esto te permitirá ejecutar tus trabajos en paralelo mientras implementas una lógica robusta de manejo de errores.

##### Creación Jobs de Spark

En esta sección crearás cuatro trabajos de Spark de CDE a través de la interfaz de usuario de CDE Jobs. Es importante que ***no ejecutes los jobs de Spark cuando los crees***. Si los ejecutas por error, por favor levanta la mano durante el taller y pide ayuda para implementar una solución alternativa.

1. **Validación de Datos**:
  - Nombre: nómbralo según tu usuario, por ejemplo, si eres usuario "user010" llámalo "02_data_val_user010".
  - Archivo de Aplicación: "02_data_validation.py" ubicado en tu recurso de Archivos de CDE.
  - Argumentos: introduce tu nombre de usuario aquí, sin comillas (solo texto), por ejemplo, si eres usuario "user010" introduce "user010" sin comillas.
  - Ambiente de Python: elige tu recurso de Python de CDE en el menú desplegable.
  - Archivos y Recursos: elige tu recurso de Archivos de CDE en el menú desplegable (esto debería haber sido preseleccionado para ti).
  - Deja todos los demás ajustes en sus valores predeterminados y crea el trabajo.

2. **Carga de Datos de la Empresa**:
  - Nombre: nómbralo según tu usuario, por ejemplo, si eres usuario "user010" llámalo "03_co_data_user010".
  - Archivo de Aplicación: "03_co_data.py" ubicado en tu recurso de Archivos de CDE.
  - Argumentos: introduce tu nombre de usuario aquí, sin comillas (solo texto), por ejemplo, si eres usuario "user010" introduce "user010" sin comillas.
  - Entorno de Python: elige tu recurso de Python de CDE en el menú desplegable.
  - Archivos y Recursos: elige tu recurso de Archivos de CDE en el menú desplegable (esto debería haber sido preseleccionado para ti).
  - Deja todos los demás ajustes en sus valores predeterminados y crea el trabajo.

3. **Fusionar Lotes**:
  - Nombre: nómbralo según tu usuario, por ejemplo, si eres usuario "user010" llámalo "04_merge_batch_user010".
  - Archivo de Aplicación: "04_merge_batches.py" ubicado en tu CDE files resource.

4. **Reporte Incremental**:
  - Nombre: nombra esto según tu usuario, por ejemplo, si eres usuario "user010" llámalo "05_inc_report_user010".
  - Archivo de Aplicación: "05_incremental_report.py" ubicado en tu recurso de Archivos de CDE.
  - Argumentos: introduce tu nombre de usuario aquí, sin comillas (solo texto), por ejemplo, si eres usuario "user010" introduce "user010" sin comillas.
  - Archivos y Recursos: elige tu recurso de Archivos de CDE en el menú desplegable (esto debería haber sido preseleccionado para ti).
  - Deja todos los demás ajustes en sus valores predeterminados y crea el trabajo.

##### Creación Jobs de Airflow

Para crear un Job de Airflow que orqueste los Jobs de Spark que has configurado, sigue estos pasos:

1. **Abrir el script "airflow_dag.py"**:
  - Navega hasta la carpeta "cde_airflow_jobs" en tu proyecto local.
  - Abre el archivo "airflow_dag.py" y familiarízate con el código. Observa las clases Python importadas necesarias para los operadores DAG. Asegúrate de que el operador `CDEJobRunOperator` esté incluido para ejecutar los jobs de Spark en CDE.
  - Examina el diccionario `default_args`, que incluye opciones para programación, establecimiento de dependencias y ejecución general.

2. **Definir los operadores CDEJobRunOperator**:
  - En el archivo "airflow_dag.py", encontrarás instancias del operador `CDEJobRunOperator`. Cada instancia representa un job de Spark que quieres ejecutar como parte del flujo de Airflow.
  - Asegúrate de ajustar los parámetros de cada operador, incluyendo el `Task ID` (identificador de tarea), el `DAG` (nombre del objeto DAG), y el `Job Name` (nombre del job de Spark CDE creado anteriormente en el paso 1).

3. **Editar la variable de usuario**:
  - En la línea 49 del archivo "airflow_dag.py", edita la variable `username` para que coincida con tu nombre de usuario de CDE.

4. **Crear un nuevo Job de CDE**:
  - Navega hasta la interfaz de usuario de CDE Jobs.
  - Selecciona "Airflow" como el tipo de job.
  - Asigna un nombre único al job de CDE basado en tu usuario, por ejemplo, "my_airflow_job_user010".
  - Ejecuta el job.

5. **Monitorizar la ejecución en Airflow**:
  - Una vez que el job de Airflow se haya iniciado, abre la interfaz de usuario de Airflow para monitorizar la ejecución del DAG y los trabajos de Spark asociados.

Al seguir estos pasos, podrás configurar y ejecutar un flujo de trabajo completo utilizando Airflow para orquestar tus jobs de Spark en Cloudera Data Engineering. Asegúrate de ajustar cualquier parámetro adicional según sea necesario para tu caso específico de uso.


### Resumen

Cada clúster virtual de CDE incluye una instancia integrada de Apache Airflow. Con los flujos de trabajo basados en Airflow, los usuarios pueden especificar su pipeline de Spark utilizando un archivo de configuración Python simple llamado DAG de Airflow.

Un DAG básico de Airflow en CDE puede estar compuesto por una combinación de operadores de Hive y Spark que ejecutan automáticamente trabajos en el Almacén de Datos de CDP (CDW) y CDE, respectivamente, con la seguridad y gobernanza subyacentes proporcionadas por SDX (Cloudera Shared Data Experience).

Esta integración permite a los usuarios orquestar fácilmente pipelines de datos complejos, combinando tareas de Spark y Hive en un flujo de trabajo unificado y administrado, aprovechando la potencia de Cloudera Data Engineering junto con las capacidades de gestión y seguridad de Cloudera Shared Data Experience (SDX).

### Enlaces y Recursos Útiles

Si estás interesado en aprender más sobre Airflow y su integración con Cloudera Data Engineering (CDE), así como en explorar casos de uso avanzados, te recomiendo revisar los siguientes enlaces y recursos:

   1. **Documentación de Apache Airflow**:
      - Explora la documentación oficial de Apache Airflow para comprender mejor su funcionamiento y cómo crear flujos de trabajo avanzados: [Documentación de Apache Airflow](https://airflow.apache.org/docs/)

   2. **Documentación de Cloudera Data Engineering (CDE)**:
      - Accede a la documentación oficial de Cloudera Data Engineering para aprender más sobre la integración de Airflow con CDE y cómo aprovechar al máximo las capacidades de CDE para ejecutar flujos de trabajo de Spark: [Documentación de Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/cloud/index.html)

   3. **Foros y Comunidad de Apache Airflow**:
      - Únete a la comunidad de Apache Airflow para hacer preguntas, compartir conocimientos y aprender de otros usuarios: [Comunidad de Apache Airflow](https://community.apache.org/)

   4. **Foros y Comunidad de Cloudera**:
      - Participa en la comunidad de Cloudera para discutir sobre Airflow, CDE y otras tecnologías relacionadas con expertos y profesionales del campo: [Comunidad de Cloudera](https://community.cloudera.com/)

   5. **Blogs y Artículos de Cloudera**:
      - Encuentra artículos interesantes sobre integración de Airflow con CDE y casos de uso avanzados en el blog oficial de Cloudera: [Blogs de Cloudera](https://blog.cloudera.com/)

Estos recursos te ayudarán a profundizar en el uso de Airflow en el contexto de Cloudera Data Engineering, así como a explorar casos de uso avanzados y las mejores prácticas para la orquestación de flujos de trabajo de Spark.
