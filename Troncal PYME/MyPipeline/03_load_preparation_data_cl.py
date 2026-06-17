# Databricks notebook source
# MAGIC %md
# MAGIC # 03_Load_Preparation_Data
# MAGIC
# MAGIC *Propósito:* Construir el universo de clientes PYME, extraer variables
# MAGIC desde cada tabla fuente, aplicar reemplazo de dummies, capping (clip),
# MAGIC seleccionar las features del modelo y escribir la table_master en Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Config
# MAGIC %run ../config/config

# COMMAND ----------

# DBTITLE 1,Dependencias
# MAGIC %%capture
# MAGIC %pip install catboost optuna xgboost lightgbm openpyxl category_encoders fsspec

# COMMAND ----------

# DBTITLE 1,Imports
import sys
import time
from pyspark.sql import SparkSession

sys.path.insert(0, str(PROJECT_PATH))
from utils.logger import MLOpsLogger
from pipelines.universo_implementacion import UniversoImplementacion

# COMMAND ----------

# DBTITLE 1,Logger
logger = MLOpsLogger(
    name='03_load_preparation_data',
    log_level='DEBUG' if DEBUG_MLOPS else 'INFO',
    log_dir=LOGS_PATH,
    is_job_run=True,
    job_context={
        'mes_vta': MES_VTA,
        'environment': ENV,
        'notebook': '03_load_preparation_data'
    }
)

# COMMAND ----------

# DBTITLE 1,Código Load Preparation Data
try:
    logger.log_stage_start(
        'load_preparation_data',
        mes_vta=MES_VTA,
        environment=ENV,
        table_master=TABLE_MASTER,
        source_table=SOURCE_TABLE,
    )
    start_time = time.time()

    spark = SparkSession.builder.getOrCreate()

    logger.info("=" * 60)
    logger.info("CONSTRUYENDO UNIVERSO E INSUMOS")
    logger.info("=" * 60)
    logger.info(f"Mes proceso (mes_vta)  : {MES_VTA}")
    logger.info(f"Tabla fuente portafolio: {SOURCE_TABLE}")
    logger.info(f"Tabla master destino   : {TABLE_MASTER}")
    logger.info(f"Features del modelo    : {len(FEATURE_NAMES)}")

    # ==========================================================
    # Instanciar y ejecutar la clase
    # ==========================================================
    universo = UniversoImplementacion(
        spark=spark,
        mes_vta=MES_VTA,
        table_master=TABLE_MASTER,
        feature_names=FEATURE_NAMES,
        source_table=SOURCE_TABLE,
    )

    df_master = universo.execute()

    # ==========================================================
    # Validación básica del resultado
    # ==========================================================
    count_master = df_master.count()
    logger.info(f"✅ Table master generada: {count_master:,} registros")

    if count_master == 0:
        raise ValueError(
            f"La table_master quedó vacía para mes_vta={MES_VTA}. "
            "Verificar disponibilidad de tablas fuente."
        )

    # ==========================================================
    # Task values para notebooks posteriores
    # ==========================================================
    duration = time.time() - start_time

    dbutils.jobs.taskValues.set(key="count_master",    value=int(count_master))
    dbutils.jobs.taskValues.set(key="table_master",    value=TABLE_MASTER)
    dbutils.jobs.taskValues.set(key="preparation_ok",  value=True)

    logger.log_data_quality(
        dataset_name='table_master',
        total_records=int(count_master),
        valid_records=int(count_master),
        invalid_records=0,
        errors_found=0,
    )
    logger.log_stage_end(
        'load_preparation_data',
        duration=duration,
        count_master=int(count_master),
    )

    logger.info("=" * 60)
    logger.info(f"✅ PREPARACIÓN COMPLETADA en {duration:.2f}s")
    logger.info("=" * 60)

except Exception as e:
    logger.log_exception(
        operation='load_preparation_data',
        exception=e,
        should_raise=True,
        mes_vta=MES_VTA,
        environment=ENV,
    )
    dbutils.jobs.taskValues.set(key="preparation_ok", value=False)

finally:
    if 'logger' in locals():
        logger.info(f"Finalizando notebook: {logger.name}")
        logger._flush_all_handlers()
        logger.close()

