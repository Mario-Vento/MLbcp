from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import *

def write_to_unity_catalog(
    df,
    table_name: str,
    partition_col: str = "codmes",
    mode: str = "append",
    overwrite_partition: bool = True,
):
    """
    Escribe un DataFrame en Unity Catalog.
    - Si la tabla no existe, la crea.
    - Si existe, hace append o overwrite según el mode.
    - Si overwrite_partition=True y mode='overwrite', solo sobreescribe las particiones presentes en el df.
    """
    if overwrite_partition and mode == "overwrite":
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .option("mergeSchema", "true")
            .partitionBy(partition_col)
            .saveAsTable(table_name)
        )
    else:
        (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .partitionBy(partition_col)
            .saveAsTable(table_name)
        )
    print(f"✅ Tabla escrita: {table_name} (mode={mode})")

def print_spark(df, n=20):
    """Reemplazo de printSpark de Utils"""
    df.show(n, truncate=False)

def join_variable_table(
    df_base,
    path_src: str,
    columns: list,
    spark, # Necesario recibir spark session como argumento si ejecutamos desde módulo py
    join_key: str = "codclavepartycli",
    month_offset: int = 0,
    df_name: str = "",
    persist: bool = True,
):
    """
    Lee una tabla fuente, aplica desfase de mes, hace left join con df_base
    y opcionalmente persiste el resultado.
    """
    df_src = (
        spark.table(path_src)
        .select("codmes", join_key, *columns)
        .withColumn("codmes_fecha", F.to_date(F.concat(F.col("codmes").cast("string"), F.lit("01")), "yyyyMMdd"))
        .withColumn("codmesJoin", F.date_format(F.add_months("codmes_fecha", month_offset), "yyyyMM").cast("int"))
        .drop("codmes_fecha")
        .drop("codmes")
    )

    l = (
        df_base.select("codmes", "codclavepartycli", "codclaveunicocli")
        .withColumn("codmesJoin", F.col("codmes"))
        .alias("l")
    )

    r = df_src.alias("r")

    result = (
        l.join(
            r,
            (F.col(f"l.{join_key}") == F.col(f"r.{join_key}")) &
            (F.col("l.codmesJoin") == F.col("r.codmesJoin")),
            "left",
        )
        .drop(l.codmesJoin, r.codmesJoin)
        .drop(F.col(f"r.{join_key}"))
    )

    if persist:
        result = result.persist(StorageLevel.MEMORY_AND_DISK)
        cnt = result.count()
        print(f"  ✅ {df_name}: {cnt:,} registros")
        if df_name:
            result.createOrReplaceTempView(df_name)

    return result

# =================================================================================
# Helpers portados desde el Utils de Notebook de Modelador Arnold + apply_unified.
# =================================================================================

def replace_sentinels_with_null(spark, df, sentinels=None):
    """
    Reemplaza valores centinela por null en todas las columnas numéricas.
    IDÉNTICA a Arnold_UTILS.
    """
    if sentinels is None:
        sentinels = (
            1111111111, -1111111111,
            2222222222, -2222222222,
            3333333333, -3333333333,
            4444444444, 44444444444,
            5555555555,
            6666666666,
            7777777777,
            99999,
        )

    numeric_types = (
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DecimalType
    )

    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, numeric_types)]
    if not numeric_cols:
        return df

    return df.na.replace(list(sentinels), None, subset=numeric_cols)


def decimals_to_double(spark, df):
    """Convierte columnas DecimalType a double preservando nombres y orden.
    IDÉNTICA a Arnold_UTILS."""
    decimal_cols = {f.name for f in df.schema.fields if isinstance(f.dataType, DecimalType)}
    return df.select([
        F.col(f.name).cast(DoubleType()).alias(f.name) if f.name in decimal_cols else F.col(f.name)
        for f in df.schema.fields
    ])


def rename_columns_safe(df, mapping: dict):
    """Rename por diccionario {viejo: nuevo}. Equivale al rename_columns_safe
    que usa ArnoldNotebook celda 30. Ignora columnas ausentes e identidades."""
    for old, new in mapping.items():
        if old in df.columns and old != new:
            df = df.withColumnRenamed(old, new)
    return df


def apply_caps_xgb_cap24(df):
    """Caps del modelo model_xgboost_cap_24_mono_200 (= apply_unified,
    ArnoldNotebook). Debe correr DESPUÉS de rename_columns_safe(dict_names_sas)."""
    df = df.withColumn('ctdmora_intra_0_o', F.when(F.col('ctdmora_intra_0').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('ctdmora_intra_0'), F.lit(1)), F.lit(30))).cast('double'))

    df = df.withColumn('max_mora_intra_u6m_o', F.when(F.col('max_mora_intra_u6m').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('max_mora_intra_u6m'), F.lit(1)), F.lit(32))).cast('double'))

    df = df.withColumn('prd_pct_pmpas_pmact_2_000_o', F.when(F.col('prd_pct_pmpas_pmact_2_000').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('prd_pct_pmpas_pmact_2_000'), F.lit(0.00692478)), F.lit(117.23953515))).cast('double'))

    df = df.withColumn('fatc_pct_pag_mn_ctami_000_o', F.when(F.col('fatc_pct_pag_mn_ctami_000').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('fatc_pct_pag_mn_ctami_000'), F.lit(0)), F.lit(26.23016959))).cast('double'))

    df = df.withColumn('rcc_pct_rdv_prm_u3m_ooo', F.when(F.col('rcc_pct_rdv_prm_u3m').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('rcc_pct_rdv_prm_u3m'), F.lit(0.00011443993572)), F.lit(0.04283144838078))).cast('double'))

    df = df.withColumn('prd_prm_tsav_mnn_6_6_rt6_ooo', F.when(F.col('prd_prm_tsav_mnn_6_6_rt6').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('prd_prm_tsav_mnn_6_6_rt6'), F.lit(0.06626325)), F.lit(0.77784588))).cast('double'))

    df = df.withColumn('q_diamora_max_100_u24_o', F.when(F.col('q_diamora_max_100_u24').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('q_diamora_max_100_u24'), F.lit(0)), F.lit(17))).cast('double'))

    df = df.withColumn('edad_o', F.when(F.col('edad').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('edad'), F.lit(22)), F.lit(69))).cast('double'))

    df = df.withColumn('ctdpdhu24_ooo', F.when(F.col('ctdpdhu24').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('ctdpdhu24'), F.lit(3)), F.lit(24))).cast('double'))

    df = df.withColumn('isav_q_opea_desm_prm_u3m_o', F.when(F.col('isav_q_opea_desm_prm_u3m').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('isav_q_opea_desm_prm_u3m'), F.lit(0)), F.lit(0.33333333333333))).cast('double'))

    df = df.withColumn('mto_deu_mora_sol_u48_o', F.when(F.col('mto_deu_mora_sol_u48').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('mto_deu_mora_sol_u48'), F.lit(0)), F.lit(51084.120726))).cast('double'))

    df = df.withColumn('rcc_mto_deu_ind_pj_pr_000_o', F.when(F.col('rcc_mto_deu_ind_pj_pr_000').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('rcc_mto_deu_ind_pj_pr_000'), F.lit(0)), F.lit(180195.45))).cast('double'))

    df = df.withColumn('pos_tkt_trx_com_sol_p_000_ooo', F.when(F.col('pos_tkt_trx_com_sol_p_000').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('pos_tkt_trx_com_sol_p_000'), F.lit(28.6940555566666)), F.lit(262.574863266666))).cast('double'))

    df = df.withColumn('rcc_mto_gar_ope_cre_o', F.when(F.col('rcc_mto_gar_ope_cre').isNull(), F.lit(None)).otherwise(F.least(F.greatest(F.col('rcc_mto_gar_ope_cre'), F.lit(0)), F.lit(1760351.33))).cast('double'))

    return df