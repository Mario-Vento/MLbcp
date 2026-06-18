from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F

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