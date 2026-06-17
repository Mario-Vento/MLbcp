from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, trim
# from utils.dataprep import write_to_unity_catalog
from utils.data_preparation.utils_dataprep import write_to_unity_catalog

class FuenteHmAtrasoIntra:
    """
    Clase para la preparación de datos de la fuente HM Atraso Intra.
    Basada en el notebook 01_fuente_hm_atraso_intra.ipynb.
    """

    def __init__(
        self,
        spark: SparkSession,
        codmes_ini: int,
        codmes_fin: int,
        src_catalog: str,
        sink_catalog: str,
        sink_schema: str,
        sink_table_hm_atraso_cta: str,
        sink_table_hm_atraso: str
    ):
        self.spark = spark
        self.codmes_ini = int(codmes_ini)
        self.codmes_fin = int(codmes_fin)

        # Paths derivados
        self.path_table_hm_atraso_cta = f"{sink_catalog}.{sink_schema}.{sink_table_hm_atraso_cta}"
        self.path_table_hm_atraso = f"{sink_catalog}.{sink_schema}.{sink_table_hm_atraso}"

        # Tablas fuente (todas udv)
        self.path_h_cuentafinanciera = f"{src_catalog}.bcp_udv_int_vu.h_cuentafinanciera"
        self.path_h_saldocuentatarjetacredito = f"{src_catalog}.bcp_udv_int_vu.h_saldocuentatarjetacredito"
        self.path_h_saldocuentacreditopersonal = f"{src_catalog}.bcp_udv_int_vu.h_saldocuentacreditopersonal"
        self.path_h_tipocambio = f"{src_catalog}.bcp_udv_int_vu.h_tipocambio"

    def execute(self):
        print(f"Rango meses      : {self.codmes_ini} -> {self.codmes_fin}")
        print(f"Tabla intermedia cta : {self.path_table_hm_atraso_cta}")
        print(f"Tabla final cliente  : {self.path_table_hm_atraso}")

        # Base: h_cuentafinanciera (VP y ALS)
        df_h_cuentafinanciera = (
            self.spark.table(self.path_h_cuentafinanciera)
            .withColumn("CODMES", F.date_format(F.col("FECDIA"), "yyyyMM").cast("int"))
            .filter(
                (F.col("CODMES") >= F.lit(self.codmes_ini)) &
                (F.col("CODMES") <= F.lit(self.codmes_fin))
            )
            .select("CODCLAVECTA", "CODCLAVEPARTYCLI", "CODAPP")
            .dropDuplicates(["CODCLAVECTA", "CODCLAVEPARTYCLI", "CODAPP"])
        )

        # Cuentas VP
        df_ctas_vp = (
            df_h_cuentafinanciera
            .filter(F.trim(F.col("CODAPP")) == F.lit("VPLU"))
            .select("CODCLAVECTA", "CODCLAVEPARTYCLI")
        )

        # Saldos diarios VP
        df_h_saldo_tc = (
            self.spark.table(self.path_h_saldocuentatarjetacredito)
            .select(
                "FECSALDO", "CODCLAVECTA", "CODMONEDA",
                "CTDDIAMOROSO",
                F.col("MTOBALANCEACTUAL").alias("MTODEUDATOTAL"),
            )
            .withColumn("CODMES", F.date_format(F.col("FECSALDO"), "yyyyMM").cast("int"))
        )

        # Filtrar por cuentas VP, mora > 0 y rango de meses
        df_saldo_vp = (
            df_h_saldo_tc
            .join(df_ctas_vp.select("CODCLAVECTA").distinct(), on="CODCLAVECTA", how="inner")
            .filter(F.col("CTDDIAMOROSO") > 0)
            .filter(
                (F.col("CODMES") >= F.lit(self.codmes_ini)) &
                (F.col("CODMES") <= F.lit(self.codmes_fin))
            )
        )

        # Resultado VP con codclavepartycli
        df_matriz_infodiariavp = (
            df_saldo_vp.alias("A")
            .join(df_ctas_vp.alias("B"), on="CODCLAVECTA", how="inner")
            .select(
                F.col("A.FECSALDO"),
                F.col("A.CODCLAVECTA"),
                F.col("B.CODCLAVEPARTYCLI"),
                F.col("A.CODMONEDA"),
                F.col("A.CTDDIAMOROSO"),
                F.col("A.MTODEUDATOTAL"),
                F.col("A.CODMES"),
            )
        )

        # Informacion diaria ALS (crédito personal)
        # Cuentas ALS
        df_ctas_als = (
            df_h_cuentafinanciera
            .filter(F.trim(F.col("CODAPP")) == F.lit("ALS"))
            .select("CODCLAVECTA", "CODCLAVEPARTYCLI")
        )

        # Saldos diarios ALS
        df_h_saldo_cp = (
            self.spark.table(self.path_h_saldocuentacreditopersonal)
            .select(
                "FECSALDO", "CODCLAVECTA", "CODMONEDA",
                F.col("CTDDIAVCDA").alias("CTDDIAMOROSO"),
                F.col("MTOTOTALDEUDA").alias("MTODEUDATOTAL"),
            )
            .withColumn("CODMES", F.date_format(F.col("FECSALDO"), "yyyyMM").cast("int"))
        )

        # Filtrar por cuentas ALS, mora > 0 y rango de meses
        df_saldo_als = (
            df_h_saldo_cp
            .join(df_ctas_als.select("CODCLAVECTA").distinct(), on="CODCLAVECTA", how="inner")
            .filter(F.col("CTDDIAMOROSO") > 0)
            .filter(
                (F.col("CODMES") >= F.lit(self.codmes_ini)) &
                (F.col("CODMES") <= F.lit(self.codmes_fin))
            )
        )

        # Resultado ALS con codclavepartycli
        df_matriz_infodiariaals = (
            df_saldo_als.alias("A")
            .join(df_ctas_als.alias("B"), on="CODCLAVECTA", how="inner")
            .select(
                F.col("A.FECSALDO"),
                F.col("A.CODCLAVECTA"),
                F.col("B.CODCLAVEPARTYCLI"),
                F.col("A.CODMONEDA"),
                F.col("A.CTDDIAMOROSO"),
                F.col("A.MTODEUDATOTAL"),
                F.col("A.CODMES"),
            )
        )

        # Unión VP + ALS y conversión a soles
        cols_union = ["codmes", "fecsaldo", "codclavecta", "codclavepartycli", "codmoneda", "ctddiamoroso",
                      "mtodeudatotal"]

        df_tabla_morosidadsaldos = (
            df_matriz_infodiariavp.select(*cols_union)
            .unionByName(df_matriz_infodiariaals.select(*cols_union), allowMissingColumns=False)
        )

        # Tipo de cambio (GLM)
        df_tipocambio = (
            self.spark.table(self.path_h_tipocambio)
            .filter(F.trim(F.col("CODAPP")) == F.lit("GLM"))
            .select(
                F.col("codmonedaorigen"),
                F.col("fectipcambio"),
                F.col("mtocambiomonedaorigenmonedadestino"),
            )
        )

        # Conversión a soles
        df_tabla_morosidadsldtipocmb = (
            df_tabla_morosidadsaldos.alias("A")
            .join(
                df_tipocambio.alias("B"),
                (F.col("A.codmoneda") == F.col("B.codmonedaorigen")) &
                (F.col("A.fecsaldo") == F.col("B.fectipcambio")),
                how="left",
            )
            .select(
                F.col("A.codmes"),
                F.col("A.fecsaldo"),
                F.col("A.codclavecta"),
                F.col("A.codclavepartycli"),
                F.col("A.ctddiamoroso"),
                F.when(F.col("A.codmoneda") == F.lit("1001"),
                       F.col("A.mtodeudatotal") * F.col("B.mtocambiomonedaorigenmonedadestino"))
                .when(F.col("A.codmoneda") == F.lit("0001"),
                      F.col("A.mtodeudatotal"))
                .otherwise(F.lit(None).cast("double"))
                .alias("mtodeudatotalsol"),
            )
        )

        # Escritura tabla intermedia (nivel cuenta-día)
        write_to_unity_catalog(
            df=df_tabla_morosidadsldtipocmb,
            table_name=self.path_table_hm_atraso_cta,
            partition_col="codmes",
            mode="overwrite",
            overwrite_partition=True,
        )

        # Agregación final: mora intramés a nivel cliente-mes
        df_tabla_morosidadsldtipocmb_read = self.spark.read.table(self.path_table_hm_atraso_cta)

        df_matriz_cpto_moraintrames = (
            df_tabla_morosidadsldtipocmb_read
            .filter(F.col("mtodeudatotalsol") > F.lit(0))
            .groupBy("codmes", "codclavepartycli")
            .agg(
                F.max(F.col("ctddiamoroso")).alias("ctdmoraintr"),
                # F.max(F.when(F.col("mtodeudatotalsol") > F.lit(30), F.col("ctddiamoroso"))).alias("ctdmoraint_30"), # Ya no va
                # F.max(F.when(F.col("mtodeudatotalsol") > F.lit(100), F.col("ctddiamoroso"))).alias("ctdmoraint_100"), # Ya no va
            )
        )

        # Escritura tabla final (nivel cliente-mes)
        write_to_unity_catalog(
            df=df_matriz_cpto_moraintrames,
            table_name=self.path_table_hm_atraso,
            partition_col="codmes",
            mode="overwrite",
            overwrite_partition=True,
        )

        # Validación
        print("Conteo por codmes - tabla final:")
        self.spark.read.table(self.path_table_hm_atraso).groupBy("codmes").count().orderBy("codmes").show(50, truncate=False)