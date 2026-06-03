# universo_implementacion.py
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.storagelevel import StorageLevel

# Importar funciones helper locales (add_codmes_spark, operacionesMaxBetweenCols_udf)
from helpers import add_codmes_spark

class UniversoImplementacion:
    """
    Clase para la construcción del universo de clientes y variables
    utilizadas en el modelo PD BHV Troncal (Sherly).
    """

    def __init__(
        self,
        spark: SparkSession,
        codmes_ini: int,
        codmes_fin: int,
        sink_catalog: str,
        sink_schema: str,
        sink_table: str,
        verbosity: bool = True
    ):
        self.spark = spark
        self.codmes_ini = int(codmes_ini)
        self.codmes_fin = int(codmes_fin)
        self.sink_path = f"{sink_catalog}.{sink_schema}.{sink_table}"
        self.verbosity = verbosity

        # Lista de valores dummy que deben reemplazarse por NULL
        self.GLOB_DUMMY_LIST = [
            1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555,
            6666666666, 7777777777, -99, -999, 44444.4444,
            555555.5555, 666666.6666, 77777.7777, 111111.1111,
            -111111.1111, 222222.2222, -222222.2222, 333333.3333,
            -333333.3333, None
        ]

        # Definición de variables finales (features) según el modelo
        self.features = [
            'MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m_cap',
            'CLASI_EXPER_CLI__ctdempleado_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12_cap',
            'EVOL_CLI_PYM__ctdmaxdiamorau6m_cap',
            'MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_act_min_6_6_rt_u6m',
            'MOD_DEMO__ctdmesantiguedadempsunat_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_med_1_6_rt_u6m',
            'APP_SCORE_APROB_PYME__utl_3_rl_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24_cap',
            'APP_SCORE_APROB_PYME__montoade_act_max6_s_hip_rl',
            'MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12_cap',
            'MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m_cap',
            'MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m_cap',
            'MTX_MOV_CARGO_PAS__isav_tkt_opec_pago_srv_prm_u3m',
            'APP_SCORE_APROB_PYME__edad_fin_cap',
            'PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_12_12_rt_u12',
            'MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m_cap',
            'VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12_cap',
            'RNG_ACTIVIDAD_ECONOM',
            'MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12_cap',
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12_cap',
            'MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m_cap',
            'MOD_ACT__pctratiomtoopeaprmu6mopecprmu12_cap',
            'MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m_cap',
            'APP_SCORE_APROB_PYME__meses_activo_sf_bu_ma6_0_ag',
            'MTX_TRX_CANAL_PAGO_TRANSF__can_ctd_tmo_tot_pag_bcp_frq_u6m',
            'APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl_cap',
        ]

    def _build_universe(self) -> DataFrame:
        """Construye el universo base de clientes (clientes)"""
        clientes0 = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_seginfobasesgenerales_vu.hm_portafoliocredito"
        ).select(
            "codmes", "codclaveunicocli", "codinternocomputacional", "codclavepartycli"
        ).filter(
            F.trim(F.col("CODPRODUCTO")).isin(
                'CCOPCV', 'CCOEFT', 'CNEEFT', 'PAGPYM', 'CCOALP', 'CCOFRO',
                'CCOCTB', 'CCORHB', 'CCORHC', 'CCOCTI', 'CPERLM', 'CPEJLM',
                'CCOEFG', 'CCOEFM', 'CNEEFA', 'CNEEFG', 'CPEEFC', 'CCOEFC',
                'DSGEFN', 'TCRCJD', 'TCRCJS', 'TCRCND', 'TCRCNS', 'TCRNEJ', 'TCRNEN'
            )
            & (F.col("codmes").between(self.codmes_ini, self.codmes_fin))
            & (F.col("codclaveunicocli").isNotNull())
            & (F.col("codinternocomputacional").isNotNull())
            & (F.col("ctdmesmaduración") > 0)
        ).distinct()

        clientes = clientes0.groupBy("codclaveunicocli", "codmes").agg(
            F.max("codinternocomputacional").alias("codinternocomputacional"),
            F.max("codclavepartycli").alias("codclavepartycli")
        )
        return clientes

    def _read_and_join_variables(self, df_universe: DataFrame) -> DataFrame:
        """Lee todas las tablas de variables y realiza los joins left"""
        CODMES_IN = self.codmes_ini

        # ------------------------------------------------------------
        # Tablas que se unen por codclaveunicocli
        # (cada una se filtra por codmes >= CODMES_IN, se aplica desplazamiento si es necesario)
        # ------------------------------------------------------------
        # 1. MOD_DEMO (desfase +1)
        bd_MOD_DEMO = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_rbmbcapym_apppyme_vu.hm_scoreappbasepymemodulodemografico"
        ).select(
            F.col("CODMES").alias("CODMES_0"),
            "codeclaveunicocl",
            F.col("ctdmesantiguedadempsunat").alias("MOD_DEMO__ctdmesantiguedadempsunat")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        bd_MOD_DEMO = bd_MOD_DEMO.withColumn("codmes", add_codmes_spark("CODMES_0", +1))
        bd_MOD_DEMO = bd_MOD_DEMO.drop("CODMES_0").withColumnRenamed("codeclaveunicocl", "codclaveunicocli")

        # 2. MOD_ACT (desfase +1)
        bd_MOD_ACT = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_rbmbcapym_apppyme_vu.hm_scoreappbasepymemoduloactivo"
        ).select(
            F.col("CODMES_0").alias("CODMES_0"),
            "codclaveunicolli",
            F.col("pctratiomtodecdeudapymertmtopasivoprmu3m").alias("MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m"),
            F.col("pctratiomtoopeaprmu6mopecprmu12").alias("MOD_ACT__pctratiomtoopeaprmu6mopecprmu12"),
            F.col("isav_mto_opea_estvta_pym_u6m_rt_max_u12").alias("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        bd_MOD_ACT = bd_MOD_ACT.withColumn("codmes", add_codmes_spark("CODMES_0", +1))
        bd_MOD_ACT = bd_MOD_ACT.drop("CODMES_0").withColumnRenamed("codclaveunicolli", "codclaveunicocli")

        # 3. APP_SCORE_APROB_PYME (solo mes fijo 202506, desfase +1)
        bd_APP_SCORE_APROB_PYME = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_rbmbcapym_apppyme_vu.hm_scorepreaprobacionapppyme"
        ).select(
            F.col("CODMES").alias("CODMES_0"),
            "codclaveunicocl1",
            F.col("atrasomax_crnenr_12_rl").alias("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl"),
            F.col("montoade_act_max6_s_hip_rl").alias("APP_SCORE_APROB_PYME__montoade_act_max6_s_hip_rl"),
            F.col("utl_3_rl").alias("APP_SCORE_APROB_PYME__utl_3_rl"),
            F.col("act_eco_fin").alias("APP_SCORE_APROB_PYME__act_eco_fin"),
            F.col("edad_fin").alias("APP_SCORE_APROB_PYME__edad_fin"),
            F.col("meses_activo_sf_bu_ma6_0_ag").alias("APP_SCORE_APROB_PYME__meses_activo_sf_bu_ma6_0_ag")
        ).filter(F.col("codmes") == 202506).distinct()
        bd_APP_SCORE_APROB_PYME = bd_APP_SCORE_APROB_PYME.withColumn("codmes", add_codmes_spark("CODMES_0", +1))
        bd_APP_SCORE_APROB_PYME = bd_APP_SCORE_APROB_PYME.drop("CODMES_0").withColumnRenamed("codclaveunicocl1", "codclaveunicocli")

        # RNG_ACTIVIDAD_ECONOM derivada
        bd_APP_SCORE_APROB_PYME = bd_APP_SCORE_APROB_PYME.withColumn(
            "RNG_ACTIVIDAD_ECONOM",
            F.when(F.col("APP_SCORE_APROB_PYME__act_eco_fin").isNull(), 1)
            .when(F.col("APP_SCORE_APROB_PYME__act_eco_fin").isin(
                'PESCA', 'OTROS', 'SERVICIOS', 'ENERGIA', 'CONSTRUCCION',
                'ADM_PUBLICA', 'ACT INMOB, EMP Y DE ALQ', 'INDUST_MANUFACT',
                'COMERCIO', 'HOGAR', 'SALUD'
            ), 1).otherwise(0)
        )

        # 4. VIDEVAR_MTX_MORA_POND_CLI_MMGR (mes 202505, desfase +1)
        VIDEVAR_MTX_MORA_POND_CLI_MMGR = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_matrizmoraponderadaclientemmgr"
        ).select(
            F.col("CODMES").alias("CODMES_0"),
            "codclaveunicocli",
            F.col("mtodeudaclasifriesgofactordsctosolu12").alias("VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12")
        ).filter(F.col("codmes") == 202505).distinct()
        VIDEVAR_MTX_MORA_POND_CLI_MMGR = VIDEVAR_MTX_MORA_POND_CLI_MMGR.withColumn("codmes", add_codmes_spark("CODMES_0", +1))
        VIDEVAR_MTX_MORA_POND_CLI_MMGR = VIDEVAR_MTX_MORA_POND_CLI_MMGR.drop("CODMES_0")

        # 5. PASIVO_EVOL_SALD_PYM (mes 202505, desfase +1) - join por codinternocomputacional
        PASIVO_EVOL_SALD_PYM = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variablepasivoevolucionsaldopyme"
        ).select(
            F.col("CODMES").alias("CODMES_0"),
            "codinternocomputacional",
            F.col("mtoprmincrvariacionmensualprmvigsolu6m").alias("PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m")
        ).filter(F.col("codmes") == 202505).distinct()
        PASIVO_EVOL_SALD_PYM = PASIVO_EVOL_SALD_PYM.withColumn("codmes", add_codmes_spark("CODMES_0", +1))
        PASIVO_EVOL_SALD_PYM = PASIVO_EVOL_SALD_PYM.drop("CODMES_0")

        # 6. MTX_RCC_OTRA_DEUDA (sin desfase)
        MTX_RCC_OTRA_DEUDA = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccotradeuda"
        ).select(
            "codmes", "codclaveunicocli",
            F.col("rcc_mto_rdv_max_u3m").alias("MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()

        # 7. CLASI_EXPER_CLI
        CLASI_EXPER_CLI = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_clasificacionclientenivelexperienciapyme"
        ).select(
            "codmes", "codclaveunicocli",
            F.col("ctdempleado").alias("CLASI_EXPER_CLI__ctdempleado")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()

        # 8. EVOL_CLI_PYM (join por codinternocomputacional)
        EVOL_CLI_PYM = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variableactivoevolucionclientepyme"
        ).select(
            "codmes", "codinternocomputacional",
            F.col("ctdmaxdiamorau6m").alias("EVOL_CLI_PYM__ctdmaxdiamorau6m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()

        # 9. MTX_RESUMEN_SALDO
        MTX_RESUMEN_SALDO = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldo"
        ).select(
            "codmes", "codclaveunicocli",
            F.col("prod_ctd_sld_act_u1m").alias("MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()

        # 10. MTX_RESUMEN_ACT_PAS
        MTX_RESUMEN_ACT_PAS = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo"
        ).select(
            "codmes", "codclaveunicocli",
            F.col("prod_mto_sld_fim_pas_min_24_24_rt_u24").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24"),
            F.col("prod_mto_sld_fim_tsav_max_12_12_rt_u12").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_12_12_rt_u12"),
            F.col("prod_mto_sld_fim_tsav_med_1_6_rt_u6m").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_med_1_6_rt_u6m"),
            F.col("prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m"),
            F.col("prod_mto_sld_prm_tsav_med_1_6_rt_u6m").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m"),
            F.col("prod_pct_pmtsav_pmact_24_24_rt_u24").alias("MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24"),
            F.col("prod_mto_sld_prm_pas_max_min_12_12_rt_u12").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12"),
            F.col("prod_mto_sld_fim_act_min_6_6_rt_u6m").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_act_min_6_6_rt_u6m"),
            F.col("prod_mto_sld_prm_act_max_min_6_6_rt_u6m").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m"),
            F.col("prod_mto_sld_fim_tsav_max_min_12_12_rt_u12").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()

        # 11. MTX_MOV_ABONO_PAS
        MTX_MOV_ABONO_PAS = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizmovimientoabonopasivo"
        ).select(
            "codmes", "codclaveunicolcli",
            F.col("isav_tkt_opea_trnf_dol_max_u3m").alias("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        MTX_MOV_ABONO_PAS = MTX_MOV_ABONO_PAS.withColumnRenamed("codclaveunicolcli", "codclaveunicocli")

        # 12. MTX_MOV_CARGO_PAS
        MTX_MOV_CARGO_PAS = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizmovimientocargopasivo"
        ).select(
            "codmes", "codclaveunicolcli",
            F.col("isav_tkt_opec_pago_srv_prm_u3m").alias("MTX_MOV_CARGO_PAS__isav_tkt_opec_pago_srv_prm_u3m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        MTX_MOV_CARGO_PAS = MTX_MOV_CARGO_PAS.withColumnRenamed("codclaveunicolcli", "codclaveunicocli")

        # 13. MTX_TRX_CANAL_PAGO_TRANSF
        MTX_TRX_CANAL_PAGO_TRANSF = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanalpagotransferencia"
        ).select(
            "codmes", "codclaveunicoli",
            F.col("can_ctd_tmo_tot_pag_bcp_frq_u6m").alias("MTX_TRX_CANAL_PAGO_TRANSF__can_ctd_tmo_tot_pag_bcp_frq_u6m"),
            F.col("can_mto_tmo_tot_pag_bcp_prm_u6m").alias("MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        MTX_TRX_CANAL_PAGO_TRANSF = MTX_TRX_CANAL_PAGO_TRANSF.withColumnRenamed("codclaveunicoli", "codclaveunicocli")

        # 14. MTX_RCC_PROD
        MTX_RCC_PROD = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto"
        ).select(
            "codmes", "codclaveunicoli",
            F.col("rcc_tip_cond_mor_max_crnor_max_u6m").alias("MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        MTX_RCC_PROD = MTX_RCC_PROD.withColumnRenamed("codclaveunicoli", "codclaveunicocli")

        # 15. bd_MTX_TRX_CANAL
        bd_MTX_TRX_CANAL = self.spark.table(
            "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanal"
        ).select(
            "codmes", "codclaveunicoli",
            F.col("can_tkt_tmo_tot_sol_min_u12").alias("MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12")
        ).filter(F.col("codmes") >= CODMES_IN).distinct()
        bd_MTX_TRX_CANAL = bd_MTX_TRX_CANAL.withColumnRenamed("codclaveunicoli", "codclaveunicocli")

        # ------------------------------------------------------------
        # Realizar joins por codclaveunicocli
        # ------------------------------------------------------------
        joins_codclave = [
            bd_MOD_DEMO, bd_MOD_ACT, bd_APP_SCORE_APROB_PYME,
            VIDEVAR_MTX_MORA_POND_CLI_MMGR, MTX_RCC_OTRA_DEUDA, CLASI_EXPER_CLI,
            MTX_RESUMEN_SALDO, MTX_RESUMEN_ACT_PAS, MTX_MOV_ABONO_PAS,
            MTX_MOV_CARGO_PAS, MTX_TRX_CANAL_PAGO_TRANSF, MTX_RCC_PROD, bd_MTX_TRX_CANAL
        ]
        df_result = df_universe
        for tabla in joins_codclave:
            df_result = df_result.join(
                tabla.select("codmes", "codclaveunicocli", *[c for c in tabla.columns if c not in ("codmes", "codclaveunicocli")]),
                on=["codmes", "codclaveunicocli"],
                how="left"
            )

        # ------------------------------------------------------------
        # Joins por codinternocomputacional
        # ------------------------------------------------------------
        joins_codinterno = [PASIVO_EVOL_SALD_PYM, EVOL_CLI_PYM]
        for tabla in joins_codinterno:
            df_result = df_result.join(
                tabla.select("codmes", "codinternocomputacional", *[c for c in tabla.columns if c not in ("codmes", "codinternocomputacional")]),
                on=["codmes", "codinternocomputacional"],
                how="left"
            )

        return df_result

    def _replace_dummies(self, df: DataFrame) -> DataFrame:
        """Reemplaza valores dummy por NULL en todas las columnas numéricas"""
        # Identificar columnas numéricas
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
        # Aplicar reemplazo
        for col_name in numeric_cols:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isin(self.GLOB_DUMMY_LIST), F.lit(None))
                 .otherwise(F.col(col_name))
            )
        return df

    def _apply_cappings(self, df: DataFrame) -> DataFrame:
        """Aplica los cappings (clips) según las definiciones del notebook Troncal"""
        # Cada transformación crea una nueva columna con sufijo _cap
        # Usamos F.greatest(F.least(...)) para hacer clip

        # 1. MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m_cap
        col1 = "MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m"
        df = df.withColumn(col1 + "_cap", F.greatest(F.least(F.col(col1), F.lit(54058.633)), F.lit(None)))

        # 2. CLASI_EXPER_CLI__ctdempleado_cap
        col2 = "CLASI_EXPER_CLI__ctdempleado"
        df = df.withColumn(col2 + "_cap", F.when(F.col(col2).isNotNull(), F.greatest(F.least(F.col(col2), F.lit(203.0)), F.lit(0))).otherwise(F.lit(None)))

        # 3. MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12_cap
        col3 = "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12"
        df = df.withColumn(col3 + "_cap", F.greatest(F.least(F.col(col3), F.lit(88040.516)), F.lit(None)))

        # 4. EVOL_CLI_PYM__ctdmaxdiamorau6m_cap
        col4 = "EVOL_CLI_PYM__ctdmaxdiamorau6m"
        df = df.withColumn(col4 + "_cap", F.when(F.col(col4).isNotNull(), F.greatest(F.least(F.col(col4), F.lit(59.0)), F.lit(0))).otherwise(F.lit(None)))

        # 5. MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m_cap
        col5 = "MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m"
        df = df.withColumn(col5 + "_cap", F.greatest(F.least(F.col(col5), F.lit(13.0)), F.lit(None)))

        # 6. MOD_DEMO__ctdmesantiguedadempsunat_cap
        col6 = "MOD_DEMO__ctdmesantiguedadempsunat"
        df = df.withColumn(col6 + "_cap", F.greatest(F.least(F.col(col6), F.lit(396.0)), F.lit(None)))

        # 7. APP_SCORE_APROB_PYME__utl_3_rl_cap
        col7 = "APP_SCORE_APROB_PYME__utl_3_rl"
        df = df.withColumn(col7 + "_cap", F.when(F.col(col7).isNotNull(), F.greatest(F.least(F.col(col7), F.lit(261.9096)), F.lit(0))).otherwise(F.lit(None)))

        # 8. MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24_cap
        col8 = "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24"
        df = df.withColumn(col8 + "_cap", F.greatest(F.least(F.col(col8), F.lit(0.8768666)), F.lit(None)))

        # 9. MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12_cap
        col9 = "MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12"
        df = df.withColumn(col9 + "_cap", F.greatest(F.least(F.col(col9), F.lit(20916.992)), F.lit(None)))

        # 10. MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24_cap
        col10 = "MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24"
        df = df.withColumn(col10 + "_cap", F.greatest(F.least(F.col(col10), F.lit(19.579279)), F.lit(None)))

        # 11. MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m_cap
        col11 = "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m"
        df = df.withColumn(col11 + "_cap", F.greatest(F.least(F.col(col11), F.lit(4771.6816)), F.lit(None)))

        # 12. MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m_cap
        col12 = "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m"
        df = df.withColumn(col12 + "_cap", F.greatest(F.least(F.col(col12), F.lit(4.0419364)), F.lit(None)))

        # 13. MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m_cap
        col13 = "MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m"
        df = df.withColumn(col13 + "_cap", F.greatest(F.least(F.col(col13), F.lit(480.26755)), F.lit(None)))

        # 14. APP_SCORE_APROB_PYME__edad_fin_cap
        col14 = "APP_SCORE_APROB_PYME__edad_fin"
        df = df.withColumn(col14 + "_cap", F.when(F.col(col14).isNotNull(), F.greatest(F.least(F.col(col14), F.lit(75)), F.lit(25))).otherwise(F.lit(None)))

        # 15. PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m_cap
        col15 = "PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m"
        df = df.withColumn(col15 + "_cap", F.greatest(F.least(F.col(col15), F.lit(211663.56)), F.lit(None)))

        # 16. MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m_cap
        col16 = "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m"
        df = df.withColumn(col16 + "_cap", F.greatest(F.least(F.col(col16), F.lit(386732.84)), F.lit(None)))

        # 17. MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m_cap
        col17 = "MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m"
        df = df.withColumn(col17 + "_cap", F.greatest(F.least(F.col(col17), F.lit(469.555)), F.lit(None)))

        # 18. VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12_cap
        col18 = "VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12"
        df = df.withColumn(col18 + "_cap", F.greatest(F.least(F.col(col18), F.lit(709515.0)), F.lit(None)))

        # 19. MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12_cap
        col19 = "MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12"
        df = df.withColumn(col19 + "_cap", F.when(F.col(col19).isNotNull(), F.greatest(F.least(F.col(col19), F.lit(1)), F.lit(0))).otherwise(F.lit(None)))

        # 20. MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12_cap
        col20 = "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12"
        df = df.withColumn(col20 + "_cap", F.greatest(F.least(F.col(col20), F.lit(372685.25)), F.lit(None)))

        # 21. MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m_cap
        col21 = "MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m"
        df = df.withColumn(col21 + "_cap", F.greatest(F.least(F.col(col21), F.lit(379400.0)), F.lit(None)))

        # 22. MOD_ACT__pctratiomtoopeaprmu6mopecprmu12_cap
        col22 = "MOD_ACT__pctratiomtoopeaprmu6mopecprmu12"
        df = df.withColumn(col22 + "_cap", F.greatest(F.least(F.col(col22), F.lit(2.637802)), F.lit(None)))

        # 23. MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m_cap
        col23 = "MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m"
        df = df.withColumn(col23 + "_cap", F.greatest(F.least(F.col(col23), F.lit(112905.56)), F.lit(None)))

        # 24. APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl_cap
        col24 = "APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl"
        df = df.withColumn(col24 + "_cap", F.when(F.col(col24).isNotNull(), F.greatest(F.least(F.col(col24), F.lit(46.0)), F.lit(0))).otherwise(F.lit(None)))

        # NOTA: Las columnas que no tienen sufijo _cap (como MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_act_min_6_6_rt_u6m,
        # APP_SCORE_APROB_PYME__montoade_act_max6_s_hip_rl, etc.) se usan directamente sin capping.
        # Además, la columna RNG_ACTIVIDAD_ECONOM ya está creada.
        return df

    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        """Selecciona las columnas clave y las features finales"""
        # Asegurar que todas las features existan en el DataFrame
        available_cols = df.columns
        selected_features = [f for f in self.features if f in available_cols]
        missing = set(self.features) - set(selected_features)
        if missing:
            print(f"⚠️ Advertencia: Las siguientes features no se encontraron: {missing}")
        # Incluir siempre las columnas PK
        pk_cols = ["codmes", "codclaveunicocli", "codinternocomputacional", "codclavepartycli"]
        final_cols = pk_cols + selected_features
        return df.select(*final_cols)

    def _write_table(self, df: DataFrame) -> None:
        """Escribe el DataFrame final en Unity Catalog"""
        # Particionar por codmes para eficiencia
        df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("codmes") \
            .saveAsTable(self.sink_path)
        print(f"✅ Tabla escrita en: {self.sink_path}")

    def execute(self) -> None:
        """Ejecuta el pipeline completo de construcción del universo y variables"""
        print("=" * 60)
        print("Iniciando construcción del universo para modelo PD BHV Troncal")
        print(f"Rango de meses: {self.codmes_ini} - {self.codmes_fin}")
        print("=" * 60)

        # 1. Universo base
        print("1. Construyendo universo de clientes...")
        universe = self._build_universe()
        universe.persist(StorageLevel.MEMORY_AND_DISK)
        cnt = universe.count()
        print(f"   Universo tamaño: {cnt:,} registros")

        # 2. Joins con variables
        print("2. Uniendo tablas de variables...")
        df_enriched = self._read_and_join_variables(universe)
        df_enriched.persist(StorageLevel.MEMORY_AND_DISK)
        print(f"   Dataframe enriquecido: {df_enriched.count():,} registros, {len(df_enriched.columns)} columnas")

        # 3. Reemplazo de dummies
        print("3. Reemplazando valores dummy por NULL...")
        df_clean = self._replace_dummies(df_enriched)

        # 4. Aplicar cappings
        print("4. Aplicando cappings (clips) a variables...")
        df_capped = self._apply_cappings(df_clean)

        # 5. Seleccionar columnas finales
        print("5. Seleccionando features finales...")
        df_final = self._select_final_columns(df_capped)

        # 6. Escribir tabla
        print("6. Escribiendo tabla master...")
        self._write_table(df_final)

        print("=" * 60)
        print("✅ Proceso completado exitosamente")
        print("=" * 60)

        # Liberar caché
        universe.unpersist()
        df_enriched.unpersist()