# universo_implementacion.py
import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.storagelevel import StorageLevel

from utils.data_preparation.helpers import add_codmes_spark

class UniversoImplementacion:
    def __init__(
        self,
        spark: SparkSession,
        codmes: int = None,
        codmes_ini: int = None,
        codmes_fin: int = None,
        src_catalog: str = "catalog_lhcl_prod.bcp",
        sink_catalog: str = None,
        sink_schema: str = None,
        sink_table: str = None,
        verbosity: bool = True
    ):
        self.spark = spark
        self.verbosity = verbosity

        if codmes is not None:
            self.codmes_ini = int(codmes)
            self.codmes_fin = int(codmes)
        elif codmes_ini is not None and codmes_fin is not None:
            self.codmes_ini = int(codmes_ini)
            self.codmes_fin = int(codmes_fin)
        else:
            raise ValueError("Debe proporcionar 'codmes' o ('codmes_ini', 'codmes_fin')")

        self.src_catalog = src_catalog
        if sink_catalog and sink_schema and sink_table:
            self.sink_path = f"{sink_catalog}.{sink_schema}.{sink_table}"
        else:
            self.sink_path = None

        self.GLOB_DUMMY_LIST = [
            1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555,
            6666666666, 7777777777, -99, -999, 44444.4444,
            555555.5555, 666666.6666, 77777.7777, 111111.1111,
            -111111.1111, 222222.2222, -222222.2222, 333333.3333,
            -333333.3333, None
        ]

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

    # --------------------------------------------------------------
    # 1. Universo base
    # --------------------------------------------------------------
    def _build_universe(self) -> DataFrame:
        clientes0 = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_adrmmgr_seginfobasesgenerales_vu.hm_portafoliocredito"
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
            & (F.col("ctdmesmaduracion") > 0)
        ).distinct()

        clientes = clientes0.groupBy("codclaveunicocli", "codmes").agg(
            F.max("codinternocomputacional").alias("codinternocomputacional"),
            F.max("codclavepartycli").alias("codclavepartycli")
        )
        return clientes

    # --------------------------------------------------------------
    # 2. Lectura y joins de todas las variables
    # --------------------------------------------------------------
    def _read_and_join_variables(self, df_universe: DataFrame) -> DataFrame:
        CODMES_IN = self.codmes_ini

        # --- Tablas que requieren offset +1 (desfase) ---
        # a) MOD_DEMO
        bd_MOD_DEMO = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_scoreappbasepymemodulodemografico"
        ).select(
            F.col("codmes").alias("codmes_original"),
            F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("ctdmesantiguedadempsunat").alias("MOD_DEMO__ctdmesantiguedadempsunat")
        ).distinct()
        bd_MOD_DEMO = bd_MOD_DEMO.withColumn(
            "codmes",
            add_codmes_spark("codmes_original", +1)
        ).drop("codmes_original")

        # b) MOD_ACT
        bd_MOD_ACT = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_scoreappbasepymemoduloactivo"
        ).select(
            F.col("codmes").alias("codmes_original"),
            F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("pctratiomtodecdeudapymertmtopasivoprmu3m").alias("MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m"),
            F.col("pctratiomtoopeaprmu6mopecprmu12").alias("MOD_ACT__pctratiomtoopeaprmu6mopecprmu12"),
            F.col("isav_mto_opea_estvta_pym_u6m_rt_max_u12").alias("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12")
        ).distinct()
        bd_MOD_ACT = bd_MOD_ACT.withColumn(
            "codmes",
            add_codmes_spark("codmes_original", +1)
        ).drop("codmes_original")

        # c) APP_SCORE_APROB_PYME
        bd_APP_SCORE_APROB_PYME = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_scorepreaprobacionapppyme"
        ).select(
            F.col("codmes").alias("codmes_original"),
            F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("atrasomax_crnenr_12_rl").alias("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl"),
            F.col("montoade_act_max6_s_hip_rl").alias("APP_SCORE_APROB_PYME__montoade_act_max6_s_hip_rl"),
            F.col("utl_3_rl").alias("APP_SCORE_APROB_PYME__utl_3_rl"),
            F.col("act_eco_fin").alias("APP_SCORE_APROB_PYME__act_eco_fin"),
            F.col("edad_fin").alias("APP_SCORE_APROB_PYME__edad_fin"),
            F.col("meses_activo_sf_bu_ma6_0_ag").alias("APP_SCORE_APROB_PYME__meses_activo_sf_bu_ma6_0_ag")
        ).distinct()
        bd_APP_SCORE_APROB_PYME = bd_APP_SCORE_APROB_PYME.withColumn(
            "codmes",
            add_codmes_spark("codmes_original", +1)
        ).drop("codmes_original")
        # Derivar RNG_ACTIVIDAD_ECONOM
        bd_APP_SCORE_APROB_PYME = bd_APP_SCORE_APROB_PYME.withColumn(
            "RNG_ACTIVIDAD_ECONOM",
            F.when(F.col("APP_SCORE_APROB_PYME__act_eco_fin").isNull(), 1)
            .when(F.col("APP_SCORE_APROB_PYME__act_eco_fin").isin(
                'PESCA', 'OTROS', 'SERVICIOS', 'ENERGIA', 'CONSTRUCCION',
                'ADM_PUBLICA', 'ACT INMOB, EMP Y DE ALQ', 'INDUST_MANUFACT',
                'COMERCIO', 'HOGAR', 'SALUD'
            ), 1).otherwise(0)
        )

        # d) VIDEVAR_MTX_MORA_POND_CLI_MMGR 
        VIDEVAR_MTX_MORA_POND_CLI_MMGR = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_matrizmoraponderadaclientemmgr"
        ).select(
            F.col("codmes").alias("codmes_original"),
            "codclaveunicocli",
            F.col("mtodeudaclasifriesgofactordsctosolu12").alias("VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12")
        ).distinct()
        VIDEVAR_MTX_MORA_POND_CLI_MMGR = VIDEVAR_MTX_MORA_POND_CLI_MMGR.withColumn(
            "codmes",
            add_codmes_spark("codmes_original", +1)
        ).drop("codmes_original")

        # e) PASIVO_EVOL_SALD_PYM  - join por codinternocomputacional
        PASIVO_EVOL_SALD_PYM = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variablepasivoevolucionsaldopyme"
        ).select(
            F.col("codmes").alias("codmes_original"),
            "codinternocomputacional",
            F.col("mtoprmincrvariacionmensualprmvigsolu6m").alias("PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m")
        ).distinct()
        PASIVO_EVOL_SALD_PYM = PASIVO_EVOL_SALD_PYM.withColumn(
            "codmes",
            add_codmes_spark("codmes_original", +1)
        ).drop("codmes_original")

        # --- Tablas sin offset (se unen directamente por codmes) ---
        # f) MTX_RCC_OTRA_DEUDA
        MTX_RCC_OTRA_DEUDA = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccotradeuda"
        ).select("codmes", "codclaveunicocli",
                 F.col("rcc_mto_rdv_max_u3m").alias("MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m")
        ).distinct()

        # g) CLASI_EXPER_CLI
        CLASI_EXPER_CLI = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_clasificacionclientenivelexperienciapyme"
        ).select("codmes", "codclaveunicocli",
                 F.col("ctdempleado").alias("CLASI_EXPER_CLI__ctdempleado")
        ).distinct()

        # h) EVOL_CLI_PYM (join por codinternocomputacional)
        EVOL_CLI_PYM = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variableactivoevolucionclientepyme"
        ).select("codmes", "codinternocomputacional",
                 F.col("ctdmaxdiamorau6m").alias("EVOL_CLI_PYM__ctdmaxdiamorau6m")
        ).distinct()

        # i) MTX_RESUMEN_SALDO
        MTX_RESUMEN_SALDO = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldo"
        ).select("codmes", "codclaveunicocli",
                 F.col("prod_ctd_sld_act_u1m").alias("MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m")
        ).distinct()

        # j) MTX_RESUMEN_ACT_PAS
        MTX_RESUMEN_ACT_PAS = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo"
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
        ).distinct()

        # k) MTX_MOV_ABONO_PAS
        MTX_MOV_ABONO_PAS = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientoabonopasivo"
        ).select(
            F.col("codmes"), F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("isav_tkt_opea_trnf_dol_max_u3m").alias("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m")
        ).distinct()

        # l) MTX_MOV_CARGO_PAS
        MTX_MOV_CARGO_PAS = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientocargopasivo"
        ).select(
            F.col("codmes"), F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("isav_tkt_opec_pago_srv_prm_u3m").alias("MTX_MOV_CARGO_PAS__isav_tkt_opec_pago_srv_prm_u3m")
        ).distinct()

        # m) MTX_TRX_CANAL_PAGO_TRANSF
        MTX_TRX_CANAL_PAGO_TRANSF = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanalpagotransferencia"
        ).select(
            F.col("codmes"), F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("can_ctd_tmo_tot_pag_bcp_frq_u6m").alias("MTX_TRX_CANAL_PAGO_TRANSF__can_ctd_tmo_tot_pag_bcp_frq_u6m"),
            F.col("can_mto_tmo_tot_pag_bcp_prm_u6m").alias("MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m")
        ).distinct()

        # n) MTX_RCC_PROD
        MTX_RCC_PROD = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto"
        ).select(
            F.col("codmes"), F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("rcc_tip_cond_mor_max_crnor_max_u6m").alias("MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m")
        ).distinct()

        # o) bd_MTX_TRX_CANAL
        bd_MTX_TRX_CANAL = self.spark.table(
            f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanal"
        ).select(
            F.col("codmes"), F.col("codclaveunicocli").alias("codclaveunicocli"),
            F.col("can_tkt_tmo_tot_sol_min_u12").alias("MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12")
        ).distinct()

        # ------------------------------------------------------------
        # Realizar joins secuenciales (todos left)
        # ------------------------------------------------------------
        # Uniones por codclaveunicocli
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

        # Uniones por codinternocomputacional
        joins_codinterno = [PASIVO_EVOL_SALD_PYM, EVOL_CLI_PYM]
        for tabla in joins_codinterno:
            df_result = df_result.join(
                tabla.select("codmes", "codinternocomputacional", *[c for c in tabla.columns if c not in ("codmes", "codinternocomputacional")]),
                on=["codmes", "codinternocomputacional"],
                how="left"
            )

        return df_result

    # --------------------------------------------------------------
    # 3. Reemplazo de dummies
    # --------------------------------------------------------------
    def _replace_dummies(self, df: DataFrame) -> DataFrame:
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
        for col_name in numeric_cols:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isin(self.GLOB_DUMMY_LIST), F.lit(None))
                 .otherwise(F.col(col_name))
            )
        return df

    # --------------------------------------------------------------
    # 4. Aplicar cappings (clips) según el notebook de Sherly
    # --------------------------------------------------------------
    def _apply_cappings(self, df: DataFrame) -> DataFrame:
        # Listado completo de cappings (24 variables)
        df = df.withColumn("MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m_cap",
                           F.greatest(F.least(F.col("MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m"), F.lit(54058.633)), F.lit(None)))
        df = df.withColumn("CLASI_EXPER_CLI__ctdempleado_cap",
                           F.when(F.col("CLASI_EXPER_CLI__ctdempleado").isNotNull(),
                                  F.greatest(F.least(F.col("CLASI_EXPER_CLI__ctdempleado"), F.lit(203.0)), F.lit(0)))
                           .otherwise(F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12"), F.lit(88040.516)), F.lit(None)))
        df = df.withColumn("EVOL_CLI_PYM__ctdmaxdiamorau6m_cap",
                           F.when(F.col("EVOL_CLI_PYM__ctdmaxdiamorau6m").isNotNull(),
                                  F.greatest(F.least(F.col("EVOL_CLI_PYM__ctdmaxdiamorau6m"), F.lit(59.0)), F.lit(0)))
                           .otherwise(F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m"), F.lit(13.0)), F.lit(None)))
        df = df.withColumn("MOD_DEMO__ctdmesantiguedadempsunat_cap",
                           F.greatest(F.least(F.col("MOD_DEMO__ctdmesantiguedadempsunat"), F.lit(396.0)), F.lit(None)))
        df = df.withColumn("APP_SCORE_APROB_PYME__utl_3_rl_cap",
                           F.when(F.col("APP_SCORE_APROB_PYME__utl_3_rl").isNotNull(),
                                  F.greatest(F.least(F.col("APP_SCORE_APROB_PYME__utl_3_rl"), F.lit(261.9096)), F.lit(0)))
                           .otherwise(F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24"), F.lit(0.8768666)), F.lit(None)))
        df = df.withColumn("MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12_cap",
                           F.greatest(F.least(F.col("MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12"), F.lit(20916.992)), F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24"), F.lit(19.579279)), F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m"), F.lit(4771.6816)), F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m"), F.lit(4.0419364)), F.lit(None)))
        df = df.withColumn("MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m_cap",
                           F.greatest(F.least(F.col("MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m"), F.lit(480.26755)), F.lit(None)))
        df = df.withColumn("APP_SCORE_APROB_PYME__edad_fin_cap",
                           F.when(F.col("APP_SCORE_APROB_PYME__edad_fin").isNotNull(),
                                  F.greatest(F.least(F.col("APP_SCORE_APROB_PYME__edad_fin"), F.lit(75)), F.lit(25)))
                           .otherwise(F.lit(None)))
        df = df.withColumn("PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m_cap",
                           F.greatest(F.least(F.col("PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m"), F.lit(211663.56)), F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m"), F.lit(386732.84)), F.lit(None)))
        df = df.withColumn("MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m_cap",
                           F.greatest(F.least(F.col("MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m"), F.lit(469.555)), F.lit(None)))
        df = df.withColumn("VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12_cap",
                           F.greatest(F.least(F.col("VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12"), F.lit(709515.0)), F.lit(None)))
        df = df.withColumn("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12_cap",
                           F.when(F.col("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12").isNotNull(),
                                  F.greatest(F.least(F.col("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12"), F.lit(1)), F.lit(0)))
                           .otherwise(F.lit(None)))
        df = df.withColumn("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12_cap",
                           F.greatest(F.least(F.col("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12"), F.lit(372685.25)), F.lit(None)))
        df = df.withColumn("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m_cap",
                           F.greatest(F.least(F.col("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m"), F.lit(379400.0)), F.lit(None)))
        df = df.withColumn("MOD_ACT__pctratiomtoopeaprmu6mopecprmu12_cap",
                           F.greatest(F.least(F.col("MOD_ACT__pctratiomtoopeaprmu6mopecprmu12"), F.lit(2.637802)), F.lit(None)))
        df = df.withColumn("MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m_cap",
                           F.greatest(F.least(F.col("MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m"), F.lit(112905.56)), F.lit(None)))
        df = df.withColumn("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl_cap",
                           F.when(F.col("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl").isNotNull(),
                                  F.greatest(F.least(F.col("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl"), F.lit(46.0)), F.lit(0)))
                           .otherwise(F.lit(None)))
        return df

    # --------------------------------------------------------------
    # 5. Seleccionar columnas finales (PK + features)
    # --------------------------------------------------------------
    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        available = set(df.columns)
        selected = [f for f in self.features if f in available]
        missing = set(self.features) - set(selected)
        if missing:
            print(f"⚠️ Features faltantes: {missing}")
        pk = ["codmes", "codclaveunicocli", "codinternocomputacional", "codclavepartycli"]
        final_cols = pk + selected
        return df.select(*final_cols)

    # --------------------------------------------------------------
    # 6. Escritura en Unity Catalog
    # --------------------------------------------------------------
    def _write_table(self, df: DataFrame) -> None:
        if self.sink_path is None:
            print("⚠️ No se escribirá tabla porque no se definió sink_path")
            return
        df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("codmes") \
            .saveAsTable(self.sink_path)
        print(f"✅ Tabla guardada en {self.sink_path}")

    # --------------------------------------------------------------
    # 7. Ejecución completa
    # --------------------------------------------------------------
    def execute(self) -> None:
        print(f"Procesando meses {self.codmes_ini} - {self.codmes_fin}")
        universe = self._build_universe()
        universe.persist(StorageLevel.MEMORY_AND_DISK)
        cnt = universe.count()
        print(f"Universe: {cnt} registros")

        df_enriched = self._read_and_join_variables(universe)
        df_enriched.persist(StorageLevel.MEMORY_AND_DISK)
        print(f"Después de joins: {df_enriched.count()} registros, {len(df_enriched.columns)} columnas")

        df_clean = self._replace_dummies(df_enriched)
        df_capped = self._apply_cappings(df_clean)
        df_final = self._select_final_columns(df_capped)

        if self.sink_path:
            self._write_table(df_final)

        universe.unpersist()
        df_enriched.unpersist()
        print("Proceso finalizado")