# universo_implementacion.py////
# Propósito: Construir el universo de clientes PYME, extraer y unir variables
#            desde cada tabla fuente, aplicar reemplazo de dummies, capping (clip),
#            seleccionar las features y escribir la table_master en Unity Catalog.
#
# Basado en los notebooks de Sherly:
#   - 10_Score_BHV_Troncal (construcción del universo y joins de variables)
#   - 10_Recableo_variables_score (Recableo de tablas con prefijo "SCORE")
#
# El codmes_data (mes de las tablas fuente sin desfase) = codmes - 1 mes.

import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DoubleType
from pyspark.sql.functions import udf, array

from utils.data_preparation.helpers import add_codmes_spark, operacionesMaxBetweenCols_udf, _mes_anterior

# ---------------------------------------------------------------------------
# Lista de dummies a reemplazar por NULL (estándar BCP)
# ---------------------------------------------------------------------------
GLOB_DUMMY_LIST = [
    1111111111, -1111111111, 2222222222, -2222222222,
    3333333333, -3333333333, 4444444444, 5555555555,
    6666666666, 7777777777, -99, -999,
    44444.4444, 555555.5555, 666666.6666, 77777.7777,
    111111.1111, -111111.1111, 222222.2222, -222222.2222,
    333333.3333, -333333.3333,
]


# ---------------------------------------------------------------------------
# Reglas de capping — valores extraídos del notebook Troncal de Sherly
# Formato: (col_origen, col_destino, lower, upper)  None = sin límite
# ---------------------------------------------------------------------------
CAPPING_RULES = [
    ("MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m",                           "MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m_cap",                           None, 54058.633),
    ("CLASI_EXPER_CLI__ctdempleado",                                        "CLASI_EXPER_CLI__ctdempleado_cap",                                        0.0,  203.0),
    ("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12",    "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12_cap",    None, 88040.516),
    ("EVOL_CLI_PYM__ctdmaxdiamorau6m",                                     "EVOL_CLI_PYM__ctdmaxdiamorau6m_cap",                                      0.0,  59.0),
    ("MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m",                            "MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m_cap",                             None, 13.0),
    ("MOD_DEMO__ctdmesantiguedadempsunat",                                  "MOD_DEMO__ctdmesantiguedadempsunat_cap",                                   None, 396.0),
    ("APP_SCORE_APROB_PYME__utl_3_rl",                                     "APP_SCORE_APROB_PYME__utl_3_rl_cap",                                       0.0,  261.9096),
    ("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24",        "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24_cap",        None, 0.8768666),
    ("MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12",                         "MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12_cap",                          None, 20916.992),
    ("MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24",           "MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24_cap",           None, 19.579279),
    ("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m",    "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m_cap",    None, 4771.6816),
    ("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m",        "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m_cap",        None, 4.0419364),
    ("MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m",                  "MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m_cap",                   None, 480.26755),
    ("APP_SCORE_APROB_PYME__edad_fin",                                     "APP_SCORE_APROB_PYME__edad_fin_cap",                                       25.0, 75.0),
    ("PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m",       "PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m_cap",       None, 211663.56),
    ("MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m",     "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m_cap",     None, 386732.84),
    ("MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m",                   "MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m_cap",                    None, 469.555),
    ("VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12", "VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12_cap", None, 709515.0),
    ("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12",                   "MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12_cap",                    0.0,  1.0),
    ("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12",   "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12_cap",   None, 372685.25),
    ("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m",                 "MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m_cap",                 None, 379400.0),
    ("MOD_ACT__pctratiomtoopeaprmu6mopecprmu12",                           "MOD_ACT__pctratiomtoopeaprmu6mopecprmu12_cap",                            None, 2.637802),
    ("MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m",       "MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m_cap",       None, 112905.56),
    ("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl",                       "APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl_cap",                        None, 46.0),
]


class UniversoImplementacion:
    """
    Construye el universo de clientes PYME, extrae variables desde cada tabla
    fuente, las une, aplica reemplazo de dummies y capping (clip), selecciona
    las features del modelo y escribe la table_master en Unity Catalog.

    Parámetros (alineados con notebook 03_load_preparation_data)
    ------------------------------------------------------------
    spark        : SparkSession
    codmes       : int   — mes del proceso en formato YYYYMM
    src_catalog  : str   — catálogo fuente de tablas LHCL (ej: "catalog_lhcl_prod_bcp")
    sink_catalog : str   — catálogo destino Unity Catalog  (ej: CATALOG_NAME del config)
    sink_schema  : str   — schema destino                  (ej: SCHEMA_NAME del config)
    sink_table   : str   — nombre base de la tabla destino (ej: BASE_NAME_TABLE_MASTER)
    feature_names: list  — lista de features del modelo (FEATURE_NAMES del config).
                           Opcional; si no se pasa, el execute() devuelve todas las cols.
    source_table : str   — tabla portafolio completa (SOURCE_TABLE del config).
                           Opcional; si no se pasa se construye desde src_catalog.
    """

    _PRODUCTOS_PYME = [
        'CCOPCV', 'CCOEFT', 'CNEEFT', 'PAGPYM', 'CCOALP', 'CCOFRO',
        'CCOCTB', 'CCORHB', 'CCORHC', 'CCOCTI',
        'CPERLM', 'CPEJLM',
        'CCOEFG', 'CCOEFM', 'CNEEFA', 'CNEEFG', 'CPEEFC', 'CCOEFC', 'DSGEFN',
        'TCRCJD', 'TCRCJS', 'TCRCND', 'TCRCNS', 'TCRNEJ', 'TCRNEN',
    ]

    def __init__(
        self,
        spark: SparkSession,
        codmes: int,
        src_catalog: str,
        sink_catalog: str,
        sink_schema: str,
        sink_table: str,
        feature_names: list = None,
        source_table: str = None,
    ):
        # --- Atributos principales ---
        self.spark        = spark
        self.codmes       = int(codmes)                    # mes del proceso
        self.codmes_data  = _mes_anterior(self.codmes)     # mes tablas fuente (sin desfase)

        # --- Catálogos y tabla destino ---
        self.src_catalog  = src_catalog
        self.sink_catalog = sink_catalog
        self.sink_schema  = sink_schema
        self.sink_table   = sink_table
        self.table_master = f"{sink_catalog}.{sink_schema}.{sink_table}"

        # --- Features y tabla fuente ---
        self.feature_names = feature_names  # puede ser None → se devuelven todas las cols
        self.source_table  = (
            source_table
            if source_table
            else f"{src_catalog}.bcp_ddv_adrmmgr_seginfobasesgenerales_vu.hm_portafoliocredito"
        )

    # ------------------------------------------------------------------
    # MÉTODO PRINCIPAL
    # ------------------------------------------------------------------
    def execute(self) -> DataFrame:
        """
        Ejecuta el pipeline completo y ESCRIBE la tabla master en Unity Catalog.
        Devuelve el DataFrame resultante para que el notebook pueda contar registros.
        """
        print("=" * 70)
        print(f"UniversoImplementacion.execute()")
        print(f"  codmes      : {self.codmes}")
        print(f"  codmes_data : {self.codmes_data}  (mes tablas fuente sin desfase)")
        print(f"  table_master: {self.table_master}")
        print("=" * 70)
        start = time.time()

        # 1. Universo base PYME
        df_universo = self._build_universo()
        print(f"[1] Universo PYME: {df_universo.count():,} registros")

        # 2. Recableo: construir variables de hm_scorepreaprobacionapppyme
        df_app_score = self._build_app_score_aprob_pyme(df_universo)
        print(f"[2] APP_SCORE_APROB_PYME: {df_app_score.count():,} registros")

        # 3. Lectura de tablas de variables troncales
        df_mod_demo            = self._read_mod_demo()
        df_mod_act             = self._read_mod_act()
        df_videvar_mora_pond   = self._read_videvar_mora_pond()
        df_pasivo_evol_sald    = self._read_pasivo_evol_sald()
        df_mtx_rcc_otra_deuda  = self._read_mtx_rcc_otra_deuda()
        df_clasi_exper_cli     = self._read_clasi_exper_cli()
        df_evol_cli_pym        = self._read_evol_cli_pym()
        df_mtx_resumen_saldo   = self._read_mtx_resumen_saldo()
        df_mtx_resumen_act_pas = self._read_mtx_resumen_act_pas()
        df_mtx_mov_abono_pas   = self._read_mtx_mov_abono_pas()
        df_mtx_mov_cargo_pas   = self._read_mtx_mov_cargo_pas()
        df_mtx_trx_canal_pago  = self._read_mtx_trx_canal_pago_transf()
        df_mtx_rcc_prod        = self._read_mtx_rcc_prod()
        df_mtx_trx_canal       = self._read_mtx_trx_canal()
        print("[3] Tablas de variables troncales leídas")

        # 4. Join de todas las tablas al universo
        result = self._join_all(
            df_universo, df_app_score, df_mod_demo, df_mod_act,
            df_videvar_mora_pond, df_pasivo_evol_sald,
            df_mtx_rcc_otra_deuda, df_clasi_exper_cli,
            df_evol_cli_pym, df_mtx_resumen_saldo, df_mtx_resumen_act_pas,
            df_mtx_mov_abono_pas, df_mtx_mov_cargo_pas,
            df_mtx_trx_canal_pago, df_mtx_rcc_prod, df_mtx_trx_canal,
        )
        print(f"[4] Join completo: {len(result.columns)} columnas")

        # 5. Reemplazo de dummies por NULL
        result = self._replace_dummies(result)
        print("[5] Dummies reemplazados por NULL")

        # 6. Capping (clip)
        result = self._apply_capping(result)
        print("[6] Capping aplicado")

        # 7. RNG_ACTIVIDAD_ECONOM
        result = self._build_rng_actividad_econom(result)

        # 8. Selección de features (si feature_names fue provisto)
        if self.feature_names:
            result = self._select_features(result)
        print(f"[7] Features seleccionadas: {len(result.columns)} columnas")

        #Normalizar tipos a Doubletype
        result = self._cast_features_to_double(result)
        print("[7.1] Tipos normalizados a DoubleType")
        
        # 9. Escritura en Unity Catalog
        self._write_table_master(result)
        print(f"[8] Tabla escrita en: {self.table_master}")

        elapsed = time.time() - start
        print(f"\n✅ execute() finalizado en {elapsed:.1f}s")
        return result

    # ------------------------------------------------------------------
    # PASO 1: Universo base PYME
    # ------------------------------------------------------------------
    def _build_universo(self) -> DataFrame:
        return (
            self.spark.table(self.source_table)
            .select(
                "codmes", "codclaveunicocli",
                "codinternocomputacional", "codclavepartycli",
                "codproducto", "ctdmesmaduracion",
            )
            .filter(
                F.trim(F.col("codproducto")).isin(self._PRODUCTOS_PYME)
                & (F.col("codmes") == self.codmes)
                & F.col("codclaveunicocli").isNotNull()
                & F.col("codinternocomputacional").isNotNull()
                & (F.col("ctdmesmaduracion") > 0)
            )
            .distinct()
            .groupBy("codclaveunicocli")
            .agg(
                F.max("codinternocomputacional").alias("codinternocomputacional"),
                F.max("codclavepartycli").alias("codclavepartycli"),
            ).withColumn("codmes", F.lit(self.codmes).cast(IntegerType())) 
        )

    # ------------------------------------------------------------------
    # PASO 2: Recableo — construir hm_scorepreaprobacionapppyme
    # ------------------------------------------------------------------
    def _build_app_score_aprob_pyme(self, df_universo: DataFrame) -> DataFrame:
        cd = self.codmes_data

        # 2.1 Tipo de identificación
        cliente_prospecto = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_rbmbcapym_modelogestion_vu.hm_clienteprospectopyme"
            )
            .filter(F.col("codmes") == cd)
            .select("CODCLAVEUNICOCLI", "tippartyidentificacion")
        )

        # 2.2 Relacionados
        relacionados = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_relacionadoapppyme"
            )
            .filter(F.col("codmes") == cd)
            .select("CODCLAVEUNICOCLI", "CODCLAVEUNICOCLIREL", "DESTIPREL", "PCTPARTICIPACIONREL")
            .join(cliente_prospecto, ["CODCLAVEUNICOCLI"], "left_outer")
        )

        # 2.3/2.4 Dueño único
        duenios = relacionados.filter(
            F.col("DESTIPREL").isin("DUENIO", "DUENIO P.N.")
        ).select(
            "CODCLAVEUNICOCLI", "CODCLAVEUNICOCLIREL",
            F.when(F.col("tippartyidentificacion") == "6", "J")
             .otherwise("N").alias("FLGTIPPER"),
        )
        duenio_unico = duenios.groupBy("CODCLAVEUNICOCLI").agg(
            F.max(
                F.when(F.col("FLGTIPPER") == "N", F.col("CODCLAVEUNICOCLI"))
                 .otherwise(F.col("CODCLAVEUNICOCLIREL"))
            ).alias("CODCLAVEUNICOCLIREL")
        )

        # 2.5 FLGRLDUENIO
        relacionados_con_flag = (
            relacionados.alias("A")
            .join(
                duenio_unico.alias("B"),
                (F.col("A.CODCLAVEUNICOCLI") == F.col("B.CODCLAVEUNICOCLI")) &
                (F.col("A.CODCLAVEUNICOCLIREL") == F.col("B.CODCLAVEUNICOCLIREL")),
                "left_outer",
            )
            .select(
                F.col("A.CODCLAVEUNICOCLI"),
                F.col("A.CODCLAVEUNICOCLIREL"),
                F.col("A.DESTIPREL"),
                F.when(
                    F.col("A.DESTIPREL").isin("DUENIO", "DUENIO P.N."),
                    F.when(F.col("B.CODCLAVEUNICOCLI").isNull(), 0).otherwise(1),
                ).otherwise(0).alias("FLGRELDUENIO"),
            )
        )
        relacion_dueno_final = (
            relacionados_con_flag
            .filter(F.col("FLGRELDUENIO") == 1)
            .select("CODCLAVEUNICOCLI", "CODCLAVEUNICOCLIREL")
        )

        # 2.6 Universo APP PYME (edad, actividad económica)
        universo_apppyme = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_clientepreaprobacionapppyme"
            )
            .filter(F.col("codmes") == cd)
            .select(
                "CODCLAVEUNICOCLI",
                F.col("NUMEDAD").cast(IntegerType()).alias("EDAD_FIN"),
                F.col("DESSECCIONECONOMICAAPPPYME").alias("ACT_ECO_FIN"),
                "TIPPARTYIDENTIFICACION",
            )
        )

        # 2.7 Carretera RCC + campo base meses_activo
        carretera_rcc = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto"
            )
            .filter(F.col("codmes") == cd)
            .select(
                "codclaveunicocli",
                F.col("RCC_TIP_COND_MOR_MAX_CRNNR_MAX_U12").alias("ATRASOMAX_CRNENR_12"),
                F.col("RCC_MTO_ADE_ACT_SHIP_RT_U6M").alias("MONTOADE_ACT_MAX6_S_HIP"),
                F.col("RCC_PCT_UTL_3_RT_U3M").alias("UTL_3"),
                F.col("RCC_CTD_SF_CAL_CPP_FRQ_0_U24").alias("SF_NUM_CAL_CPP24"),
                F.col("RCC_CTD_MES_ACT_SF_BUEN_MAL_0_U6M").alias("MESES_ACTIVO_SF_BU_MA6_0_BASE"),
            )
        )

        # 2.8 Resumen saldo activo/pasivo
        resumen_saldo = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo"
            )
            .filter(F.col("codmes") == cd)
            .select(
                "codclaveunicocli",
                F.col("PROD_MTO_SLD_PRM_TSAV_FRQ_100_U24").alias("MESES_PMSAVMF_24_100"),
                F.col("PROD_CTD_MES_PAS_ACT_MAX_V2_1000_U12").alias("MESES_PASIVO_ACTIVO_12_1000"),
            )
        )

        # 2.9 Materialidad
        materialidad = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_variablerccmaterialidadapppyme"
            )
            .filter(F.col("codmes") == cd)
            .select(
                "codclaveunicocli",
                F.col("RCC_TIP_COND_MOR_MAX_CRNNR_100_MAX_U12").alias("ATRASOMAX_CRNENR_12_100"),
                F.col("RCC_CTD_SF_CAL_CPP_FRQ_100_U24").alias("SF_NUM_CAL_CPP24_100"),
                F.col("RCC_CTD_MES_ACT_SF_BUEN_MAL_100_U6M").alias("MESES_ACTIVO_SF_BU_MA6_100"),
            )
        )

        # 2.10 Carretera completa con materialidad
        carretera_final = (
            carretera_rcc
            .join(resumen_saldo, ["codclaveunicocli"], "fullouter")
            .join(materialidad,  ["codclaveunicocli"], "fullouter")
            .select(
                "codclaveunicocli",
                F.when(F.col("ATRASOMAX_CRNENR_12_100").isNull(), F.col("ATRASOMAX_CRNENR_12"))
                 .otherwise(F.col("ATRASOMAX_CRNENR_12_100")).alias("ATRASOMAX_CRNENR_12"),
                F.when(F.col("SF_NUM_CAL_CPP24_100").isNull(), F.col("SF_NUM_CAL_CPP24"))
                 .otherwise(F.col("SF_NUM_CAL_CPP24_100")).alias("SF_NUM_CAL_CPP24"),
                F.when(F.col("MESES_ACTIVO_SF_BU_MA6_100").isNull(), F.col("MESES_ACTIVO_SF_BU_MA6_0_BASE"))
                 .otherwise(F.col("MESES_ACTIVO_SF_BU_MA6_100")).alias("MESES_ACTIVO_SF_BU_MA6_0"),
                "MONTOADE_ACT_MAX6_S_HIP",
                "UTL_3",
            )
        )

        # 2.11 Variables del dueño (_RL)
        variables_dueno = (
            relacion_dueno_final
            .join(
                carretera_final.select(
                    F.col("codclaveunicocli").alias("CODCLAVEUNICOCLIREL"),
                    F.col("ATRASOMAX_CRNENR_12").alias("ATRASOMAX_CRNENR_12_RL"),
                    F.col("MONTOADE_ACT_MAX6_S_HIP").alias("MONTOADE_ACT_MAX6_S_HIP_RL"),
                    F.col("UTL_3").alias("UTL_3_RL"),
                    F.col("MESES_ACTIVO_SF_BU_MA6_0").alias("MESES_ACTIVO_SF_BU_MA6_0_RL"),
                ),
                "CODCLAVEUNICOCLIREL", "left_outer",
            )
            .select(
                "CODCLAVEUNICOCLI",
                "ATRASOMAX_CRNENR_12_RL",
                "MONTOADE_ACT_MAX6_S_HIP_RL",
                "UTL_3_RL",
                "MESES_ACTIVO_SF_BU_MA6_0_RL",
            )
        )

        # 2.12 Join universo + apppyme + dueño + cliente (para AG)
        resultado = (
            df_universo
            .join(universo_apppyme,   ["CODCLAVEUNICOCLI"], "left_outer")
            .join(variables_dueno,    ["CODCLAVEUNICOCLI"], "left_outer")
            .join(
                carretera_final.select("codclaveunicocli", "MESES_ACTIVO_SF_BU_MA6_0"),
                ["CODCLAVEUNICOCLI"], "left_outer",
            )
        )

        # 2.13 MESES_ACTIVO_SF_BU_MA6_0_AG: PN→cliente, PJ→max(cliente, dueño)
        resultado = (
            resultado
            .withColumn(
                "MESES_ACTIVO_SF_BU_MA6_0_AG_raw",
                operacionesMaxBetweenCols_udf(
                    array(F.col("MESES_ACTIVO_SF_BU_MA6_0"), F.col("MESES_ACTIVO_SF_BU_MA6_0_RL"))
                ),
            )
            .withColumn(
                "MESES_ACTIVO_SF_BU_MA6_0_AG",
                F.when(F.col("TIPPARTYIDENTIFICACION") != "6", F.col("MESES_ACTIVO_SF_BU_MA6_0"))
                 .otherwise(F.col("MESES_ACTIVO_SF_BU_MA6_0_AG_raw"))
                 .cast(IntegerType()),
            )
        )

        # 2.14 Selección y cast final + prefijos de tabla
        resultado = (
            resultado
            .select(
                "codclaveunicocli",
                F.lit(self.codmes).alias("codmes"),
                F.col("EDAD_FIN").cast(IntegerType()).alias("APP_SCORE_APROB_PYME__edad_fin"),
                F.col("ACT_ECO_FIN").alias("APP_SCORE_APROB_PYME__act_eco_fin"),
                F.col("ATRASOMAX_CRNENR_12_RL").cast(IntegerType()).alias("APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl"),
                F.col("MONTOADE_ACT_MAX6_S_HIP_RL").cast(DecimalType(19, 8)).alias("APP_SCORE_APROB_PYME__montoade_act_max6_s_hip_rl"),
                F.col("UTL_3_RL").cast(DecimalType(23, 6)).alias("APP_SCORE_APROB_PYME__utl_3_rl"),
                F.col("MESES_ACTIVO_SF_BU_MA6_0_AG").cast(IntegerType()).alias("APP_SCORE_APROB_PYME__meses_activo_sf_bu_ma6_0_ag"),
            )
        )

        # 2.15 Limpieza de dummies en el resultado del recableo
        cols = resultado.columns
        return resultado.select(*[
            F.when(F.col(c).isin(GLOB_DUMMY_LIST), None).otherwise(F.col(c)).alias(c)
            for c in cols
        ])

    # ------------------------------------------------------------------
    # PASO 3: Lectura de tablas troncales (todas con desfase +1)
    # ------------------------------------------------------------------
    def _read_with_offset(self, table, key_col, cols, filter_extra=None, mes_lectura=None):
        # Mes del que se leen los datos. Por defecto codmes_data (mes anterior);
        # algunas tablas requieren el mes de proceso (self.codmes).
        if mes_lectura is None:
            mes_lectura = self.codmes_data
        select_list = [key_col] + [
            F.col(orig).alias(dest) for orig, dest in cols.items()
        ]
        df = self.spark.table(table).filter(F.col("codmes") == mes_lectura)
        if filter_extra is not None:
            df = df.filter(filter_extra)
        df = df.select(*select_list).distinct()
        return df.withColumn("codmes", F.lit(self.codmes).cast(IntegerType()))

    def _read_mod_demo(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_scoreappbasepymemodulodemografico",
            key_col="codclaveunicocli",
            cols={"ctdmesantiguedadempsunat": "MOD_DEMO__ctdmesantiguedadempsunat"},
        )

    def _read_mod_act(self) -> DataFrame:
        df = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_scoreappbasepymemoduloactivo"
            )
            .filter(F.col("codmes") == self.codmes_data)
            .select(
                "codclaveunicocli",
                F.col("pctratiomtodecdeudapymertmtopasivoprmu3m").alias("MOD_ACT__pctratiomtodecdeudapymertmtopasivoprmu3m"),
                F.col("pctratiomtoopeaprmu6mopecprmu12").alias("MOD_ACT__pctratiomtoopeaprmu6mopecprmu12"),
                F.col("isav_mto_opea_estvta_pym_u6m_rt_max_u12").alias("MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12"),
            )
            .distinct()
        )
        return df.withColumn("codmes", F.lit(self.codmes).cast(IntegerType()))

    def _read_videvar_mora_pond(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_matrizmoraponderadaclientemmgr",
            key_col="codclaveunicocli",
            cols={"mtodeudaclasifriesgofactordsctosolu12": "VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12"},
        )

    def _read_pasivo_evol_sald(self) -> DataFrame:
        """Join por codinternocomputacional."""
        df = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variablepasivoevolucionsaldopyme"
            )
            .filter(F.col("codmes") == self.codmes_data)
            .select(
                "codinternocomputacional",
                F.col("mtoprmincrvariacionmensualprmvigsolu6m")
                 .alias("PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m"),
            )
            .distinct()
        )
        return df.withColumn("codmes", F.lit(self.codmes).cast(IntegerType()))

    def _read_mtx_rcc_otra_deuda(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccotradeuda",
            key_col="codclaveunicocli",
            cols={"rcc_mto_rdv_max_u3m": "MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m"},
            mes_lectura=self.codmes,
        )

    def _read_clasi_exper_cli(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_clasificacionclientenivelexperienciapyme",
            key_col="codclaveunicocli",
            cols={"ctdempleado": "CLASI_EXPER_CLI__ctdempleado"},
            mes_lectura=self.codmes,
        )

    def _read_evol_cli_pym(self) -> DataFrame:
        """Join por codinternocomputacional."""
        df = (
            self.spark.table(
                f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variableactivoevolucionclientepyme"
            )
            .filter(F.col("codmes") == self.codmes)
            .select(
                "codinternocomputacional",
                F.col("ctdmaxdiamorau6m").alias("EVOL_CLI_PYM__ctdmaxdiamorau6m"),
            )
            .distinct()
        )
        return df.withColumn("codmes", F.lit(self.codmes).cast(IntegerType()))

    def _read_mtx_resumen_saldo(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldo",
            key_col="codclaveunicocli",
            cols={"prod_ctd_sld_act_u1m": "MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m"},
            mes_lectura=self.codmes,
        )

    def _read_mtx_resumen_act_pas(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo",
            key_col="codclaveunicocli",
            cols={
                "prod_mto_sld_fim_pas_min_24_24_rt_u24":     "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24",
                "prod_mto_sld_fim_tsav_max_12_12_rt_u12":    "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_12_12_rt_u12",
                "prod_mto_sld_fim_tsav_med_1_6_rt_u6m":      "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_med_1_6_rt_u6m",
                "prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m":  "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m",
                "prod_mto_sld_prm_tsav_med_1_6_rt_u6m":      "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m",
                "prod_pct_pmtsav_pmact_24_24_rt_u24":         "MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24",
                "prod_mto_sld_prm_pas_max_min_12_12_rt_u12":  "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12",
                "prod_mto_sld_fim_act_min_6_6_rt_u6m":        "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_act_min_6_6_rt_u6m",
                "prod_mto_sld_prm_act_max_min_6_6_rt_u6m":    "MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m",
                "prod_mto_sld_fim_tsav_max_min_12_12_rt_u12": "MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12",
            },
            mes_lectura=self.codmes,
        )

    def _read_mtx_mov_abono_pas(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientoabonopasivo",
            key_col="codclaveunicocli",
            cols={"isav_tkt_opea_trnf_dol_max_u3m": "MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m"},
            mes_lectura=self.codmes,
        )

    def _read_mtx_mov_cargo_pas(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientocargopasivo",
            key_col="codclaveunicocli",
            cols={"isav_tkt_opec_pago_srv_prm_u3m": "MTX_MOV_CARGO_PAS__isav_tkt_opec_pago_srv_prm_u3m"},
            mes_lectura=self.codmes,
        )

    def _read_mtx_trx_canal_pago_transf(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanalpagotransferencia",
            key_col="codclaveunicocli",
            cols={
                "can_ctd_tmo_tot_pag_bcp_frq_u6m": "MTX_TRX_CANAL_PAGO_TRANSF__can_ctd_tmo_tot_pag_bcp_frq_u6m",
                "can_mto_tmo_tot_pag_bcp_prm_u6m": "MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m",
            },
            mes_lectura=self.codmes,
        )

    def _read_mtx_rcc_prod(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto",
            key_col="codclaveunicocli",
            cols={"rcc_tip_cond_mor_max_crnor_max_u6m": "MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m"},
            mes_lectura=self.codmes,
        )

    def _read_mtx_trx_canal(self) -> DataFrame:
        return self._read_with_offset(
            table=f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanal",
            key_col="codclaveunicocli",
            cols={"can_tkt_tmo_tot_sol_min_u12": "MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12"},
            mes_lectura=self.codmes,
        )

    # ------------------------------------------------------------------
    # PASO 4: Join de todas las tablas al universo
    # ------------------------------------------------------------------
    def _join_all(self, df_universo, df_app_score, df_mod_demo, df_mod_act,
                  df_videvar_mora_pond, df_pasivo_evol_sald,
                  df_mtx_rcc_otra_deuda, df_clasi_exper_cli,
                  df_evol_cli_pym, df_mtx_resumen_saldo,
                  df_mtx_resumen_act_pas, df_mtx_mov_abono_pas,
                  df_mtx_mov_cargo_pas, df_mtx_trx_canal_pago,
                  df_mtx_rcc_prod, df_mtx_trx_canal) -> DataFrame:

        # Joins por codclaveunicocli
        joins_clave = [
            df_app_score, df_mod_demo, df_mod_act, df_videvar_mora_pond,
            df_mtx_rcc_otra_deuda, df_clasi_exper_cli, df_mtx_resumen_saldo,
            df_mtx_resumen_act_pas, df_mtx_mov_abono_pas, df_mtx_mov_cargo_pas,
            df_mtx_trx_canal_pago, df_mtx_rcc_prod, df_mtx_trx_canal,
        ]
        result = df_universo
        for tabla in joins_clave:
            var_cols = [c for c in tabla.columns if c not in ("codmes", "codclaveunicocli")]
            result = result.join(
                tabla.select("codmes", "codclaveunicocli", *var_cols)
                     .dropDuplicates(["codmes", "codclaveunicocli"]),
                on=["codmes", "codclaveunicocli"],
                how="left",
            )

        # Joins por codinternocomputacional
        for tabla in [df_pasivo_evol_sald, df_evol_cli_pym]:
            var_cols = [c for c in tabla.columns if c not in ("codmes", "codinternocomputacional")]
            result = result.join(
                tabla.select("codmes", "codinternocomputacional", *var_cols)
                     .dropDuplicates(["codmes", "codinternocomputacional"]),
                on=["codmes", "codinternocomputacional"],
                how="left",
            )
        return result

    # ------------------------------------------------------------------
    # PASO 5: Reemplazo de dummies por NULL
    # ------------------------------------------------------------------
    def _replace_dummies(self, df: DataFrame) -> DataFrame:
        cols = df.columns
        return df.select(*[
            F.when(F.col(c).isin(GLOB_DUMMY_LIST), None).otherwise(F.col(c)).alias(c)
            for c in cols
        ])

    # ------------------------------------------------------------------
    # PASO 6: Capping (clip)
    # ------------------------------------------------------------------
    def _apply_capping(self, df: DataFrame) -> DataFrame:
        existing_cols = set(df.columns)
        for col_orig, col_dest, lower, upper in CAPPING_RULES:
            if col_orig not in existing_cols:
                df = df.withColumn(col_dest, F.lit(None).cast("double"))
                continue
            expr = F.col(col_orig)
            if lower is not None:
                expr = F.greatest(F.lit(lower), expr)
            if upper is not None:
                expr = F.least(F.lit(upper), expr)
            df = df.withColumn(
                col_dest,
                F.when(F.col(col_orig).isNotNull(), expr).otherwise(F.lit(None)),
            )
        return df

    # ------------------------------------------------------------------
    # PASO 7: RNG_ACTIVIDAD_ECONOM
    # ------------------------------------------------------------------
    def _build_rng_actividad_econom(self, df: DataFrame) -> DataFrame:
        if "RNG_ACTIVIDAD_ECONOM" in df.columns:
            return df
        act_col = "APP_SCORE_APROB_PYME__act_eco_fin"
        if act_col not in df.columns:
            return df.withColumn("RNG_ACTIVIDAD_ECONOM", F.lit(None).cast("int"))
        categorias_1 = [
            "PESCA", "OTROS", "SERVICIOS", "ENERGIA", "CONSTRUCCION",
            "ADM_PUBLICA", "ACT INMOB, EMP Y DE ALQ", "INDUST_MANUFACT",
            "COMERCIO", "HOGAR", "SALUD",
        ]
        return df.withColumn(
            "RNG_ACTIVIDAD_ECONOM",
            F.when(F.col(act_col).isNull(), 1)
             .when(F.col(act_col).isin(categorias_1), 1)
             .otherwise(0),
        )

    # ------------------------------------------------------------------
    # PASO 8: Selección de features
    # ------------------------------------------------------------------
    def _select_features(self, df: DataFrame) -> DataFrame:
        key_cols = ["codmes", "codclaveunicocli", "codclavepartycli", "codinternocomputacional"]
        existing = set(df.columns)
        select_list = [F.col(c) for c in key_cols if c in existing]
        for feat in self.feature_names:
            if feat in existing:
                select_list.append(F.col(feat))
            else:
                select_list.append(F.lit(None).cast("double").alias(feat))
        return df.select(*select_list)
    
    # ------------------------------------------------------------------
    # PASO 8.5: Normalización de tipos a DoubleType
    # ------------------------------------------------------------------
    def _cast_features_to_double(self, df: DataFrame) -> DataFrame:
        """
        Normaliza columnas a DoubleType para alinear el schema con la tabla
        de referencia. codmes se mantiene Integer; los identificadores string
        y act_eco_fin (string en el target) no se castean.
        """
        keep_as_is = {
            "codclaveunicocli", "codclavepartycli", "codinternocomputacional",
            "APP_SCORE_APROB_PYME__act_eco_fin",   # string en el target
        }
        select_list = []
        for c in df.columns:
            if c in keep_as_is:
                select_list.append(F.col(c))
            else:
                select_list.append(F.col(c).cast(DoubleType()).alias(c))
        return df.select(*select_list)

    # ------------------------------------------------------------------
    # PASO 9: Escritura en Unity Catalog
    # ------------------------------------------------------------------
    def _write_table_master(self, df: DataFrame) -> None:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"codmes = {self.codmes}")
            .partitionBy("codmes")
            .saveAsTable(self.table_master)
        )