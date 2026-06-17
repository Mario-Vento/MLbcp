# universo_implementacion.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType
from pyspark.sql.window import Window
from datetime import datetime
from dateutil.relativedelta import relativedelta

from utils.data_preparation.helpers import add_codmes_spark, operacionesMaxBetweenCols_udf

class UniversoImplementacion:
    """
    Clase que replica fielmente el notebook de Sherly para construir la tabla maestra
    con todas las variables necesarias (aprox. 109) para el modelo PD BHV PYME.
    Todos los meses se calculan dinámicamente a partir del parámetro 'codmes'.
    """

    def __init__(self,
                 spark: SparkSession,
                 codmes: int,                     # mes de proceso, ej. 202604
                 src_catalog: str = "catalog_lhcl_prod_bcp",
                 sink_catalog: str = None,
                 sink_schema: str = None,
                 sink_table: str = None):
        """
        Constructor.
        :param spark: SparkSession activa.
        :param codmes: Mes de proceso (ej. 202604). Todas las fechas se derivan de este mes.
        :param src_catalog: Catálogo fuente (por defecto catalog_lhcl_prod_bcp).
        :param sink_catalog: Catálogo destino (Unity Catalog).
        :param sink_schema: Esquema destino.
        :param sink_table: Nombre de la tabla destino (tabla maestra).
        """
        self.spark = spark
        self.codmes_proceso = codmes
        self.codmes_data = codmes                     # mes actual
        self.codmes_data_1 = self._previous_month(codmes)   # mes anterior (desfase de 1 mes)

        # Meses relativos usados en ventanas del módulo activo
        self.codmes_cuatro = self._previous_month(self._previous_month(self._previous_month(self.codmes_data_1)))  # -3 meses
        self.codmes_tres = self._previous_month(self._previous_month(self.codmes_data_1))                         # -2 meses

        # Catálogos y esquemas
        self.src_catalog = src_catalog
        self.sink_catalog = sink_catalog
        self.sink_schema = sink_schema
        self.sink_table = sink_table
        self.full_sink_table = f"{sink_catalog}.{sink_schema}.{sink_table}" if sink_catalog and sink_schema and sink_table else None

        # Lista de valores dummy a reemplazar por NULL (exactamente igual al notebook)
        self.GLOB_DUMMY_LIST = [
            111111111, -111111111, 222222222, -222222222, 333333333, -333333333,
            444444444, 555555555, 666666666, 777777777, -99, -999,
            222222222.2222, -333333333.3333, -99, -333333333.3333, 444444.4444,
            555555.5555, 666666.6666, 777777.7777, 111111.1111, -111111.1111, 222222.2222,
            -222222.2222, 333333.3333, -333333.3333, None
        ]

        # Configuración de capping (valores exactos del notebook)
        self.capping_config = {
            'MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m': (None, 54058.633),
            'CLASI_EXPER_CLI__ctdempleado': (0, 203.0),
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_pas_max_min_12_12_rt_u12': (None, 88040.516),
            'EVOL_CLI_PYM__ctdmaxdiamorau6m': (0, 59.0),
            'MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m': (None, 13.0),
            'MOD_DEMO__ctdmesantiguedadempsunat': (None, 396.0),
            'APP_SCORE_APROB_PYME__utl_3_rl': (0, 261.9096),
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24': (None, 0.8768666),
            'MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12': (None, 20916.992),
            'MTX_RESUMEN_ACT_PAS__prod_pct_pmtsav_pmact_24_24_rt_u24': (None, 19.579279),
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_max_min_6_6_rt_u6m': (None, 4771.6816),
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_tsav_med_1_6_rt_u6m': (None, 4.0419364),
            'MOD_ACT__pctratiomtodecdeudapymermtopasivoprmu3m': (None, 480.26755),
            'APP_SCORE_APROB_PYME__edad_fin': (25, 75),
            'PASIVO_EVOL_SALD_PYM__mtoprmincrvariacionmensualprmvigsolu6m': (None, 211663.56),
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_prm_act_max_min_6_6_rt_u6m': (None, 386732.84),
            'MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_min_u9m': (None, 46430.434),
            'MTX_RCC_PROD__rcc_tip_cond_mor_max_crnor_max_u6m': (None, 469.555),
            'VIDEVAR_MTX_MORA_POND_CLI_MMGR__mtodeudaclasifriesgofactordsctosolu12': (None, 709515.0),
            'MOD_ACT__isav_mto_opea_estvta_pym_u6m_rt_max_u12': (0, 1),
            'MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_min_12_12_rt_u12': (None, 372685.25),
            'MTX_RESUMEN_SALDO__prod_antmin_cvi_prm_u12': (None, 318.5),
            'MTX_RESUMEN_SALDO__prod_mto_sld_pai_g6m': (None, 58.04616),
            'MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m': (None, 379400.0),
            'MOD_ACT__pctratiomtoopeaprmu6mopecprmu12': (None, 2.637802),
            'MTX_TRX_POS__pos_tkt_trx_td_prm_u6m': (0, 6189.8115),
            'MTX_TRX_CANAL_PAGO_TRANSF__can_mto_tmo_tot_pag_bcp_prm_u6m': (None, 112905.56),
            'MTX_RESUMEN_ACT_PAS__prod_pct_fm_pmpas_med_12_12_rt_u12': (None, 3.8366303),
            'MTX_TRX_POS__pos_tkt_trx_com_sol_prm_p6m': (0, 7833.3335),
            'MTX_TRX_CANAL_PAGO_TRANSF__can_tkt_tmo_tot_pag_srv_sol_g6m': (None, 133.00755),
            'APP_SCORE_APROB_PYME__atrasomax_crnenr_12_rl': (None, 46.0),
        }

    @staticmethod
    def _previous_month(period: int) -> int:
        """Devuelve el mes anterior en formato YYYYMM."""
        year = period // 100
        month = period % 100
        if month == 1:
            return (year - 1) * 100 + 12
        else:
            return year * 100 + (month - 1)

    # ======================== PASOS DE CONSTRUCCIÓN ========================

    def _build_universe(self):
        """Universo de clientes (bd_202509_202601)"""
        portafolio = f"{self.src_catalog}.bcp_ddv_adrmmgr_seginfobasesgenerales_vu.hm_portafoliocredito"
        df = self.spark.table(portafolio) \
            .select("codmes", "codclaveunicocli", "codinternocomputacional", "codclavepartycli",
                    "codproductonivel0rbm", "ctdmesmaduracion") \
            .filter(F.trim(F.col("codproductonivel0rbm")).isin('PYME REVOLVENTE', 'PYME NO REVOLVENTE')) \
            .filter(F.col("codmes") >= 202405) \
            .filter(F.col("codclaveunicocli").isNotNull()) \
            .filter(F.col("ctdmesmaduracion") > 0) \
            .distinct()
        universe = df.groupBy("codclaveunicocli", "codmes") \
            .agg(F.max("codinternocomputacional").alias("codinternocomputacional"),
                 F.max("codclavepartycli").alias("codclavepartycli"))
        return universe

    def _get_cliente_prospecto(self):
        tabla = f"{self.src_catalog}.bcp_ddv_rbmbcapym_modelogestion_vu.hm_clienteprospectopyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "CODCLAVEUNICOCLI", "tippartyidentificacion")
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return df

    def _get_relacionados(self, cliente_prospecto):
        tabla = f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_relacionadoapppyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "CODCLAVEUNICOCIL", "CODCLAVEUNICOCILREL",
                    "DESTIPREL", "PCTPARTICIPACIONREL")
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        df = df.join(cliente_prospecto, ["codmes", "CODCLAVEUNICOCIL"], "left_outer")
        # Dueños
        duenios = df.filter(F.col("DESTIPREL").isin('DUENIO', 'DUENIO P.N.')) \
            .select("codmes", "CODCLAVEUNICOCIL", "CODCLAVEUNICOCILREL",
                    F.when(F.col("tippartyidentificacion") == '6', 'J').otherwise('N').alias("FLGTIPPER"))
        duenio_unico = duenios.groupBy("codmes", "CODCLAVEUNICOCIL") \
            .agg(F.max(F.when(F.col('FLGTIPPER') == 'N', F.col('CODCLAVEUNICOCIL'))
                       .otherwise(F.col('CODCLAVEUNICOCILREL'))).alias('CODCLAVEUNICOCILREL'))
        # Flag
        relacion_con_flag = df.alias("A").join(duenio_unico.alias("B"),
            (F.col("A.CODCLAVEUNICOCLI") == F.col("B.CODCLAVEUNICOCLI")) &
            (F.col("A.CODCLAVEUNICOCLIREL") == F.col("B.CODCLAVEUNICOCLIREL")) &
            (F.col("A.codmes") == F.col("B.codmes")), "left_outer") \
            .select(F.col("A.codmes"), F.col("A.CODCLAVEUNICOCLI"), F.col("A.CODCLAVEUNICOCLIREL"),
                    F.col("A.DESTIPREL"), F.col("A.PCTPARTICIPACIONREL"),
                    F.when(F.col("A.DESTIPREL").isin('DUENIO', 'DUENIO_P_N'),
                           F.when(F.col("B.CODCLAVEUNICOCLI").isNull(), 0).otherwise(1))
                     .otherwise(0).alias("FLGRLDUENIO"))
        relacion_dueno_final = relacion_con_flag.filter(F.col('FLGRLDUENIO') == 1) \
            .select('codmes', 'CODCLAVEUNICOCLI', 'CODCLAVEUNICOCLIREL')
        return relacion_dueno_final

    def _get_universo_apppyme(self):
        tabla = f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_clientepreaprobacionapppyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "CODCLAVEUNICOCLI",
                    F.col("NUMEDAD").cast(IntegerType()).alias("EDAD_FIN"),
                    F.col("DESSECCIONECONOMICAAPPPYME").alias("ACT_ECO_FIN"),
                    "TIPPARTYIDENTIFICACION")
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return df

    def _get_carretera_variables(self):
        """Construye carretera con materialidad (ATRASOMAX, SF_NUM_CAL, MESES_ACTIVO, etc.)"""
        # RCC producto
        rcc = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto"
        df_rcc = self.spark.table(rcc) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
                    F.col("RCC_TIP_COND_MOR_MAX_CRNNR_MAX_U12").alias("ATRASOMAX_CRNNR_12"),
                    F.col("RCC_MTO_ADE_ACT_SHIP_RT_U6M").alias("MONTOADE_ACT_MAX6_S_HIP"),
                    F.col("RCC_PCT_UTL_3_RT_U3M").alias("UTL_3"),
                    F.col("RCC_CTD_SF_CAL_CPP_FRQ_0_U24").alias("SF_NUM_CAL_CPP24"))
        df_rcc = df_rcc.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")

        # Resumen saldo activo/pasivo
        resumen = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo"
        df_res = self.spark.table(resumen) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
                    F.col("PROD_MTO_SLD_PRM_TSAV_FRQ_100_U24").alias("MESES_PMSAVMF_24_100"),
                    F.col("PROD_CTD_MES_PAS_ACT_MAX_V2_1000_U12").alias("MESES_PASIVO_ACTIVO_12_1000"))
        df_res = df_res.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")

        # Materialidad
        mat = f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_variablerccmaterialidadapppyme"
        df_mat = self.spark.table(mat) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
                    F.col("RCC_TIP_COND_MOR_MAX_CRNNR_100_MAX_U12").alias("ATRASOMAX_CRNNR_12_100"),
                    F.col("RCC_CTD_SF_CAL_CPP_FRO_100_U24").alias("SF_NUM_CAL_CPP24_100"),
                    F.col("RCC_CTD_MES_ACT_SF_BUEN_MAL_100_U6M").alias("MESES_ACTIVO_SF_BU_MA6_100"))
        df_mat = df_mat.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")

        # Full outer joins
        carretera = df_rcc.join(df_res, ["codmes", "codclaveunicocli"], "fullouter")
        carretera = carretera.join(df_mat, ["codmes", "codclaveunicocli"], "fullouter")

        carretera = carretera.select(
            "codmes", "codclaveunicocli",
            F.when(F.col("ATRASOMAX_CRNNR_12_100").isNull(), F.col("ATRASOMAX_CRNNR_12"))
             .otherwise(F.col("ATRASOMAX_CRNNR_12_100")).alias("ATRASOMAX_CRNNR_12"),
            F.when(F.col("SF_NUM_CAL_CPP24_100").isNull(), F.col("SF_NUM_CAL_CPP24"))
             .otherwise(F.col("SF_NUM_CAL_CPP24_100")).alias("SF_NUM_CAL_CPP24"),
            F.col("MONTOADE_ACT_MAX6_S_HIP"),
            F.col("UTL_3"),
            F.col("MESES_PMSAVMF_24_100"),
            F.col("MESES_ACTIVO_SF_BU_MA6_100").alias("MESES_ACTIVO_SF_BU_MA6_0_MAT")
        )

        # Obtener base para MESES_ACTIVO
        rcc_base = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto"
        df_base = self.spark.table(rcc_base) \
            .filter(F.col("codmes") >= 202404) \
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
                    F.col("RCC_CTD_MES_ACT_SF_BUEN_MAL_0_UGM").alias("MESES_ACTIVO_SF_BU_MA6_0_BASE"))
        df_base = df_base.withColumn('codmes', add_codmes_spark("CODMES_0", 1)).drop("CODMES_0")
        carretera = carretera.join(df_base, ["codmes", "codclaveunicocli"], "left_outer")
        carretera = carretera.withColumn(
            "MESES_ACTIVO_SF_BU_MA6_0",
            F.when(F.col("MESES_ACTIVO_SF_BU_MA6_0_MAT").isNull(), F.col("MESES_ACTIVO_SF_BU_MA6_0_BASE"))
             .otherwise(F.col("MESES_ACTIVO_SF_BU_MA6_0_MAT"))
        ).drop("MESES_ACTIVO_SF_BU_MA6_0_MAT", "MESES_ACTIVO_SF_BU_MA6_0_BASE")
        return carretera

    def _build_variables_dueno(self, relacion_dueno_final, carretera):
        df_dueno = relacion_dueno_final.join(
            carretera.select(
                "codmes",
                F.col("codclaveunicocli").alias("CODCLAVEUNICOCLIREL"),
                F.col("ATRASOMAX_CRNNR_12").alias("ATRASOMAX_CRNNR_12_RL"),
                F.col("MONTOADE_ACT_MAX6_S_HIP").alias("MONTOADE_ACT_MAX6_S_HIP_RL"),
                F.col("UTL_3").alias("UTL_3_RL"),
                F.col("SF_NUM_CAL_CPP24").alias("SF_NUM_CAL_CPP24_RL"),
                F.col("MESES_ACTIVO_SF_BU_MA6_0").alias("MESES_ACTIVO_SF_BU_MA6_0_RL"),
                F.col("MESES_PMSAVMF_24_100").alias("MESES_PMSAVMF_24_100_RL")
            ),
            ["CODCLAVEUNICOCLIREL", "codmes"], "left_outer"
        ).select("codmes", "CODCLAVEUNICOCLI",
                 "ATRASOMAX_CRNNR_12_RL", "MONTOADE_ACT_MAX6_S_HIP_RL",
                 "UTL_3_RL", "SF_NUM_CAL_CPP24_RL", "MESES_ACTIVO_SF_BU_MA6_0_RL",
                 "MESES_PMSAVMF_24_100_RL")
        return df_dueno

    def _build_variables_cliente(self, carretera):
        return carretera.select("codmes", "codclaveunicocli", "SF_NUM_CAL_CPP24", "MESES_ACTIVO_SF_BU_MA6_0")

    def _build_mod_demo(self, universe_df):
        tabla = f"{self.src_catalog}.bcp_ddv_rbmbcapym_modelogestion_vu.hm_contribuyentesunatpyme"
        df = self.spark.table(tabla) \
            .select(F.col("CODMES").alias("CODMES_0"), F.col("CODCLAVEUNICOCLI"),
                    F.col("CTDMESANTIGUEDADEMP").cast(IntegerType()).alias("ctdmesantiguedadempsunat"))
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return universe_df.join(df, ["CODCLAVEUNICOCLI", "codmes"], "left_outer")

    def _build_mod_activo(self, base_universe):
        """Implementación completa del módulo activo (transacciones, variación de deuda) usando fechas dinámicas."""
        # Usar las fechas calculadas en el constructor
        codmescuatro = self.codmes_cuatro
        codmestres = self.codmes_tres
        codmes_data_1 = self.codmes_data_1
        codmes_data = self.codmes_data

        # Universo segmento (clientes con flgactmay100may6u12 = 1 en codmes_data_1)
        segmento = f"{self.src_catalog}.bcp_ddv_rbmbcapym_segmentacionpyme_vu.hm_segmentoinformativopyme"
        universo_segmento = self.spark.table(segmento) \
            .filter((F.col("codmes") == codmes_data_1) & (F.col("flgactmay100may6u12") == 1)) \
            .select("CODCLAVEUNICOCLI").distinct() \
            .withColumn("en_universo", F.lit(1))

        # Transacciones (isav_mto_opea_estvta_pym_u6m_rt_max_u12 y pctratmtoopeapermu6mopecprmu12)
        transacciones_tabla = f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_matrizbasepasivoclienteapppyme"
        transacciones = self.spark.table(transacciones_tabla) \
            .filter(F.col("codmes") == codmes_data_1) \
            .select(
                F.col("CODCLAVEPARTYCLI"),
                (F.col("ISAV_MTO_OPEA_ESTVTA_PYM_PRM_U6M") / F.col("ISAV_MTO_OPEA_ESTVTA_PYM_MAX_U12"))
                    .alias("isav_mto_opea_estvta_pym_u6m_rt_max_u12"),
                (F.col("ISAV_MTO_OPEA_ESTVTA_PYM_U6M") / F.col("ISAV_MTO_OPEC_ESTVTA_PYM_PRM_U12"))
                    .alias("pctratmtoopeapermu6mopecprmu12")
            )

        # Variación de deuda (dec_var_MONTO_meanPrev3)
        portafolio = f"{self.src_catalog}.bcp_ddv_adrmmgr_seginfobasesgenerales_vu.hm_portafoliocredito"
        df_port = self.spark.table(portafolio) \
            .filter((F.col("CODMES").between(codmescuatro, codmes_data_1)) &
                    (F.trim(F.col("FLGCTAVALIDA")) == "1") &
                    (F.col("TIPESTADOCTA").isin("A", "AC", "D"))) \
            .select("CODCLAVEPARTYCLI", "MTOSALDOCAPITALSOL", "CODMES")

        port_agrup = df_port.groupBy("CODCLAVEPARTYCLI", "CODMES") \
            .agg(F.sum("MTOSALDOCAPITALSOL").alias("mtosaldocapitalsol_tmp"))

        # Generar lista de meses
        def generar_meses(mes_inicio, mes_fin):
            meses = []
            fecha_fin = datetime.strptime(str(mes_fin), "%Y%m")
            fecha_actual = datetime.strptime(str(mes_inicio), "%Y%m")
            while fecha_actual <= fecha_fin:
                meses.append(fecha_actual.strftime("%Y%m"))
                fecha_actual += relativedelta(months=1)
            return meses
        lista_meses = generar_meses(codmescuatro, codmes_data_1)
        df_meses = self.spark.createDataFrame(lista_meses, StringType()).select(F.col("value").alias("CODMES"))
        partys = port_agrup.select("CODCLAVEPARTYCLI").distinct()
        cross = partys.crossJoin(F.broadcast(df_meses))
        port_completo = port_agrup.join(cross, on=["CODMES", "CODCLAVEPARTYCLI"], how="right_outer")

        w = Window.partitionBy("CODCLAVEPARTYCLI").orderBy(F.col("CODMES").desc())
        port_var = port_completo.withColumn(
            "mtosaldocapitalsol_tmp_prev", F.lead("mtosaldocapitalsol_tmp", 1).over(w).cast("double")
        ).withColumn(
            "mtosaldocapitalsol_tmp_VAR_DEC",
            F.when(
                F.col("mtosaldocapitalsol_tmp").isNull() & F.col("mtosaldocapitalsol_tmp_prev").isNull(),
                F.lit(None)
            ).otherwise(
                F.when(
                    F.coalesce(F.col("mtosaldocapitalsol_tmp"), F.lit(0)) -
                    F.coalesce(F.col("mtosaldocapitalsol_tmp_prev"), F.lit(0)) > 0,
                    F.lit(0)
                ).otherwise(
                    F.abs(
                        F.coalesce(F.col("mtosaldocapitalsol_tmp"), F.lit(0)) -
                        F.coalesce(F.col("mtosaldocapitalsol_tmp_prev"), F.lit(0))
                    )
                )
            )
        )
        dec_var_prm3 = port_var.filter(
            F.col("CODMES").between(str(codmestres), str(codmes_data_1))
        ).groupBy("CODCLAVEPARTYCLI").agg(
            F.avg("mtosaldocapitalsol_tmp_VAR_DEC").alias("dec_var_MONTO_meanPrev3")
        )

        # Pasivos
        pasivos_tabla = f"{self.src_catalog}.bcp_ddv_rbmbcapym_apppyme_vu.hm_matrizbaseresumensaldoapppyme"
        pasivos = self.spark.table(pasivos_tabla) \
            .filter(F.col("codmes") == codmes_data_1) \
            .select("CODCLAVEUNICOCLI", "PROD_MTO_SLD_PRM_VIG_PAS_AHO_CTEORD_PRM_U3M")

        # Unir con universo base
        base_filtrada = base_universe.filter(F.col("codmes") == codmes_data)
        bd_con_flag = base_filtrada.join(universo_segmento, "CODCLAVEUNICOCLI", "left")
        resultado = bd_con_flag \
            .join(transacciones, "CODCLAVEPARTYCLI", "left") \
            .join(dec_var_prm3, "CODCLAVEPARTYCLI", "left") \
            .join(pasivos, "CODCLAVEUNICOCLI", "left")
        resultado = resultado.withColumn(
            "pctratiotodecdeudapymermtopasivoprmu3m_raw",
            F.col("dec_var_MONTO_meanPrev3") / F.col("PROD_MTO_SLD_PRM_VIG_PAS_AHO_CTEORD_PRM_U3M")
        )
        # Aplicar regla: si no está en universo, NULL
        resultado = resultado.withColumn(
            "isav_mto_opea_estvta_pym_u6m_rt_max_u12",
            F.when(F.col("en_universo") == 1, F.col("isav_mto_opea_estvta_pym_u6m_rt_max_u12"))
             .otherwise(F.lit(None))
        ).withColumn(
            "pctratiomtodecdeudapymermtopasivoprmu3m",
            F.when(F.col("en_universo") == 1, F.col("pctratiotodecdeudapymermtopasivoprmu3m_raw"))
             .otherwise(F.lit(None))
        ).withColumn(
            "pctratiomtoopeaprmu6mopecprmu12",
            F.when(F.col("en_universo") == 1, F.col("pctratmtoopeapermu6mopecprmu12"))
             .otherwise(F.lit(None))
        )
        mod_act = resultado.select(
            "codmes", "codclaveunicocli",
            F.col("isav_mto_opea_estvta_pym_u6m_rt_max_u12").cast(DecimalType(19,8)).alias("isav_mto_opea_estvta_pym_u6m_rt_max_u12"),
            F.col("pctratiomtodecdeudapymermtopasivoprmu3m").cast(DecimalType(19,8)).alias("pctratiomtodecdeudapymermtopasivoprmu3m"),
            F.col("pctratiomtoopeaprmu6mopecprmu12").cast(DecimalType(19,8)).alias("pctratiomtoopeaprmu6mopecprmu12")
        )
        return mod_act

    # ======================== TABLAS CON DESFASE DE 1 MES ========================
    def _build_consol_deud_relat(self):
        tabla = f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_consolidadovariabledeudavalorrelativizado"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data_1) \
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
                    F.col("pctrelprmmtoprisolu12m5").alias("CONSOL_CALCUL_DEUD_RELAT_pctrelprmmtoprisolu12m5")) \
            .distinct()
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return df

    def _build_videvar_mora_pond(self):
        tabla = f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_matrizmoraponderadaclientemmgr"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data_1) \
            .select(F.col("CODMES").alias("CODMES_0"), "codclaveunicocli",
                    F.col("mtodeudaclasifriesgofactordsctosolu12").alias("VIDEVAR_MTX_MORA_POND_CLI_MMGR_mtodeudaclasifriesgofactordsctosolu12")) \
            .distinct()
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return df

    def _build_pasivo_evol_sald_pym(self):
        tabla = f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variablepasivoevolucionalsaldopyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data_1) \
            .select(F.col("CODMES").alias("CODMES_0"), "codinternocomputacional",
                    F.col("mtoprmvariacionmensualprmvigsolu24m").alias("PASIVO_EVOL_SALD_PYM__mtoprmvariacionmensualprmvigsolu24m"),
                    F.col("mtoprmvariacionmensualprmvigsolu6m").alias("PASIVO_EVOL_SALD_PYM__mtoprmvariacionmensualprmvigsolu6m"),
                    F.col("pctprmdecvariacionmensualvigsolu3mu6m").alias("PASIVO_EVOL_SALD_PYM__pctprmdecvariacionmensualvigsolu3mu6m"),
                    F.col("pctprmincrvariacionmensualprmvigsolu3mu6m").alias("PASIVO_EVOL_SALD_PYM__pctprmincrvariacionmensualprmvigsolu3mu6m"),
                    F.col("pctprmincrvariacionmensualvigsolu6mu12m").alias("PASIVO_EVOL_SALD_PYM__pctprmincrvariacionmensualvigsolu6mu12m"),
                    F.col("mtoprmvaricionmensualprmvigsolu6m").alias("PASIVO_EVOL_SALD_PYM__mtoprmvaricionmensualprmvigsolu6m"),
                    F.col("mtovaricionmensualprmvigsol").alias("PASIVO_EVOL_SALD_PYM__mtovaricionmensualprmvigsol")) \
            .distinct()
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return df

    def _build_evol_comp_trx_pym(self):
        tabla = f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_evolucioncomportamientotransaccionalpyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data_1) \
            .select(F.col("CODMES").alias("CODMES_0"), "codinternocomputacional",
                    F.col("mtomaxtotalnetorxu6m").alias("EVOL_COMP_TRX_PYM_mtomaxtotalnetorxu6m"),
                    F.col("pctratiomtototalabonoprmu3mu6m").alias("EVOL_COMP_TRX_PYM_pctratiomtototalabonoprmu3mu6m"),
                    F.col("pctratiomtototalabonoprmu6mu12m").alias("EVOL_COMP_TRX_PYM_pctratiomtototalabonoprmu6mu12m"),
                    F.col("pctratiomnutotalcargoprmu6mu12m").alias("EVOL_COMP_TRX_PYM_pctratiomnutotalcargoprmu6mu12m")) \
            .distinct()
        df = df.withColumn('codmes', add_codmes_spark('CODMES_0', 1)).drop("CODMES_0")
        return df

    # ======================== TABLAS SIN DESFASE ========================
    def _build_mtx_rcc_otra_deuda(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccotradeuda"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli", F.col("rcc_mto_rdv_max_u3m").alias("MTX_RCC_OTRA_DEUDA__rcc_mto_rdv_max_u3m")) \
            .distinct()
        return df

    def _build_clasi_exper_cli(self):
        tabla = f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_clasificacionclientenivelexperienciapyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli", F.col("ctdempleado").alias("CLASI_EXPER_CLI__ctdempleado")) \
            .distinct()
        return df

    def _build_evol_cli_pym(self):
        tabla = f"{self.src_catalog}.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_variableactivoevolucionclientepyme"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codinternocomputacional",
                    F.col("ctddiamoramay0u6m").alias("EVOL_CLI_PYM__ctddiamoramay0u6m"),
                    F.col("ctdmaxdiamorau6m").alias("EVOL_CLI_PYM__ctdmaxdiamorau6m")) \
            .distinct()
        return df

    def _build_mtx_resumen_saldo(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldo"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("prod_antmin_cvi_prm_u12").alias("MTX_RESUMEN_SALDO__prod_antmin_cvi_prm_u12"),
                    F.col("prod_ctd_pai_max_u6m").alias("MTX_RESUMEN_SALDO__prod_ctd_pai_max_u6m"),
                    F.col("prod_ctd_sld_act_u1m").alias("MTX_RESUMEN_SALDO__prod_ctd_sld_act_u1m"),
                    F.col("prod_mto_sld_pai_g6m").alias("MTX_RESUMEN_SALDO__prod_mto_sld_pai_g6m")) \
            .distinct()
        return df

    def _build_mtx_resumen_act_pas(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("prod_ctd_mes_pas_act_max_v2_0_u12").alias("MTX_RESUMEN_ACT_PAS__prod_ctd_mes_pas_act_max_v2_0_u12"),
                    F.col("prod_mto_sld_fim_pas_min_24_24_rt_u24").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_pas_min_24_24_rt_u24"),
                    F.col("prod_mto_sld_fim_tsav_max_12_12_rt_u12").alias("MTX_RESUMEN_ACT_PAS__prod_mto_sld_fim_tsav_max_12_12_rt_u12"),
                    F.col("prod_pct_fm_pmpas_med_12_12_rt_u12").alias("MTX_RESUMEN_ACT_PAS__prod_pct_fm_pmpas_med_12_12_rt_u12"),
                    F.col("prod_pct_fm_pmpas_med_24_24_rt_u24").alias("MTX_RESUMEN_ACT_PAS__prod_pct_fm_pmpas_med_24_24_rt_u24"),
                    F.col("prod_pct_fm_pmpas_med_6_6_rt_u6").alias("MTX_RESUMEN_ACT_PAS__prod_pct_fm_pmpas_med_6_6_rt_u6"),
                    F.col("prod_pct_fm_pmpas_med_3_3_rt_u3").alias("MTX_RESUMEN_ACT_PAS__prod_pct_fm_pmpas_med_3_3_rt_u3"),
                    F.col("prod_pct_fm_pmpas_med_1_1_rt_u1").alias("MTX_RESUMEN_ACT_PAS__prod_pct_fm_pmpas_med_1_1_rt_u1")) \
            .distinct()
        return df

    def _build_mtx_mov_abono_pas(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientoabonopasivo"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("isav_flg_opea_nhab_ncts_dol_max_u12").alias("MTX_MOV_ABONO_PAS__isav_flg_opea_nhab_ncts_dol_max_u12"),
                    F.col("isav_tkt_opea_trnf_dol_max_u3m").alias("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_dol_max_u3m"),
                    F.col("isav_tkt_opea_trnf_min_u9m").alias("MTX_MOV_ABONO_PAS__isav_tkt_opea_trnf_min_u9m")) \
            .distinct()
        return df

    def _build_mtx_mov_cargo_pas(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientocargopasivo"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("isav_mto_opec_retr_g6m").alias("MTX_MOV_CARGO_PAS__isav_mto_opec_retr_g6m"),
                    F.col("isav_tkt_opec_pago_srv_prm_u3m").alias("MTX_MOV_CARGO_PAS__isav_tkt_opec_pago_srv_prm_u3m")) \
            .distinct()
        return df

    def _build_mtx_mov_pas(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizmovimientopasivo"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("isav_tkt_opec_sav_min_u9m").alias("MTX_MOV_PAS__isav_tkt_opec_sav_min_u9m")) \
            .distinct()
        return df

    def _build_mtx_trx_canal_pago_transf(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanalpagotransferencia"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("can_ctd_tmo_tot_pag_bcp_frq_u6m").alias("MTX_TRX_CANAL_PAGO_TRANSF_can_ctd_tmo_tot_pag_bcp_frq_u6m"),
                    F.col("can_mto_tmo_tot_pag_bcp_prm_u6m").alias("MTX_TRX_CANAL_PAGO_TRANSF_can_mto_tmo_tot_pag_bcp_prm_u6m"),
                    F.col("can_tkt_tmo_tot_pag_srv_g6m").alias("MTX_TRX_CANAL_PAGO_TRANSF_can_tkt_tmo_tot_pag_srv_g6m"),
                    F.col("can_tkt_tmo_tot_pag_srv_sol_g6m").alias("MTX_TRX_CANAL_PAGO_TRANSF_can_tkt_tmo_tot_pag_srv_sol_g6m")) \
            .distinct()
        return df

    def _build_mtx_trx_pos(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccionpos"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("pos_tkt_trx_com_sol_prm_p6m").alias("MTX_TRX_POS_pos_tkt_trx_com_sol_prm_p6m"),
                    F.col("pos_tkt_trx_td_prm_u6m").alias("MTX_TRX_POS_pos_tkt_trx_td_prm_u6m")) \
            .distinct()
        return df

    def _build_mtx_rcc_prod(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("rcc_ctd_mes_act_sf_buen_1000_frq_v_u12").alias("MTX_RCC_PROD_rcc_ctd_mes_act_sf_buen_1000_frq_v_u12"),
                    F.col("rcc_pct_deu_cpp_max_u12").alias("MTX_RCC_PROD_rcc_pct_deu_cpp_max_u12"),
                    F.col("rcc_pct_deu_defc_max_u6m").alias("MTX_RCC_PROD_rcc_pct_deu_defc_max_u6m"),
                    F.col("rcc_tip_cond_mor_max_crnor_max_u6m").alias("MTX_RCC_PROD_rcc_tip_cond_mor_max_crnor_max_u6m")) \
            .distinct()
        return df

    def _build_bd_mtx_trx_canal(self):
        tabla = f"{self.src_catalog}.bcp_ddv_matrizvariables_vu.hm_matriztransaccioncanal"
        df = self.spark.table(tabla) \
            .filter(F.col("codmes") == self.codmes_data) \
            .select("codmes", "codclaveunicocli",
                    F.col("can_tkt_tmo_tot_ret_sol_max_u6m").alias("MTX_TRX_CANAL__can_tkt_tmo_tot_ret_sol_max_u6m"),
                    F.col("can_tkt_tmo_tot_sol_min_u12").alias("MTX_TRX_CANAL__can_tkt_tmo_tot_sol_min_u12")) \
            .distinct()
        return df

    # ======================== MÉTODO PRINCIPAL ========================

    def execute(self):
        """Orquesta la construcción completa de la tabla maestra."""
        # 1. Universo base
        universe = self._build_universe()
        cliente_prospecto = self._get_cliente_prospecto()
        relacion_dueno = self._get_relacionados(cliente_prospecto)
        universo_app = self._get_universo_apppyme()
        carretera = self._get_carretera_variables()
        vars_dueno = self._build_variables_dueno(relacion_dueno, carretera)
        vars_cliente = self._build_variables_cliente(carretera)

        # Unir partes principales (APP_SCORE_APROB_PYME ya viene de universos + dueno + cliente)
        df = universe \
            .join(universo_app, ["codmes", "CODCLAVEUNICOCLI"], "left_outer") \
            .join(vars_dueno, ["codmes", "CODCLAVEUNICOCLI"], "left_outer") \
            .join(vars_cliente, ["codmes", "CODCLAVEUNICOCLI"], "left_outer")

        # Módulo demográfico
        df = self._build_mod_demo(df)

        # Módulo activo
        mod_act = self._build_mod_activo(universe)
        df = df.join(mod_act, ["codmes", "codclaveunicocli"], "left_outer")

        # Tablas con desfase
        consol = self._build_consol_deud_relat()
        df = df.join(consol, ["codmes", "codclaveunicocli"], "left_outer")
        videvar = self._build_videvar_mora_pond()
        df = df.join(videvar, ["codmes", "codclaveunicocli"], "left_outer")
        pasivo_evol = self._build_pasivo_evol_sald_pym()
        df = df.join(pasivo_evol, ["codmes", "codinternocomputacional"], "left_outer")
        evol_trx = self._build_evol_comp_trx_pym()
        df = df.join(evol_trx, ["codmes", "codinternocomputacional"], "left_outer")

        # Tablas sin desfase
        mtx_rcc_otra = self._build_mtx_rcc_otra_deuda()
        df = df.join(mtx_rcc_otra, ["codmes", "codclaveunicocli"], "left_outer")
        clasi_exp = self._build_clasi_exper_cli()
        df = df.join(clasi_exp, ["codmes", "codclaveunicocli"], "left_outer")
        evol_cli = self._build_evol_cli_pym()
        df = df.join(evol_cli, ["codmes", "codinternocomputacional"], "left_outer")
        mtx_res_saldo = self._build_mtx_resumen_saldo()
        df = df.join(mtx_res_saldo, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_res_act_pas = self._build_mtx_resumen_act_pas()
        df = df.join(mtx_res_act_pas, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_mov_abono = self._build_mtx_mov_abono_pas()
        df = df.join(mtx_mov_abono, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_mov_cargo = self._build_mtx_mov_cargo_pas()
        df = df.join(mtx_mov_cargo, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_mov_pas = self._build_mtx_mov_pas()
        df = df.join(mtx_mov_pas, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_trx_canal_pago = self._build_mtx_trx_canal_pago_transf()
        df = df.join(mtx_trx_canal_pago, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_trx_pos = self._build_mtx_trx_pos()
        df = df.join(mtx_trx_pos, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_rcc_prod = self._build_mtx_rcc_prod()
        df = df.join(mtx_rcc_prod, ["codmes", "codclaveunicocli"], "left_outer")
        mtx_trx_canal = self._build_bd_mtx_trx_canal()
        df = df.join(mtx_trx_canal, ["codmes", "codclaveunicocli"], "left_outer")

        # Reemplazo de dummies
        for c in df.columns:
            df = df.withColumn(c, F.when(F.col(c).isin(self.GLOB_DUMMY_LIST), None).otherwise(F.col(c)))

        # Capping y creación de columnas _cap
        for orig_col, (lower, upper) in self.capping_config.items():
            if orig_col in df.columns:
                capped = F.col(orig_col)
                if lower is not None:
                    capped = F.when(capped < lower, lower).otherwise(capped)
                if upper is not None:
                    capped = F.when(capped > upper, upper).otherwise(capped)
                df = df.withColumn(orig_col + "_cap", capped)

        # Agregar RNG_ACTIVIDAD_ECONOM (basado en ACT_ECO_FIN)
        df = df.withColumn("RNG_ACTIVIDAD_ECONOM",
            F.when(F.col("ACT_ECO_FIN").isNull(), 1)
             .when(F.col("ACT_ECO_FIN") == 'PESCA', 1)
             .when(F.col("ACT_ECO_FIN") == 'OTROS', 1)
             .when(F.col("ACT_ECO_FIN") == 'SERVICIOS', 1)
             .when(F.col("ACT_ECO_FIN") == 'ENERGIA', 1)
             .when(F.col("ACT_ECO_FIN") == 'CONSTRUCCION', 1)
             .when(F.col("ACT_ECO_FIN") == 'ADM_PUBLICA', 1)
             .when(F.col("ACT_ECO_FIN") == 'ACT_INMOB_EMP_Y_DE_ALQ', 1)
             .when(F.col("ACT_ECO_FIN") == 'INDUST_MANUFACT', 1)
             .when(F.col("ACT_ECO_FIN") == 'COMERCIO', 1)
             .when(F.col("ACT_ECO_FIN") == 'HOGAR', 1)
             .when(F.col("ACT_ECO_FIN") == 'SALUD', 1)
             .otherwise(0))

        # Seleccionar todas las columnas (aprox. 109)
        final_df = df.select(*df.columns)

        # Escritura particionada por codmes
        if self.full_sink_table:
            final_df.write.mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("codmes") \
                .saveAsTable(self.full_sink_table)
            print(f"Tabla maestra escrita en {self.full_sink_table}")
        else:
            print("No se especificó tabla destino. Retornando DataFrame.")
            return final_df

        return final_df