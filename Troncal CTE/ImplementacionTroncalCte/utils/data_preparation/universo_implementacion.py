import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import (
    desc, lit, col, trim, current_timestamp, coalesce, when,
    date_format, sum, max, min, count, countDistinct,
    substr, substring, floor, avg, concat, add_months,
    round, greatest, least, broadcast, log1p, months_between,
    last_day, to_date,
)
from pyspark.sql.column import Column
from pyspark.sql.types import (DataType, NumericType, DecimalType)
from pyspark.sql.utils import AnalysisException
from pyspark.storagelevel import StorageLevel

from utils.data_preparation.utils_dataprep import (
    write_to_unity_catalog,
    join_variable_table,
    print_spark,
    apply_caps_xgb_cap24,
    replace_sentinels_with_null,
    decimals_to_double,
    rename_columns_safe,
)

# =============================================================================
# Diccionario de renombrado a nombres SAS
# Clave = nombre de modelo (cómo queda la columna tras la capa de alias)
# Valor = nombre SAS final (truncado a <=32 chars, como lo espera el .sashdat)
# =============================================================================
DICT_NAMES_SAS = {
    "ctdmora_intra_0": "ctdmora_intra_0",
    "max_maduracion_cli": "max_maduracion_cli",
    "max_mora_intra_u6m": "max_mora_intra_u6m",
    "exp_pct_evol_ship_u3m_rt_u6m": "exp_pct_evol_ship_u3m_000",
    "prd_pct_pmpas_pmact_24_24_rt24": "prd_pct_pmpas_pmact_2_000",
    "fatc_pct_pag_mn_ctamin_u6m_rtu6": "fatc_pct_pag_mn_ctami_000",
    "rcc_mto_deu_ind_pj_prm_u3m": "rcc_mto_deu_ind_pj_pr_000",
    "rcc_pct_sf3_sf24_ship_rt_u24": "rcc_pct_sf3_sf24_ship_000",
    "rcc_pct_rdv_prm_u3m": "rcc_pct_rdv_prm_u3m",
    "prd_prm_tsav_mnn_6_6_rt6": "prd_prm_tsav_mnn_6_6_rt6",
    "mto_deu_mora_sol_u48": "mto_deu_mora_sol_u48",
    "flg_titulo": "flg_titulo",
    "q_diamora_max_100_u24": "q_diamora_max_100_u24",
    "fatc_flg_pag_ful_clant_sol_mx_u3": "fatc_flg_pag_ful_clan_000",
    "rcc_pct_sf12_sf24_rt_u24": "rcc_pct_sf12_sf24_rt_u24",
    "rcc_q_mes_act_sf_buen_mal_0_u3m": "rcc_q_mes_act_sf_buen_000",
    "max_mora_intra_g3m": "max_mora_intra_g3m",
    "ctdpdhu24": "ctdpdhu24",
    "pos_pct_q_etcnpscl_a_sum_u6_rt6": "pos_pct_q_etcnpscl_a__000",
    "pos_tkt_trx_com_sol_prm_u3m": "pos_tkt_trx_com_sol_p_000",
    "grf_tip_clas_rie_cli_2_4_mx_u3m": "grf_tip_clas_rie_cli__000",
    "isav_q_opea_desm_prm_u3m": "isav_q_opea_desm_prm_u3m",
    "rcc_mto_deu_ship_max_u12_u24": "rcc_mto_deu_ship_max__000",
    "grf_cvta_prov_rie4_prm_u3m": "grf_cvta_prov_rie4_pr_000",
    "prod_flg_sld_aho_300": "prod_flg_sld_aho_300",
    "q_mes_mto_tot_pgsrv_sol_m0_u3m": "q_mes_mto_tot_pgsrv_s_000",
    "rcc_mto_gar_ope_cre": "rcc_mto_gar_ope_cre",
}

# Capa de alias: nombre de columna FUENTE -> nombre de modelo (ArnoldNotebook celda 18)
# Solo se listan las que difieren; el resto de columnas fuente ya coinciden.
SOURCE_TO_MODEL = {
    # --- mora intra (hm_clientemoraintrames) ---
    "ctdnivmoracli": "ctdmora_intra_0",
    "ctdmaxatrasou3m": "max_mora_intra_u3m",
    "ctdmaxatrasop3m": "max_mora_intra_p3m",
    "ctdmaxatrasou6m": "max_mora_intra_u6m",
    # --- resto de fuentes ---
    "prod_pct_pmpas_pmact_24_24_rt_u24": "prd_pct_pmpas_pmact_24_24_rt24",
    "prod_mto_sld_prm_tsav_min_6_6_rt_u6m": "prd_prm_tsav_mnn_6_6_rt6",
    "fatc_pct_pag_min_ctamin_u6m_rt_u6m": "fatc_pct_pag_mn_ctamin_u6m_rtu6",
    "fatc_flg_pag_ful_cclant_sol_max_u3m": "fatc_flg_pag_ful_clant_sol_mx_u3",
    "mtodeudadiamorafactordsctosolu48": "mto_deu_mora_sol_u48",
    "exp_ctd_diamora_max_100_u24": "q_diamora_max_100_u24",
    "rcc_ctd_mes_act_sf_buen_mal_0_u3m": "rcc_q_mes_act_sf_buen_mal_0_u3m",
    "pos_pct_ctd_etcnpscl_a_sum_u6m_rt_u6m": "pos_pct_q_etcnpscl_a_sum_u6_rt6",
    "grf_pct_tip_clas_rie_sbs_cli_2_4_max_u3m": "grf_tip_clas_rie_cli_2_4_mx_u3m",
    "isav_ctd_opea_desm_prm_u3m": "isav_q_opea_desm_prm_u3m",
    "grf_pct_cto_vta_prov_def_tip_clas_rie_sbs_4_prm_u3m": "grf_cvta_prov_rie4_prm_u3m",
    "ctdmesmtototalpagoservsolmay0u3m": "q_mes_mto_tot_pgsrv_sol_m0_u3m",
}


def _ratio_with_sentinels(num_col: str, den_col: str) -> Column:
    """Ratio A/B con valores centinela (ArnoldNotebook).
    Los centinelas se anulan luego con replace_sentinels_with_null()."""
    A = F.col(num_col)
    B = F.col(den_col)
    return (
        F.when(A.isNull() & B.isNull(), F.lit(44444444444))
        .when(A.isNull() & (B == 0), F.lit(6666666666))
        .when(A.isNull() & (B > 0), F.lit(1111111111))
        .when(A.isNull() & (B < 0), F.lit(-1111111111))
        .when((A == 0) & B.isNull(), F.lit(7777777777))
        .when((A == 0) & (B == 0), F.lit(5555555555))
        .when(B.isNull() & (A > 0), F.lit(2222222222))
        .when(B.isNull() & (A < 0), F.lit(-2222222222))
        .when((B == 0) & (A > 0), F.lit(3333333333))
        .when((B == 0) & (A < 0), F.lit(-3333333333))
        .otherwise(F.round(A / B, 8))
        .cast(DecimalType(19, 8))
    )


class UniversoImplementacion:
    """
    Preparación del Universo / Troncal Cliente para el modelo
    model_xgboost_cap_24_mono_200.

    Sigue la lógica de Notebook modelador Arnold: arma el troncal desde las tablas fuente,
    deriva variables, limpia centinelas, renombra a nombres SAS, aplica los caps
    del modelo (cap_24) y las transformaciones log. La tabla resultante queda
    lista para scoreo con el .sashdat (los nombres de columna coinciden con los
    que espera el modelo SAS).
    """

    def __init__(
        self,
        spark: SparkSession,
        codmes_ini: int,
        codmes_fin: int,
        src_catalog: str,
        src_schema_portafolio: str,
        src_table_portafolio: str,
        sink_catalog: str,
        sink_schema: str,
        sink_table_portafolio_troncal: str,
        sink_table_hm_atraso: str,
        path_mora_intrames: str = "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_clientemoraintrames",
        verbosity: bool = True,
    ):
        self.spark = spark
        self.codmes_ini = int(codmes_ini)
        self.codmes_fin = int(codmes_fin)
        self.verbosity = verbosity

        # Paths derivados
        self.path_table_portfolio_troncal = f"{sink_catalog}.{sink_schema}.{sink_table_portafolio_troncal}"
        self.v_path_portfolio = f"{src_catalog}.{src_schema_portafolio}.{src_table_portafolio}"
        # Fuente de mora intramés (ahora parametrizable)
        self.path_mora_intrames = path_mora_intrames

        # Filtros de productos y sub-productos
        self.v_list_prod_no_rev = ['CONSUMO', 'HIPOTECARIO']
        self.v_filter_prod_con = col("CODPRODUCTONIVEL0RBM") == 'CONSUMO'
        self.v_filter_prod_tc = col("CODPRODUCTONIVEL0RBM") == 'TARJETA'
        self.v_filter_prod_hip = col("CODPRODUCTONIVEL0RBM") == 'HIPOTECARIO'

        self.v_list_prod_cef = ['04. Consumo', '14. Reprogramado Consumo', '10. Otros']
        self.v_list_prod_cef_valid = ['04. Consumo', '14. Reprogramado Consumo']
        self.v_list_prod_tc_companies = ['TCRCOR', 'TCRCTA', 'TCREMP']

        self.v_flg_hip = self.v_filter_prod_hip
        self.v_flg_veh = self.v_filter_prod_con & (col("codproductonivel1rbm") == '05. Vehicular')
        self.v_flg_cef = self.v_filter_prod_con & (col("codproductonivel1rbm").isin(self.v_list_prod_cef))
        self.v_flg_rev = self.v_filter_prod_tc
        self.v_flg_no_rev = col("CODPRODUCTONIVEL0RBM").isin(self.v_list_prod_no_rev)

        # ---------------------------------------------------------------------
        # Columnas del SELECT final (contrato del modelo cap_24_mono_200)
        # = llaves/metadata de producción + 28 features con nombre SAS.
        # Se excluyen sample/selected/flg_gruyie/def*_12 (desarrollo).
        # ---------------------------------------------------------------------
        self.mt_final_cols = [
            # llaves / metadata
            'codmes',
            'codclavepartycli',
            'codclaveunicocli',
            'codinternocomputacional',
            'ctddiaatraso',
            'flgclictavalida',
            'mtosaldocapitalsol',
            'flg_tc',
            'flg_tc_personas',
            'flg_cef',
            'flg_veh',
            'flg_hip',
            # 24 features del modelo cap_24 (nombres SAS, orden de importancia)
            'ctdmora_intra_0_o',
            'max_maduracion_cli',
            'max_mora_intra_u6m_o',
            'exp_pct_evol_ship_u3m_000',
            'prd_pct_pmpas_pmact_2_000_o',
            'fatc_pct_pag_mn_ctami_000_o',
            'rcc_pct_sf3_sf24_ship_000',
            'rcc_pct_rdv_prm_u3m_ooo',
            'rcc_q_mes_act_sf_buen_000',
            'prd_prm_tsav_mnn_6_6_rt6_ooo',
            'flg_titulo',
            'q_diamora_max_100_u24_o',
            'fatc_flg_pag_ful_clan_000',
            'rcc_pct_sf12_sf24_rt_u24',
            'edad_o',
            'pos_pct_q_etcnpscl_a__000',
            'ctdpdhu24_ooo',
            'grf_tip_clas_rie_cli__000',
            'isav_q_opea_desm_prm_u3m_o',
            'grf_cvta_prov_rie4_pr_000',
            'prod_flg_sld_aho_300',
            'rcc_mto_deu_ship_max__000',
            'max_mora_intra_g3m',
            'q_mes_mto_tot_pgsrv_s_000',
        ]

    def execute(self):
        print(f"Mes inicio      : {self.codmes_ini}")
        print(f"Portafolio fuente  : {self.v_path_portfolio}")
        print(f"Mora intramés (desde 01) : {self.path_mora_intrames}")
        print(f"Tabla destino    : {self.path_table_portfolio_troncal}")

        # =====================================================================
        # 1. Lectura del portafolio de créditos
        # =====================================================================
        df_port_cta_rbm_per = self.spark.sql(f"""
        SELECT
            codmes,
            trim(codclaveunicocli) as codclaveunicocli,
            trim(codclavepartycli) as codclavepartycli,
            codinternocomputacional,
            codclavecta,
            codclavectaoriginalsolicitud,
            ctdmesmaduracion,
            codproductocredito,
            trim(tipestadocta) as tipestadocta,
            ctddiaatraso,
            trim(flgctavalida) as flgctavalida,
            trim(codapp) as codapp,
            trim(tipbloqueoproducto) as tipbloqueoproducto_trim,
            substr(trim(tipbloqueoproducto),1,1) as tipbloqueoproducto,
            trim(destipbloqueoproducto) as destipbloqueoproducto,
            trim(flgtarjetacreditoper) as flgtarjetacreditoper,
            mtosaldocapital,
            mtosaldocapitalsol as o_mtosaldocapitalsol,
            case when mtosaldocapitalsol=0 and trim(codapp) = 'VP' AND trim(codproductonivel0rbm) IN ('TARJETA')
                 then 0 else mtosaldocapitalsol end as mtosaldocapitalsol,
            mtosaldocapitaldol,
            mtolineacredito,
            mtolineacreditosol,
            trim(codproducto) as codproducto,
            trim(desproducto) as desproducto,
            trim(desproductocredito) as desproductocredito,
            trim(dessubgrupoproductorbm) as dessubgrupoproductorbm,
            trim(codproductorbm)            as codproductorbm,
            trim(desproductorbm)            as desproductorbm,
            trim(codproductonivel0rbm)      as codproductonivel0rbm,
            trim(codproductonivel1rbm)      as codproductonivel1rbm,
            trim(codsolicitud) as codsolicitud
        FROM {self.v_path_portfolio}
        WHERE trim(codinternocomputacional) is not null
        AND (
            (trim(codapp) = 'VP' AND trim(codproductonivel0rbm) IN ('TARJETA'))
            OR
            (trim(codapp) = 'ALS' AND trim(codproductonivel0rbm) IN ('CONSUMO','HIPOTECARIO'))
        )
        AND codmes=={self.codmes_ini}
        """)

        df_port_cta_rbm_per.persist(StorageLevel.MEMORY_AND_DISK)
        df_port_cta_rbm_per.count()

        # Validación del portafolio
        if self.verbosity:
            print("=" * 50)
            print("Summary of Portafolio credito by credits")
            print(f"  Total records for {self.codmes_fin}      : {df_port_cta_rbm_per.filter(col('codmes')==self.codmes_fin).count():,}")
            print(f"  Valid records for {self.codmes_fin}      : {df_port_cta_rbm_per.filter(col('codmes')==self.codmes_fin).filter(col('flgctavalida')=='1').count():,}")
            print("=" * 50)
            print("Summary of Portafolio credito by customers")
            print(f"  Unique customers     : {df_port_cta_rbm_per.filter(col('codmes')==self.codmes_fin).select('codclavepartycli').distinct().count():,}")
            print(f"  Unique customers (cta valida): {df_port_cta_rbm_per.filter(col('codmes')==self.codmes_fin).filter(col('flgctavalida')=='1').select('codclavepartycli').distinct().count():,}")

        # =====================================================================
        # 2. Agregación a nivel cliente-mes (df_porto) OBS: Esta tabla sirve para CEF, HIP, TC y VEH
        # =====================================================================
        df_porto = df_port_cta_rbm_per.groupby("codclavepartycli", "codmes").agg(
            F.max(F.trim(F.col("codinternocomputacional"))).alias("codinternocomputacional"),
            F.coalesce(F.max(F.col("codclaveunicocli")), F.lit(None)).alias("codclaveunicocli"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1", F.lit(1))), F.lit(0)).alias("flgclictavalida"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1", F.col("ctddiaatraso"))), F.lit(0)).alias("ctddiaatraso"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1", F.col("ctdmesmaduracion"))), F.lit(0)).alias("max_maduracion_cli"),
            F.coalesce(F.sum(F.when(F.col("flgctavalida") == "1", F.col("mtosaldocapitalsol"))), F.lit(0)).alias("mtosaldocapitalsol"),
            F.coalesce(F.max(F.when((F.col("flgctavalida") == "1") & self.v_flg_rev, F.lit(1))), F.lit(0)).alias("flg_tc"),
            F.coalesce(F.max(F.when((F.col("flgctavalida") == "1") & self.v_flg_rev, F.col("flgtarjetacreditoper"))), F.lit(0)).cast("int").alias("flg_tc_personas"),
            F.coalesce(F.max(F.when((F.col("flgctavalida") == "1") & self.v_flg_cef, F.lit(1))), F.lit(0)).alias("flg_cef"),
            F.coalesce(F.max(F.when((F.col("flgctavalida") == "1") & self.v_flg_veh, F.lit(1))), F.lit(0)).alias("flg_veh"),
            F.coalesce(F.max(F.when((F.col("flgctavalida") == "1") & self.v_flg_hip, F.lit(1))), F.lit(0)).alias("flg_hip"),
        )

        df_porto = df_porto.dropDuplicates(["codclaveunicocli", "codmes"])
        df_porto = df_porto.withColumn("fec_update", F.current_timestamp())
        df_porto = df_porto.withColumn(
            "NUM_PROD_PER",
            col("flg_tc_personas") + col("flg_cef") + col("flg_veh") + col("flg_hip"),
        )

        df_porto.persist(StorageLevel.MEMORY_AND_DISK)
        df_porto.count()
        df_port_cta_rbm_per.unpersist()

        if self.verbosity:
            print_spark(
                df_porto.filter(col("codmes") >= 202501)
                .groupby("codmes")
                .agg(
                    count("codclavepartycli").alias("NumOps"),
                    sum("mtosaldocapitalsol").cast("float").alias("mtosaldocapitalsol"),
                )
                .withColumn("mtosaldocapitalsol", col("mtosaldocapitalsol") / 1e6)
                .orderBy(col("codmes").asc())
            )

        # =====================================================================
        # 3. Enriquecimiento con tablas de variables (crudo)
        # =====================================================================
        VARIABLE_TABLES = [
            # 1. PDH RBM
            (
                "catalog_cemm_expl_bcp_prod.bcp_expl_mmgr_mlde.hm_calculomarcaingresopdhrbm_mlops",
                ["ctdpdhu24"],
                "codclavepartycli", 0, "pdhrbm",
            ),
            # 2. Concepto deudor RCC
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_conceptodeudorrcc",
                ["rcc_mto_gar_ope_cre"],
                "codclavepartycli", 0, "df_conceptodeudorrcc",
            ),
            # 3. Concepto resumen saldo (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_conceptoresumensaldo",
                ["prod_flg_sld_aho_300"],
                "codclavepartycli", +1, "df_cptoresumensaldo",
            ),
            # 4. Matriz demográfica
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdemografico",
                ["dem_fec_nacimiento"],
                "codclavepartycli", 0, "df_mtxdemografico",
            ),
            # 5. Deudor RCC otra deuda (join por codclaveunicocli)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccotradeuda",
                ["rcc_pct_rdv_prm_u3m"],
                "codclaveunicocli", 0, "df_mtxdeudor_rcc_odeuda",
            ),
            # 6. Deudor RCC producto
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto",
                [
                    "rcc_mto_deu_ind_pj_prm_u3m",
                    "rcc_pct_sf3_sf24_ship_rt_u24",
                    "rcc_pct_sf12_sf24_rt_u24",
                    "rcc_ctd_mes_act_sf_buen_mal_0_u3m",
                    "rcc_mto_deu_ship_max_u12",
                    "rcc_mto_deu_ship_max_u24",
                ],
                "codclavepartycli", 0, "df_mtxdeudor_rccproducto",
            ),
            # 7. Experiencia cliente
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizexperienciacliente",
                ["exp_pct_evol_ship_u3m_rt_u6m", "exp_ctd_diamora_max_100_u24"],
                "codclavepartycli", 0, "df_mtx_expcli",
            ),
            # 8. Facturación tarjeta (join por codclaveunicocli)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizfacturaciontarjeta",
                ["fatc_flg_pag_ful_cclant_sol_max_u3m"],
                "codclaveunicocli", 0, "df_mtx_fact_tc",
            ),
            # 9. Facturación transacción tarjeta
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizfacturaciontransacciontarjeta",
                ["fatc_pct_pag_min_ctamin_u6m_rt_u6m"],
                "codclavepartycli", 0, "df_mtx_fact_tx_tc",
            ),
            # 10. Grafo interacción banca (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_flujotrxcli_vu.hm_matrizgrafointeraccionbanca",
                ["grf_pct_tip_clas_rie_sbs_cli_2_4_max_u3m"],
                "codclavepartycli", +1, "df_mtx_grfitx_banca",
            ),
            # 11. Grafo atributo cliente banca (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_flujotrxcli_vu.hm_matrizgrafoatributoclientebanca",
                ["grf_pct_cto_vta_prov_def_tip_clas_rie_sbs_4_prm_u3m"],
                "codclavepartycli", +1, "df_mtx_grfatrib_cli",
            ),
            # 12. Mora ponderada (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_matrizmoraponderadaclientemmgr",
                ["mtodeudadiamorafactordsctosolu48"],
                "codclavepartycli", +1, "df_mtx_moraponderada",
            ),
            # 13. Movimiento abono pasivo
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizmovimientoabonopasivo",
                ["isav_ctd_opea_desm_prm_u3m"],
                "codclavepartycli", 0, "df_mtx_movabonopasivo",
            ),
            # 14. Pago servicios (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_trxcomportamiento_vu.hm_matrizpagoservicioclienterbm",
                ["ctdmesmtototalpagoservsolmay0u3m"],
                "codclavepartycli", +1, "df_mtx_pagoservicios",
            ),
            # 15. Resumen saldo activo/pasivo (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo",
                ["prod_pct_pmpas_pmact_24_24_rt_u24", "prod_mto_sld_prm_tsav_min_6_6_rt_u6m"],
                "codclavepartycli", +1, "df_mtx_saldo_pas_act",
            ),
            # 16. Transacciones POS (join por codclaveunicocli)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matriztransaccionpos",
                ["pos_tkt_trx_com_sol_prm_u3m"],
                "codclaveunicocli", 0, "df_mtx_trx_pos",
            ),
            # 17. Transacciones POS otro establecimiento
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matriztransaccionposotroestablecimiento",
                ["pos_pct_ctd_etcnpscl_a_sum_u6m_rt_u6m"],
                "codclavepartycli", 0, "df_mtx_trx_pos_ot_establ",
            ),
            # 18. Nivel educativo (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_niveledusuperior_vu.hm_niveleducativoclienteadr",
                [
                    "flgniveleducativobachiller",
                    "flgniveleducativodoctorado",
                    "flgniveleducativomaestria",
                    "flgniveleducativotitulado",
                ],
                "codclavepartycli", +1, "df_nivel_educativo",
            ),
            # 19. Mora intramés (ahora trae las pre-agregadas u3m/p3m/u6m + nivel)
            (
                f"{self.path_mora_intrames}",
                ["ctdnivmoracli", "ctdmaxatrasou3m", "ctdmaxatrasop3m", "ctdmaxatrasou6m"],
                "codclavepartycli", 0, "df_mora_intrames",
            ),
        ]

        print("=" * 60)
        print("Enriqueciendo con tablas de variables...")
        print("=" * 60)

        enriched_dfs = {}
        for path, cols, jk, offset, name in VARIABLE_TABLES:
            enriched_dfs[name] = join_variable_table(
                df_base=df_porto,
                spark=self.spark,
                path_src=path,
                columns=cols,
                join_key=jk,
                month_offset=offset,
                df_name=name,
                persist=False,
            )

        JOIN_KEYS = ["codmes", "codclavepartycli"]
        df_final = df_porto.dropDuplicates(JOIN_KEYS)
        for _, _, _, _, name in VARIABLE_TABLES:
            df_final = df_final.join(
                enriched_dfs[name].dropDuplicates(JOIN_KEYS).drop("codclaveunicocli"),
                JOIN_KEYS, "left",
            )

        # =====================================================================
        # 4. CAPA DE MODELO
        # =====================================================================

        # (A) Alias: columnas fuente -> nombres de modelo (celda 18)
        df_final = rename_columns_safe(df_final, SOURCE_TO_MODEL)

        # (B0) Limpieza de dummies en CRUDO (equivale al dummyList original).
        #      Debe correr ANTES de las derivaciones, para que los ratios
        #      max_mora_intra_g3m (u3m/p3m) y rcc_mto_deu_ship_max_u12_u24 (u12/u24)
        #      no se calculen sobre valores dummy.
        df_final = replace_sentinels_with_null(self.spark, df_final)

        # (B) Derivaciones (celdas 24, 26, 27, 28, 29)
        # --- edad (celda 24) ---
        ref_date = F.last_day(F.to_date(F.col("codmes").cast("string"), "yyyyMM"))
        df_final = df_final.withColumn(
            "edad",
            F.when(F.col("dem_fec_nacimiento").isNull(), F.lit(None).cast("int"))
            .when(F.col("dem_fec_nacimiento") > ref_date, F.lit(None).cast("int"))
            .otherwise(F.floor(F.months_between(ref_date, F.col("dem_fec_nacimiento")) / F.lit(12))),
        ).drop("dem_fec_nacimiento")

        # --- max_mora_intra_g3m = ratio u3m/p3m (celda 26) ---
        df_final = df_final.withColumn(
            "max_mora_intra_g3m",
            _ratio_with_sentinels("max_mora_intra_u3m", "max_mora_intra_p3m"),
        )

        # --- flg_titulo (celda 27) ---
        df_final = df_final.withColumn(
            "flg_titulo",
            F.when(
                (F.col("flgniveleducativobachiller").cast("string") == F.lit("1"))
                | (F.col("flgniveleducativodoctorado").cast("string") == F.lit("1"))
                | (F.col("flgniveleducativomaestria").cast("string") == F.lit("1"))
                | (F.col("flgniveleducativotitulado").cast("string") == F.lit("1")),
                F.lit("1"),
            ).otherwise(F.lit("0")),
        )

        # --- rcc_mto_deu_ship_max_u12_u24 = ratio u12/u24 (celda 28) ---
        df_final = df_final.withColumn(
            "rcc_mto_deu_ship_max_u12_u24",
            _ratio_with_sentinels("rcc_mto_deu_ship_max_u12", "rcc_mto_deu_ship_max_u24"),
        )

        # --- fatc flag -> string (celda 29) ---
        df_final = df_final.withColumn(
            "fatc_flg_pag_ful_clant_sol_mx_u3",
            F.round(F.col("fatc_flg_pag_ful_clant_sol_mx_u3")).cast("int").cast("string"),
        )

        # (C) Limpieza de los centinelas que generan los ratios + decimales (celda 30)
        df_final = replace_sentinels_with_null(self.spark, df_final)
        df_final = decimals_to_double(self.spark, df_final)

        # (D) Renombrado a nombres SAS (celda 19/30)
        df_final = rename_columns_safe(df_final, DICT_NAMES_SAS)

        # (E) Caps del modelo cap_24 (celda 3/31)
        df_final = apply_caps_xgb_cap24(df_final)

        df_final = df_final.withColumnRenamed("NUM_PROD_PER", "num_prod_per")
        df_final = df_final.select(*self.mt_final_cols)

        print(f"✅ df_final construido con {len(df_final.columns)} columnas")

        # =====================================================================
        # 5. Escritura a Unity Catalog
        # =====================================================================
        write_to_unity_catalog(
            df=df_final,
            table_name=self.path_table_portfolio_troncal,
            partition_col="codmes",
            mode="overwrite",
            overwrite_partition=True,
        )

        print("df_final registrado")
        df_porto.unpersist()