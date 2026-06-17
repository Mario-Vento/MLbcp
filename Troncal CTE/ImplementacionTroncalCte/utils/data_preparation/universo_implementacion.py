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
    round, greatest, broadcast,
)
from pyspark.sql.column import Column
from pyspark.sql.types import (DataType, NumericType)
from pyspark.sql.utils import AnalysisException
from pyspark.storagelevel import StorageLevel

from utils.data_preparation.utils_dataprep import write_to_unity_catalog, join_variable_table, print_spark

class UniversoImplementacion:
    """
    Clase para la preparación de datos del Universo / Implementación.
    Basada en el notebook 50_universo_implementacion.ipynb.
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
        verbosity: bool = True
    ):
        self.spark = spark
        self.codmes_ini = int(codmes_ini)
        self.codmes_fin = int(codmes_fin)
        self.verbosity = verbosity

        # Paths derivados
        self.path_table_portfolio_troncal = f"{sink_catalog}.{sink_schema}.{sink_table_portafolio_troncal}"
        self.v_path_portfolio = f"{src_catalog}.{src_schema_portafolio}.{src_table_portafolio}"
        # self.path_mora_intrames = f"{sink_catalog}.{sink_schema}.{sink_table_hm_atraso}"
        self.path_mora_intrames = "catalog_lhcl_prod.bcp.bcp_ddv_adrmmgr_videsvariablesmodelos_vu.hm_clientemoraintrames"

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

        # columnas en SELECT final
        self.mt_final_cols = [
            # pks
            'codmes',
            'codclavepartycli',
            'codinternocomputacional',
            'codclaveunicocli',
            # variables de seguimiento
            'flgclictavalida',
            'ctddiaatraso',
            'max_maduracion_cli',
            'mtosaldocapitalsol',
            'FLG_TC',
            'FLG_TC_PERSONAS',
            'FLG_CEF',
            'FLG_VEH',
            'FLG_HIP',
            # variables de modelo
            'ctdmora_intra_0_imp_cut15',
            'max_maduracion_cli_imp150',
            'max_mora_intra_u6m',
            'exp_pct_evol_ship_u3m_rt_u6m_cua',
            'prd_pct_pmpas_pmact_24_24_rt24a',
            'fatc_pct_pag_mn_ctamin_u6m_rtu6',
            'rcc_deu_ind_pj_prm_u3m_impt',
            'rcc_pct_sf3_sf24_ship_rt_u24',
            'rcc_pct_rdv_prm_u3ma',
            'prd_prm_tsav_mnn_6_6_rt6a',
            'mto_deu_mora_sol_u48_log',
            'flg_titulo_c',
            'q_diamora_max_100_u24a',
            'ftc_flg_pg_ful_clant_sol_mx_u3_c',
            'rcc_pct_sf12_sf24_rt_u24c',
            'edad_cut',
            'rcc_q_mes_act_sf_buen_mal_0_u3_3',
            'evol_mora_intra_g3m_cut',
            'ctdpdhu24_cut',
            'pos_pct_q_etcnpscl_a_sum_u6_rt6',
            'pos_tkt_trx_com_sol_prm_u3ma',
            'grf_tip_clas_rie_cli_2_4_mx_u3m',
            'isav_q_opea_desm_prm_u3m_cut',
            'evol_deu_ship_max_u12m_u24_raw',
            'grf_cvta_prov_rie4_prm_u3m',
            'prod_flg_sld_aho_300',
            'q_mes_mto_tot_pgsrv_sol_m0_u3m',
            'rcc_mto_gar_ope_cre_cut_logr'
        ]

    def execute(self):
        print(f"Mes inicio      : {self.codmes_ini}")
        print(f"Portafolio fuente  : {self.v_path_portfolio}")
        print(f"Mora intramés (desde 01) : {self.path_mora_intrames}")
        print(f"Tabla destino    : {self.path_table_portfolio_troncal}")

        # Lectura del portafolio de créditos
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

        # Agregación a nivel cliente-mes (dfPorto) OBS: Esta tabla sirve para CEF, HIP, TC y VEH
        df_porto = df_port_cta_rbm_per.groupby("codclavepartycli", "codmes").agg(
            F.max(F.trim(F.col("codinternocomputacional"))).alias("codinternocomputacional"),
            F.coalesce(F.max(F.col("codclaveunicocli")), F.lit(None)).alias("codclaveunicocli"),
            # --- Flag cliente con cuenta válida ---
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1", F.lit(1))), F.lit(0)).alias("flgclictavalida"),
            # --- Cálculos solo con cuentas válidas ---
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1", F.col("ctddiaatraso"))), F.lit(0)).alias("ctddiaatraso"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1", F.col("ctdmesmaduracion"))), F.lit(0)).alias("max_maduracion_cli"),
            F.coalesce(F.sum(F.when(F.col("flgctavalida") == "1", F.col("mtosaldocapitalsol"))), F.lit(0)).alias("mtosaldocapitalsol"),
            # --- Banderas solo con cuentas válidas ---
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1" & self.v_flg_rev, F.lit(1))), F.lit(0)).alias("FLG_TC"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1" & self.v_flg_rev, F.col("flgtarjetacreditoper"))), F.lit(0)).cast("int").alias("FLG_TC_PERSONAS"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1" & self.v_flg_cef, F.lit(1))), F.lit(0)).alias("FLG_CEF"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1" & self.v_flg_veh, F.lit(1))), F.lit(0)).alias("FLG_VEH"),
            F.coalesce(F.max(F.when(F.col("flgctavalida") == "1" & self.v_flg_hip, F.lit(1))), F.lit(0)).alias("FLG_HIP")
        )

        df_porto = df_porto.dropDuplicates(["codclaveunicocli", "codmes"])
        df_porto = df_porto.withColumn("fec_update", F.current_timestamp()) # OBS: fec_rutina o fec_registro en lugar de fec_update
        df_porto = df_porto.withColumn("NUM_PROD_PER", col("FLG_TC_PERSONAS") + col("FLG_CEF") + col("FLG_VEH") + col("FLG_HIP"))

        df_porto = df_porto.dropDuplicates(["codclaveunicocli", "codmes"])
        df_porto = df_porto.withColumn("fec_update", F.current_timestamp()) # OBS: fec_rutina o fec_registro en lugar de fec_update
        df_porto = df_porto.withColumn("NUM_PROD_PER", col("FLG_TC_PERSONAS") + col("FLG_CEF") + col("FLG_VEH") + col("FLG_HIP"))

        df_porto.persist(StorageLevel.MEMORY_AND_DISK)
        df_porto.count()
        df_port_cta_rbm_per.unpersist()

        # Resumen dfPorto
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


        # Join con tablas de variables
        VARIABLE_TABLES = [
            # 1. PDH RBM
            (
                # "catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_driverpdh_vu.hm_calculomarcaingresopdhrbm",
                "catalog_cem_expl_bcp_prod.bcp_expl_mmgr_mlde.hm_calculomarcaingresopdhrbm_mlops",
                ["ctdpdhu24"],
                "codclavepartycli", 0, "pdhrbm",
            ),
            # 2. Concepto deudor RCC
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_conceptodeudorrcc",
                ["rcc_mto_gar_ope_cre"],
                "codclavepartycli", 0, "df_conceptodeudorrcc",
            ),
            # 3. Grafo SUNEDU (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_flujotrxcli_vu.hm_conceptografoingresosunedudigitalrbm",
                ["mtoingresoinferidomedianaproveedorsol"],
                "codclavepartycli", +1, "df_grafosunedu",
            ),
            # 4. Concepto resumen saldo (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_conceptoresumensaldo",
                ["prod_flg_sld_aho_300"],
                "codclavepartycli", +1, "df_cptoresumensaldo",
            ),
            # 5. Matriz demográfica
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdemografico",
                ["dem_fec_nacimiento"],
                "codclavepartycli", 0, "df_mtxdemografico",
            ),
            # 6. Deudor RCC otra deuda (join por codclaveunicocli)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccotradeuda",
                ["rcc_pct_rdv_prm_u3m"],
                "codclaveunicocli", 0, "df_mtxdeudor_rcc_odeuda",
            ),
            # 7. Deudor RCC producto
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizdeudorrccproducto",
                [
                    "rcc_mto_deu_ind_pj_prm_u3m",
                    "rcc_pct_sf3_sf24_ship_rt_u24",
                    "rcc_pct_sf12_sf24_rt_u24",
                    "rcc_mto_deu_prod_sum_u24",
                    "rcc_ctd_mes_act_sf_buen_mal_0_u3m",
                    "rcc_mto_deu_ship_max_u12",
                    "rcc_mto_deu_ship_max_u24",
                ],
                "codclavepartycli", 0, "df_mtxdeudor_rccproducto",
            ),
            # 8. Experiencia cliente
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizexperienciacliente",
                ["exp_pct_evol_ship_u3m_rt_u6m", "exp_ctd_diamora_max_100_u24"],
                "codclavepartycli", 0, "df_mtx_expcli",
            ),
            # 9. Facturación tarjeta (join por codclaveunicocli)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizfacturaciontarjeta",
                ["fatc_flg_pg_ful_cclant_sol_max_u3m"],
                "codclaveunicocli", 0, "df_mtx_fact_tc",
            ),
            # 10. Facturación transacción tarjeta
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizfacturaciontransacciontarjeta",
                ["fatc_pct_pag_mn_ctamin_u6m_rt_u6m"],
                "codclavepartycli", 0, "df_mtx_fact_tx_tc",
            ),
            # 11. Grafo atributo cliente banca (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_flujotrxcli_vu.hm_matrizgrafoatributoclientebanca",
                ["grf_pct_cto_vta_prov_def_tip_clas_rie_tot_4_prm_u3m"],
                "codclavepartycli", +1, "df_mtx_grfatrib_cli",
            ),
            # 12. Grafo interacción banca (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_flujotrxcli_vu.hm_matrizgrafointeraccionbanca",
                ["grf_pct_tip_clas_rie_sbs_cli_2_4_max_u3m"],
                "codclavepartycli", +1, "df_mtx_grfitx_banca",
            ),
            # 13. Mora ponderada (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_matrizmoraponderadaclientemmgr",
                ["mtodeudadiamorafactordsctosolu48"],
                "codclavepartycli", +1, "df_mtx_moraponderada",
            ),
            # 14. Movimiento abono pasivo
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizmovimientoabonopasivo",
                ["isav_ctd_opea_desm_prm_u3m"],
                "codclavepartycli", 0, "df_mtx_movabonopasivo",
            ),
            # 15. Pago servicios (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_trxcomportamiento_vu.hm_matrizpagoservicioclienterbm",
                ["ctdmesmtototalpagoservsolmay0u3m"],
                "codclavepartycli", +1, "df_mtx_pagoservicios",
            ),
            # 16. Resumen saldo activo/pasivo (desfase +1)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matrizresumensaldoactivopasivo",
                ["prod_pct_pmpas_pmact_24_24_rt_u24", "prod_mto_sld_prm_tsav_min_6_6_rt_u6m"],
                "codclavepartycli", +1, "df_mtx_saldo_pas_act",
            ),
            # 17. Transacciones POS (join por codclaveunicocli)
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matriztransaccionpos",
                ["pos_tkt_trx_com_sol_prm_u3m"],
                "codclaveunicocli", 0, "df_mtx_trx_pos",
            ),
            # 18. Transacciones POS otro establecimiento
            (
                "catalog_lhcl_prod_bcp.bcp_ddv_matrizvariables_vu.hm_matriztransaccionposotroestablecimiento",
                ["pos_pct_ctd_etcnpscl_a_sum_u6m_rt_u6m"],
                "codclavepartycli", 0, "df_mtx_trx_pos_ot_establ",
            ),
            # 19. Nivel educativo (desfase +1)
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
            # 20. Mora intrames
            (
                f"{self.path_mora_intrames}",
                ["ctdnivmoracli", "ctdmaxatrasou6m", "ctdevoatrasog3m"],
                "codclavepartycli", 0, "df_mora_intrames"
            )
        ]

        # Ejecutar todos los joins
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

        # Join final: unir todas las tablas enriquecidas
        JOIN_KEYS = ["codmes", "codclavepartycli"] # funciona porque cada enriched_dfs[name] contiene "codclavepartycli" independientemente del argumento en join_key en la función join_variable_table()

        df_final = df_porto.dropDuplicates(JOIN_KEYS)

        # Todas las tablas de variables
        for _, _, _, _, name in VARIABLE_TABLES:
            df_final = df_final.join(
                enriched_dfs[name].dropDuplicates(JOIN_KEYS).drop("codclaveunicocli"),
                JOIN_KEYS, "left"
            )

        # Reemplazo de dummies en columnas numéricas
        dummyList = [1111111111, -1111111111, 2222222222, -2222222222,
                     3333333333, -3333333333, 4444444444, 5555555555, 6666666666, 7777777777]
        colsNums = [f.name for f in df_final.schema.fields if isinstance(f.dataType, NumericType)]
        df_final = df_final.replace(dummyList, None, subset=colsNums)

        # Construcción de variable ctdmora_intra_0_imp_cut15 (1)
        df_final = df_final.withColumnRenamed("ctdnivmoracli", "ctdmora_intra_0_imp_cut15")

        # Construcción de variable max_maduracion_cli_imp150 (2)
        df_final = (
            df_final
            .withColumn(
                "max_maduracion_cli_imp",
                F.when(
                    F.col("max_maduracion_cli").isNull(),
                    F.lit(0.0))
                .otherwise(F.col("max_maduracion_cli")))
            .withColumn(
                "max_maduracion_cli_imp150",
                F.greatest(F.least(F.lit(150.0), F.col("max_maduracion_cli_imp")), F.lit(1.0)))
        )

        # Construcción de variable max_mora_intra_u6m (3)
        df_final = df_final.withColumnRenamed("ctdmaxatrasou6m", "max_mora_intra_u6m")

        # Construcción de variable exp_pct_evol_ship_u3m_rt_u6m_cua (4)
        df_final = (
            df_final
            .withColumn(
                "exp_pct_evol_ship_u3m_rt_u6m_cua",
                F.when(F.col("exp_pct_evol_ship_u3m_rt_u6m").isNotNull(),
                F.greatest(
                    F.least(F.lit(2.0), F.col("exp_pct_evol_ship_u3m_rt_u6m")),
                    F.lit(0.2494651) ) ) )
        )

        # Construcción de variable prd_pct_pmpas_pmact_24_24_rt24a (5)
        df_final = (
            df_final
            .withColumn(
                "prd_pct_pmpas_pmact_24_24_rt24a",
                F.when(F.col("prod_pct_pmpas_pmact_24_24_rt_u24").isNotNull(),
                F.greatest(
                    F.least(F.lit(258.9774935), F.col("prod_pct_pmpas_pmact_24_24_rt_u24")),
                    F.lit(0.15571) ) ) )
        )

        # Construcción de variable fatc_pct_pag_mn_ctamin_u6m_rtu6 (6)
        df_final = (
            df_final
            .withColumn(
                "fatc_pct_pag_mn_ctamin_u6m_rtu6",
                F.when(F.col("fatc_pct_pag_min_ctamin_u6m_rt_u6m").isNotNull(),
                F.greatest(
                    F.least(F.lit(39.7449704), F.col("fatc_pct_pag_min_ctamin_u6m_rt_u6m")),
                    F.lit(0.105) ) ) )
        )

        # Construcción de variable rcc_deu_ind_pj_prm_u3m_impt (7)
        df_final = (
            df_final
            .withColumn(
                "rcc_deu_ind_pj_prm_u3m_imp",
                F.when(F.col("rcc_mto_deu_ind_pj_prm_u3m").isNull(), 0)
                .otherwise(F.col("rcc_mto_deu_ind_pj_prm_u3m")) )
            .withColumn(
                "rcc_deu_ind_pj_prm_u3m_impt",
                F.greatest(
                    F.least(F.lit(224813.35), F.col("rcc_deu_ind_pj_prm_u3m_imp")),
                    F.lit(0.0)) ) )
        )

        # Construcción de variable rcc_pct_rdv_prm_u3ma (9)
        df_final = (
            df_final
            .withColumn(
                "rcc_pct_rdv_prm_u3ma",
                F.when(
                    F.col("rcc_pct_rdv_prm_u3m").isNotNull(),
                    F.greatest(
                        F.least(F.lit(0.03), F.col("rcc_pct_rdv_prm_u3m")),
                        F.lit(0.00264)) ) ) )
        )

        # Construcción de variable prd_prm_tsav_mnn_6_6_rt6a (10)
        df_final = df_final.withColumn(
            "prd_prm_tsav_mnn_6_6_rt6a",
            F.when(
                F.col("prod_mto_sld_prm_tsav_min_6_6_rt_u6m").isNotNull(),
                F.greatest(F.lit(0.0010812), F.least(F.lit(0.8), F.col("prod_mto_sld_prm_tsav_min_6_6_rt_u6m")))
            ).otherwise(F.lit(None))
        )

        # Construcción de variable mto_deu_mora_sol_u48_log (11)
        df_final = df_final.withColumn(
            "mto_deu_mora_sol_u48_cut",
            F.when(
                F.col("mtodeudadiamorafactordscotsolu48").isNotNull(),
                F.greatest(F.lit(0), F.least(F.lit(105000), F.col("mtodeudadiamorafactordscotsolu48")))
            ).otherwise(F.lit(None))
        ).withColumn(
            "mto_deu_mora_sol_u48_log",
            F.when(F.col("mto_deu_mora_sol_u48_cut").isNotNull(), F.log1p(F.col("mto_deu_mora_sol_u48_cut"))).otherwise(F.lit(None))
        )

        # Construcción de variable flg_titulo_c (12)
        df_final = df_final.withColumn(
            "flg_titulo",
            F.when(
                (F.col("flgniveleducativobachiller") == "1") |
                (F.col("flgniveleducativodoctorado") == "1") |
                (F.col("flgniveleducativomaestria") == "1") |
                (F.col("flgniveleducativotitulado") == "1"),
                F.lit(1)
            ).otherwise(F.lit(0))
        ).withColumn(
            "flg_titulo_c",
            F.col("flg_titulo").cast("string")
        )

        # Construcción de variable q_diamora_max_100_u24a (13)
        df_final = df_final.withColumn(
            "q_diamora_max_100_u24a",
            F.when(
                F.col("exp_ctd_diamora_max_100_u24").isNotNull(),
                F.greatest(F.lit(0), F.least(F.lit(27), F.col("exp_ctd_diamora_max_100_u24")))
            ).otherwise(F.lit(None))
        )

        # Construcción de variable ftc_flg_pg_ful_clant_sol_mx_u3_c (14)
        df_final = df_final.withColumn(
            "ftc_flg_pg_ful_clant_sol_mx_u3_c",
            F.col("fatc_flg_pg_ful_cclient_sol_max_u3m").cast("string")
        )

        df_final = df_final.fillna(".", subset=['ftc_flg_pg_ful_clant_sol_mx_u3_c']) # OBS: SAS espera "." en lugar de null

        # Construcción de variable rcc_pct_sf12_sf24_rt_u24c (15)
        df_final = df_final.withColumn(
            "rcc_pct_sf12_sf24_rt_u24c",
            F.when(
                F.col("rcc_pct_sf12_sf24_rt_u24").isNotNull(),
                F.greatest(F.lit(0.2225584), F.least(F.lit(2.0), F.col("rcc_pct_sf12_sf24_rt_u24")))
            ).otherwise(F.lit(None))
        )

        # Construcción de variable edad_cut (16)
        df_final = df_final.withColumn(
            "edad",
            F.round(
                (
                    (F.floor(F.col("codmes") / 100) * F.lit(12) + (F.col("codmes") % F.lit(100))) -
                    (F.month("dem_fec_nacimiento") + F.year("dem_fec_nacimiento") * F.lit(12))
                ) / F.lit(12.0),
                1
            )
        )

        df_final = df_final.withColumn(
            "edad_cut",
            F.when(
                F.col("edad").isNotNull(),
                F.greatest(F.lit(22.0), F.least(F.lit(65.0), F.col("edad")))
            ).otherwise(F.lit(None))
        )

        df_final = df_final.withColumn( "edad_cut", F.round(F.col("edad_cut"), 0) ) # OBS: SAS redondea a enteros esta columna

        # Construcción de variable rcc_q_mes_act_sf_buen_mal_0_u3_3 (17)
        df_final = df_final.withColumn(
            "rcc_q_mes_act_sf_buen_mal_0_u3_3",
            F.when(
                F.col("rcc_ctd_mes_act_sf_buen_mal_0_u3m").isNotNull(),
                F.greatest(F.lit(-3), F.least(F.lit(3), F.col("rcc_ctd_mes_act_sf_buen_mal_0_u3m")))
            ).otherwise(F.lit(None))
        )

        # Construcción de variable evol_mora_intra_g3m_cut (18)
        df_final = df_final.withColumnRenamed("ctdevoatrasog3m", "evol_mora_intra_g3m_cut")

        # Construcción de variable ctdpdhu24_cut (19)
        df_final = df_final.withColumn(
            "ctdpdhu24",
            F.when(F.col("ctdpdhu24").isNull() | (F.col("ctdpdhu24") < 1), F.lit(0)).otherwise(F.col("ctdpdhu24"))
        )
        df_final = df_final.withColumn(
            "ctdpdhu24_cut",
            F.cell(F.col("ctdpdhu24") / 2) * 2
        )

        # Construcción de variable pos_pct_q_etcnpscl_a_sum_u6_rt6 (20)
        df_final = df_final.withColumn('pos_pct_q_etcnpscl_a_sum_u6_rt6', F.col('pos_pct_ctd_etcnpscl_a_sum_u6m_rt_u6m'))

        # Construcción de variable pos_tkt_trx_com_sol_prm_u3ma (21)
        df_final = df_final.withColumn(
            "pos_tkt_trx_com_sol_prm_u3ma",
            F.when(
                F.col("pos_tkt_trx_com_sol_prm_u3m").isNotNull(),
                F.greatest(F.lit(19.38888), F.least(F.lit(90.0), F.col("pos_tkt_trx_com_sol_prm_u3m")))
            ).otherwise(F.lit(None))
        )

        # Construcción de variable grf_tip_clas_rie_cli_2_4_mx_u3m (22)
        df_final = df_final.withColumn(
            "grf_tip_clas_rie_cli_2_4_mx_u3m",
            F.col("grf_pct_tip_clas_rie_sbs_cli_2_4_max_u3m")
        )

        # Construcción de variable isav_q_opea_desm_prm_u3m_cut (23)
        df_final = (
            df_final
            .withColumn(
                "isav_ctd_opea_desm_prm_u3m",
                F.when(F.col("isav_ctd_opea_desm_prm_u3m").isNull(), F.lit(0.0)).otherwise(F.col("isav_ctd_opea_desm_prm_u3m"))
            )
            .withColumn(
                "isav_q_opea_desm_prm_u3m_cut",
                F.when(
                    F.col("isav_ctd_opea_desm_prm_u3m").isNotNull(),
                    F.greatest(F.lit(0.0), F.least(F.lit(2.0), F.col("isav_ctd_opea_desm_prm_u3m")))
                ).otherwise(F.lit(None))
            )
        )

        # Construcción de variable evol_deu_ship_max_u12m_u24_raw (24)
        df_final = (
            df_final
            .withColumn(
                "rcc_deu_ship_max_u12_imp",
                F.when(F.col("rcc_mto_deu_ship_max_u12").isNull() | (F.col("rcc_mto_deu_ship_max_u12") < 100.0), F.lit(100.0))
                .otherwise(F.col("rcc_mto_deu_ship_max_u12"))
            )
            .withColumn(
                "rcc_deu_ship_max_u24_imp",
                F.when(F.col("rcc_mto_deu_ship_max_u24").isNull() | (F.col("rcc_mto_deu_ship_max_u24") < 100.0), F.lit(100.0))
                .otherwise(F.col("rcc_mto_deu_ship_max_u24"))
            )
            .withColumn(
                "evol_deu_ship_max_u12m_u24_raw",
                F.col("rcc_deu_ship_max_u12_imp") / F.col("rcc_deu_ship_max_u24_imp")
            )
        )

        # Construcción de variable grf_cvta_prov_rie4_prm_u3m (25)
        df_final = df_final.withColumn(
            "grf_cvta_prov_rie4_prm_u3m",
            F.col("grf_pct_cto_vta_prov_def_tip_clas_rie_sbs_4_prm_u3m")
        )

        # Construcción de variable prod_flg_sld_aho_300 (26)
        # No aplica

        # Construcción de variable q_mes_mto_tot_pgsrv_sol_m0_u3m (27)
        df_final = df_final.withColumn(
            "q_mes_mto_tot_pgsrv_sol_m0_u3m",
            F.col("ctdmesmtototalpagoservsolmay0u3m")
        )

        # Construcción de variable rcc_mto_gar_ope_cre_cut_logr (28)
        df_final = (
            df_final
            .withColumn(
                "rcc_mto_gar_ope_cre_cut",
                F.when(
                    F.col("rcc_mto_gar_ope_cre").isNotNull(),
                    F.greatest(F.lit(0.0), F.least(F.lit(2_000_000.0), F.col("rcc_mto_gar_ope_cre")))
                ).otherwise(F.lit(None))
            )
            .withColumn(
                "rcc_mto_gar_ope_cre_cut_log",
                F.when(F.col("rcc_mto_gar_ope_cre_cut").isNotNull(), F.log1p(F.col("rcc_mto_gar_ope_cre_cut")))
                .otherwise(F.lit(None))
            )
            .withColumn(
                "rcc_mto_gar_ope_cre_cut_logr",
                F.when(
                    F.col("rcc_mto_gar_ope_cre_cut_log").isNotNull(),
                    F.greatest(F.lit(9.68), F.least(F.lit(14.5), F.col("rcc_mto_gar_ope_cre_cut_log")))
                ).otherwise(F.lit(None))
            )
        )

        # Selección de las columnas indispensables para los procesos posteriores
        df_final = df_final.select(*self.mt_final_cols)

        print(f"✅ df_final construido con {len(df_final.columns)} columnas")

        # Escritura a Unity Catalog
        write_to_unity_catalog(
            df=df_final,
            table_name=self.path_table_portfolio_troncal,
            partition_col="codmes",
            mode="overwrite",
            overwrite_partition=True,
        )

        print(f"df_final registrado")
        df_porto.unpersist()