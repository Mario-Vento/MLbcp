"""
Configuración Compartida - Data Preparation
Aquí residen todas las variables y parámetros base compartidos
entre los scripts de Data Preparation (01_fuente_hm_atraso_intra, 50_universo_implementacion, etc.).
"""

def get_dataprep_config(periodo: str):
    """
    Retorna un diccionario con la configuración necesaria para
    instanciar las clases de preparación de datos, basado en
    el período (codmes) proporcionado por el orquestador.
    """
    config = {
        "periodo": periodo,
        "codmes_ini": periodo,
        "codmes_fin": periodo,

        # --- Catálogos y Esquemas ---
        "src_catalog": "catalog_lhcl_prod_bcp", 
        "sink_catalog": "catalog_cemm_expl_bcp_prod", # Antes "catalog_lhcl_prod_bcp"
        # "sink_schema": "bcp_expl_007_models", # Default temporal, o puede venir del entorno
        "sink_schema": "bcp_expl_007", # bcp_expl_007_models genera fallas en notebook 03

        # --- Nombres de Tablas de 01 Fuente HM Atraso ---
        "sink_table_hm_atraso_cta": "hm_atraso_cliente_cta",
        "sink_table_hm_atraso": "hm_atraso_cliente_hm",

        # --- Nombres de Tablas de 50 Universo ---
        "src_schema_portafolio": "bcp_ddv_adrmmgr_seginfobasesgenerales_vu",
        "src_table_portafolio": "hm_portafoliocredito",
        "sink_table_portafolio_troncal": "bhv_troncal_cliente_base",
        #"path_mora_intrames": "catalog_lhcl_prod_bcp.bcp_ddv_adrmmgr_videavariablesmodelos_vu.hm_clientemoraintrames",
    }

    return config