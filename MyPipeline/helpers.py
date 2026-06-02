# helpers.py
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf


## Paso 14 Notebook Sherly
def operacionesMaxBetweenCols(columnas):
    resultado = 0.0
    valorInicial = float(-9999999999999999.0)
    mayor = float(0.0)
    numero = float(0.0)
    for column in columnas:
        if column is not None and str(column) != "" and str(column).upper() != "NULL":
            numero = float(column)
        else:
            numero = -9999999999999999.0
        if numero >= valorInicial:
            mayor = float(numero)
            valorInicial = float(numero)
        elif numero < valorInicial:
            mayor = float(valorInicial)
        if mayor == -9999999999999999.0:
            resultado = None
        else:
            resultado = mayor
    return resultado

operacionesMaxBetweenCols_udf = udf(operacionesMaxBetweenCols, DoubleType())



## Función extraída de repositorio /Workspace/Repos/rodrigoasencios@bcp.com.pe/fabpyme/utils.py
def add_codmes_spark(codmesColname, n):
    """Returns a sparkDf column that adds an integer to a CODMES column"""

    return F.date_format(F.add_months(F.to_date(F.col(codmesColname).cast('int'), 'yyyyMM'), n), 'yyyyMM')
