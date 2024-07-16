import os
import pandas as pd
from sqlalchemy import create_engine

# Crear la URL de conexión para SQL Server
connection_string = (
    "mssql+pyodbc://usuario:contraseña@servidor/BaseDatos"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Crear el motor de SQLAlchemy
engine = create_engine(connection_string)

# Especificar el rango de fechas para el reporte
fecha_inicio = '11-07-2024'
fecha_fin = '15-07-2024'

# Convertir las fechas a formato YYYY-MM-DD
#fecha_inicio = pd.to_datetime(fecha_inicio_str, format='%d-%m-%Y').strftime('%Y-%m-%d')
#fecha_fin = pd.to_datetime(fecha_fin_str, format='%d-%m-%Y').strftime('%Y-%m-%d')

#Estructura del reporte final
columnas = {
    'REACCION_CODIGO': pd.Series(dtype='str'),
    'GES_INICIO_GESTION': pd.Series(dtype='datetime64[ns]'),
    'GES_FINAL_GESTION': pd.Series(dtype='datetime64[ns]'),
    'GES_DETALLE': pd.Series(dtype='str'),
    'OPE_NUMERO_OPERACION': pd.Series(dtype='str'),
    'PPR_FECHA_PROMESA_PAGO': pd.Series(dtype='datetime64[ns]'),
    'PPR_MONTO_PROMESA_PAGO': pd.Series(dtype='float64'),
    'BAN_BANCO': pd.Series(dtype='str'),
    'GES_NUMERO_DEPOSITO': pd.Series(dtype='str'),
    'GES_FECHA_DEPOSITO': pd.Series(dtype='datetime64[ns]'),
    'GES_MONTO_DEPOSITO': pd.Series(dtype='str'),
    'MEDIO_DE_CONTACTO': pd.Series(dtype='str'),
    'FILA': pd.Series(dtype='int64'),
}

# Crear el DataFrame vacío con la estructura definida
df_FINAL = pd.DataFrame(columnas)


# CONSULTA SQL ORIGINAL
consulta = f''' 
SELECT 
    g.[Id],
    g.[NUMERO RELACIONAL], 
    g.[FECHA GESTION], 
    g.[CODIGO LLAMADA], 
    g.[CODIGO ACCION1], 
    g.[CODIGO ACCION2], 
    g.[CODIGO LABOR], 
    g.[CODIGO JUDICIAL1], 
    g.[CONTACTO DIRECTO],
    g.[PROMESA PAGO],
    g.[MONTO PROMESA],
    g.[GESTOR GESTION L],
    di.[CUENTA]
FROM 
    GESTION as g
    INNER JOIN [DATOS IMPORTADOS] as di
    ON g.[NUMERO RELACIONAL] = di.[NUMERO RELACIONAL]
WHERE 
    CONVERT(date, g.[FECHA GESTION], 103) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'

'''

# Ejecutar la consulta y almacenar los resultados en un DataFrame
df = pd.read_sql_query(consulta, engine)

# SACAR registros sin FECHA GESTION
df_sin_fecha_gestion = df.dropna(subset=['FECHA GESTION'])

# Añadir la hora específica (08:00:00) a los registros que tienen solo la fecha
#df['FECHA GESTION'] = df['FECHA GESTION'] + pd.DateOffset(hours=8)
#GE1706

# Añadir nuevas columnas
df['FECHA INICIO GESTION'] = df['FECHA GESTION']
df['FECHA FIN GESTION'] = df['FECHA GESTION'] + pd.DateOffset(minutes=5)  # Añadir 5 minutos

# Código de Reacciones
cuenta_cancelada = 'CANC' #GEST
contacto_indirecto = 'CI' #GEST
contacto_sin_promesa = 'CS' #GEST
cumple_promesa = 'CU' #GEST
fallecido = 'FA' #GEST
no_cumple_promesa = 'IP' #GEST
promesa_cancelacion = 'NEF' #GEST
promesa_pago = 'PP' #GEST
no_contacto = 'NO'  #BMO
renuente_pagar = 'RP'  #BMO
segmentacion = 'SG'  #BMO
cliente_pago = 'YP' #BMO

# ------------------------------------------------------------
# Sección de procesamiento para calcular reacciones
# ------------------------------------------------------------

df['REACCION'] = None

#REGION REACCION CONTACTO INDIRECTO  #1
def F_contacto_indirecto(row):
    if row['CODIGO ACCION2'] == 'RECADO CON FAMILIAR -RC' or row['CODIGO ACCION2'] == 'RECADO CONTESTADORA CASA -MQ' or row['CODIGO ACCION2'] == 'RECADO EN TRABAJO -RW' or row['CODIGO ACCION2'] == 'RECADO CONTESTADORA CELULAR -CL':
        return contacto_indirecto
    return row['REACCION']

#REGION REACCION CONTACTO SIN PROMESA #2
def F_contacto_sin_promesa(row):
    if row['CODIGO ACCION1'] == 'CONTACTO CON CLIENTE - CC' or row['CODIGO ACCION1'] == 'CONTACTO CON CLIENTE -CC' or row['CODIGO ACCION1'] == 'TEXTO CELULAR - TE' or row['CODIGO ACCION1'] == 'TEXTO CELULAR -TE' or row['CODIGO ACCION1'] == 'CORREO ELECTRONICO -EE' and pd.isnull(row['MONTO PROMESA']):
        return contacto_sin_promesa
    return row['REACCION']

#REGION REACCION FALLECIDO  #3
def F_fallecido(row):
    if row['CODIGO ACCION2'] == 'TH FALLECIDO -FL':
        return fallecido
    return row['REACCION']

#REGION REACCION NO CUMPLE PROMESA #4
#def F_no_cumple_promesa(row):
#    if row['CODIGO ACCION1'] == 'CONTACTO CON CLIENTE -CC' and pd.isnull(row['MONTO PROMESA']):
#        return F_contacto_sin_promesa
#    else:
#        ''

#REGION REACCION PROMESA CANCELACION #5
#def F_promesa_cancelacion(row):
#    if row['CODIGO ACCION1'] == 'CONTACTO CON CLIENTE -CC' and pd.isnull(row['MONTO PROMESA']):
#        return F_contacto_sin_promesa
#    else:
#        ''

#REGION REACCION PROMESA PAGO #6
def F_promesa_pago(row):
    if row['CODIGO ACCION1'] == 'PROMETE EN FECHA -PP' and not pd.isnull(row['MONTO PROMESA']):
        return promesa_pago
    return row['REACCION']

#REGION REACCION NO CONTACTO #7
def F_no_contacto(row):
    if row['CODIGO ACCION1'] == 'NO CONTACTO -NO':
        return no_contacto
    return row['REACCION']

#REGION REACCION RENUENTE A PAGAR #8
def F_renuente_pagar(row):
    if row['CODIGO ACCION1'] == 'RENUENTE A PAGAR -RP':
        return renuente_pagar
    return row['REACCION']

#REGION REACCION SEGMENTACION #9
def F_segmentacion(row):
    if row['CODIGO LABOR'] == 'REGISTRO' or row['CODIGO LABOR'] == 'LOCALIZACION':
        return segmentacion
    return row['REACCION']

#REGION REACCION CLIENTE PAGO #10
def F_cliente_pago(row):
    if row['CODIGO ACCION1'] == 'CLIENTE PAGO -YP':
        return cliente_pago
    return row['REACCION']


# Aplicar la función para calcular la columna REACCION
df['REACCION'] = df.apply(F_segmentacion, axis=1)         #9
df['REACCION'] = df.apply(F_contacto_indirecto, axis=1)   #1
df['REACCION'] = df.apply(F_contacto_sin_promesa, axis=1) #2
df['REACCION'] = df.apply(F_fallecido, axis=1)            #3
#df['REACCION'] = df.apply(F_no_cumple_promesa, axis=1)    #4
#df['REACCION'] = df.apply(F_promesa_cancelacion, axis=1)  #5
df['REACCION'] = df.apply(F_promesa_pago, axis=1)         #6
df['REACCION'] = df.apply(F_no_contacto, axis=1)          #7
df['REACCION'] = df.apply(F_renuente_pagar, axis=1)       #8
df['REACCION'] = df.apply(F_cliente_pago, axis=1)         #10
# ------------------------------------------------------------
# Fin de la sección de procesamiento
# ------------------------------------------------------------


# ------------------------------------------------------------
# Sección de promesas de pago
# ------------------------------------------------------------
# Columnas nuevas 
df['FECHA DE PROMESA'] = None
df['MONTO DE PROMESA'] = None

# DF para las gestiones relacionadas a promesas de pago
df_gestiones_promesas = df[df['REACCION'] == 'PP'].copy()

# Extraer los valores únicos de NUMERO RELACIONAL
numeros_relacionales = df_gestiones_promesas['NUMERO RELACIONAL'].unique()

# Convertir los valores a una cadena de texto para la consulta SQL
numeros_relacionales_str = ','.join(f"'{num}'" for num in numeros_relacionales)

# CONSULTA SQL A LA TABLA PROMESAS DE PAGO
consulta = f''' 
SELECT 
    [NUMERO RELACIONAL], 
    [TIPO DE PROMESA], 
    [FECHA PROMESA], 
    [MONTO PROMESA] as [MONTO P]
FROM 
    [PROMESAS DE PAGO]
WHERE 
    [NUMERO RELACIONAL] IN ({numeros_relacionales_str})
'''

# Ejecutar la consulta y almacenar los resultados en un DataFrame
df_promesas_pago = pd.read_sql_query(consulta, engine)


# 1. Merge df_gestiones_promesas with df_promesas_pago
df_actualizado = df_gestiones_promesas.merge(df_promesas_pago, on='NUMERO RELACIONAL', how='left')

# 2. Iterar sobre los registros de df_actualizado y actualizar los valores en df
for idx, row in df_actualizado.iterrows():
    df.loc[df['Id'] == row['Id'], 'DETALLE'] = row['TIPO DE PROMESA']
    df.loc[df['Id'] == row['Id'], 'FECHA DE PROMESA'] = row['FECHA PROMESA']
    df.loc[df['Id'] == row['Id'], 'MONTO DE PROMESA'] = row['MONTO P']
# ------------------------------------------------------------
# Fin Sección de promesas de pago
# ------------------------------------------------------------

# ------------------------------------------------------------
# Inicio Sección de Pagos
# ------------------------------------------------------------
# Columnas nuevas 
df['NUMERO DEPOSITO'] = None
df['BANCO'] = None
df['FECHA DEPOSITO'] = None
df['MONTO DEPOSITO'] = None

# DF para las gestiones relacionadas a pagos
df_gestiones_pagos = df[df['REACCION'] == 'YP'].copy()

# Extraer los valores únicos de NUMERO RELACIONAL
numeros_relacionales = df_gestiones_pagos['NUMERO RELACIONAL'].unique()

# Convertir los valores a una cadena de texto para la consulta SQL
numeros_relacionales_str = ','.join(f"'{num}'" for num in numeros_relacionales)

# CONSULTA SQL A LA TABLA PROMESAS DE PAGO
if numeros_relacionales_str:
    # CONSULTA SQL A LA TABLA PAGOS
    consulta = f''' 
    SELECT 
        [NUMERO RELACIONAL], 
        [MONTO PAGO], 
        [TIPO PAGOS], 
        [FECHA PAGO],
        [FECHA APLICACION PAGO],
        [OBSERVACION], 
        [EXPEDIENTE] 
    FROM 
        [PAGOS]
    WHERE 
        [NUMERO RELACIONAL] IN ({numeros_relacionales_str})
    '''
    # Ejecutar la consulta y almacenar los resultados en un DataFrame
    df_pagos = pd.read_sql_query(consulta, engine)

    # Verificar si df_pagos no está vacío antes de proceder
    if not df_pagos.empty:
        # 1. Merge df_gestiones_pagos with df_pagos
        df_actualizado2 = df_gestiones_pagos.merge(df_pagos, on='NUMERO RELACIONAL', how='left')

        # 2. Iterar sobre los registros de df_actualizado y actualizar los valores en df
        for idx, row in df_actualizado2.iterrows():
            df.loc[df['Id'] == row['Id'], 'NUMERO DEPOSITO'] = row['EXPEDIENTE']
            df.loc[df['Id'] == row['Id'], 'DETALLE'] = row['TIPO PAGOS']
            df.loc[df['Id'] == row['Id'], 'FECHA DEPOSITO'] = row['FECHA PAGO']
            df.loc[df['Id'] == row['Id'], 'MONTO DEPOSITO'] = row['MONTO PAGO']
            df.loc[df['Id'] == row['Id'], 'BANCO'] = row['OBSERVACION']
# ------------------------------------------------------------
# Fin Sección de Pagos
# ------------------------------------------------------------


# ------------------------------------------------------------
# INICIO AÑADIR DETALLES RESTANTES
# ------------------------------------------------------------
# Columnas nuevas 
df['DETALLE'] = None

numeros_relacionales = df['NUMERO RELACIONAL'].unique()
# Convertir los valores a una cadena de texto para la consulta SQL
numeros_relacionales_str = ','.join(f"'{num}'" for num in numeros_relacionales)

# CONSULTA SQL A LA TABLA PAGOS
consulta = f''' 
SELECT 
    [NUMERO RELACIONAL], 
    [GESTION], 
    [FECHA DE GESTION]
FROM 
    [HISTORICO GESTIONES]
WHERE 
    [NUMERO RELACIONAL] IN ({numeros_relacionales_str}) AND
    CONVERT(date, [FECHA DE GESTION], 103) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
'''

# Ejecutar la consulta y almacenar los resultados en un DataFrame
df_Historico_Gestiones = pd.read_sql_query(consulta, engine)

# Crear un diccionario para almacenar los detalles concatenados
detalles_dict = {}

# Recorrer df_Historico_Gestiones y concatenar los valores de GESTION
for idx, row in df_Historico_Gestiones.iterrows():
    numero_relacional = row['NUMERO RELACIONAL']
    gestion = row['GESTION']
    if numero_relacional in detalles_dict:
        detalles_dict[numero_relacional] += f", {gestion}"
    else:
        detalles_dict[numero_relacional] = gestion

# Actualizar la columna DETALLE en el DataFrame original df
df['DETALLE'] = df['NUMERO RELACIONAL'].map(detalles_dict)
# ------------------------------------------------------------
# FIN AÑADIR DETALLES RESTANTES
# ------------------------------------------------------------


# ------------------------------------------------------------
# CONVERGER INFORMACION
# ------------------------------------------------------------
df_FINAL['REACCION_CODIGO'] = df['REACCION']
df_FINAL['GES_INICIO_GESTION'] = df['FECHA INICIO GESTION']
df_FINAL['GES_INICIO_GESTION'] = pd.to_datetime(df['FECHA INICIO GESTION'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y %I:%M:%S %p')
df_FINAL['GES_FINAL_GESTION'] = pd.to_datetime(df['FECHA FIN GESTION'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y %I:%M:%S %p')
#df_FINAL['GES_INICIO_GESTION'] = df['FECHA INICIO GESTION']
#df_FINAL['GES_FINAL_GESTION'] = df['FECHA FIN GESTION']
df_FINAL['GES_DETALLE'] = df['DETALLE']
df_FINAL['OPE_NUMERO_OPERACION'] = df['CUENTA']
df_FINAL['PPR_FECHA_PROMESA_PAGO'] = pd.to_datetime(df['FECHA DE PROMESA'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y %I:%M:%S %p')
#df_FINAL['PPR_FECHA_PROMESA_PAGO'] = df['FECHA DE PROMESA']
df_FINAL['PPR_MONTO_PROMESA_PAGO'] = df['MONTO DE PROMESA']
df_FINAL['BAN_BANCO'] = df['BANCO']
df_FINAL['GES_NUMERO_DEPOSITO'] = df['NUMERO DEPOSITO']
df_FINAL['GES_FECHA_DEPOSITO'] = pd.to_datetime(df['FECHA DEPOSITO'], format='%d/%m/%Y %H:%M:%S').dt.strftime('%d/%m/%Y %I:%M:%S %p')
#df_FINAL['GES_FECHA_DEPOSITO'] = df['FECHA DEPOSITO']
df_FINAL['GES_MONTO_DEPOSITO'] = df['MONTO DEPOSITO']
df_FINAL['MEDIO_DE_CONTACTO'] = df['CONTACTO DIRECTO']
df_FINAL['FILA'] = df['Id']
# ------------------------------------------------------------
# CONVERGER INFORMACION
# ------------------------------------------------------------

# Exportar el DataFrame a un archivo Excel
df_FINAL.to_excel('resultado_final.xlsx', index=False)
os.startfile('resultado_final.xlsx')
