from re import I
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, time, date
import requests
import json
import time as time_module
import os
import sys

# Configuración de Pandas
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)  # Sin límite de columnas visibles
pd.set_option('display.width', 0)  # Configurar ancho dinámico para la pantalla
pd.set_option('display.max_rows', 100)

# Añadir ruta al path
#DEPENDIENDO DE SI LO EJECUTAS DESDE UNA CARPETA DEL SHAREPOINT O DESDE UNA RUTA EN LOCAL SE AÑADE UN PATH U OTRO
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) #desde sharepoint
sys.path.append(r"C:\Users\llopez\Solaria Energía y Medio Ambiente\00-GEN - Documentos\Base de Datos\python") #desde local

# Importar utilidades propias
from utils.connector import execute_query, insertar_dataframe_en_mysql, update_dataframe_en_mysql
from utils.format import *

###################################################################################################################################################################################################
# FUNCIONES PRINCIPALES PARA OBTENER LOS DATOS DE INDICADORES EN LA API DE ESIOS
###################################################################################################################################################################################################

#Las funciones que aparecen en este script se han recogido de los siguientes .py:
#   - "Solaria Energía y Medio Ambiente\00-GEN - Documentos\Base de Datos\python\API_Esios\esios_indicadores.py"
#   - "Solaria Energía y Medio Ambiente\00-GEN - Documentos\Base de Datos\python\API_Esios\inserccion_bbdd_indicador600.py"
#   - "Solaria Energía y Medio Ambiente\00-GEN - Documentos\Base de Datos\python\API_Esios\inserccion_bbdd_varios_indicadores.py"


def periods_of_time_for_each_api_call(start_date, end_date):
     """
     Dado un rango de fechas, obtenemos una lista de periodos de 2 meses (máximo permitido por la API en una sola llamada)

     INPUTS:
     -------
     - start_date (str): Inicio del periodo de búsqueda de datos
     - end_date(str): Fin del periodo de busqueda de datos

     OUTPUT:
     -------
     - period: (list[list]): Lista con la división del rango de fechas en periodos de 5 meses

     """
     start_date = pd.to_datetime(start_date)
     end_date = pd.to_datetime(end_date)
     if (end_date.month - start_date.month <= 2):
          #Se pueden obtener los datos en un sola llamada a API
          return [[str(start_date), str(end_date)]]
     else:
            periods = []
            current_start = start_date
            while current_start < end_date:
                 current_end = current_start + pd.DateOffset(months=2)
                 if current_end > end_date:
                        current_end = end_date
                 periods.append([str(current_start), str(current_end)])
                 current_start = current_end + pd.DateOffset(days=1)
            return periods

def get_esios_data_raw(indicator_id, start_date, end_date, api_key, locale='es'):
    """
    Obtiene datos RAW de la API de ESIOS para un indicador. Dejamos la agregación y truncamiento por defecto.

    INPUTS:
    -------
        indicator_id (int): ID del indicador.
        start_date (str): Fecha de inicio en formato ISO8601 (YYYY-MM-DD o YYYY-MM-DDThh:mm:ssZ).
        end_date (str): Fecha de fin en formato ISO8601.
        api_key (str): Clave API de ESIOS.
        locale (str): Idioma de la respuesta ('es' o 'en'). Por defecto, 'es'.

    OUTPUTS:
    --------
        dict | None: Diccionario con la respuesta JSON en caso de éxito, o None si falla.
    """


    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    headers = {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": api_key
    }

    # Construcción de parámetros de consulta
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "locale": locale
    }

    
    try:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return None
    

def procesamiento_raw_data(data, geo_ids=None):
    """
        A partir del archivo json obtenido de la API, genera un DataFrame y filtra por geo_ids si se especifica.

        INPUTS:
        -------
            data: JSON con los datos del indicador obtenidos a través de ESIOS
            geo_ids: lista de geo_id por los que filtrar (list[str])

        OUTPUT:
        -------
            df: dataframe con los datos del indicador procesados (DataFrame)
    """

    values = data.get('indicator', {}).get('values', [])
    if not values:
        return pd.DataFrame()
    
    magnitud_id = data['indicator']['magnitud'][0]['id']
    indicator_id = data['indicator']['id']

    
    df = pd.DataFrame(values)
    #print(df)

    df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], utc=True)

    df['datetime'] = df['datetime'].dt.tz_localize(None)
    df['datetime_utc'] = df['datetime_utc'].dt.tz_localize(None)

    df = df[['datetime', 'datetime_utc', 'geo_id', 'value']]
    df['indicator_id'] = indicator_id
    df['magnitud_id'] = magnitud_id

    if geo_ids:
        df = df.loc[df['geo_id'].isin(geo_ids)]

    return df

def update_geo_ids_table(data):
    """
    Inserta en bbdd el geo_id y el nombre de la región geográfica asociada de todos los geo_ids que aparecen en data
    
    INPUTS:
    -------
        data: JSON
    
    OUTPUTS:
    --------
        None
    """
    geo_list = data['indicator']['geos']
    df = pd.DataFrame(geo_list)

    df.rename(columns={'id': 'geo_id', 'name': 'geo_name'}, inplace=True)
    insertar_dataframe_en_mysql(df, 't_rel_esios_geo', 'esios')


def update_magnitud_ids_table(data):
    """
    Inserta en bbdd la magnitud_id y el nombre de la magnitud asociada de todas las magnitud_id que aparecen en data
    
    INPUTS:
    -------
        data: JSON
    
    OUTPUTS:
    --------
        None
    """
    magnitud_list = data['indicator']['magnitud']
    df = pd.DataFrame(magnitud_list)

    df.rename(columns={'id': 'magnitud_id', 'name': 'magnitud_name'}, inplace=True)
    insertar_dataframe_en_mysql(df, 't_rel_esios_magnitud', 'esios')


def carga_masiva_datos(start_date, end_date, api_key, indicator_id, geo_ids=[8741, 3]):
    """
    Carga en bbdd los datos asociados al indicador especificado, en el rango de fechas determinado. La extracción de los datos se hace a trravés de la API de ESIOS

    INPUTS:
    ------
        start_date: fecha de inicio de recopilación de datos (Datetime)
        end_date: fecha de fin de recopilación de datos (Datetime)
        api_key: api key para hacer la llamada a la API de ESIOS (str)
        indicator_id: identificador del indicador para hacer la llamada a la Api (int)
        geo_ids: lista de los indicadores geográficos por los que filtrar la información del indicador. Por defecto, se buscan en la Península y en España (list[int])

    """

    rango_fechas = pd.date_range(start_date, end_date, freq='MS')
    df_global = pd.DataFrame()

    for i in range(len(rango_fechas)-1):
        start = rango_fechas[i]
        end = rango_fechas[i+1]
        print(f"\n\nProcesando rango de fechas {start} a {end}")

        data = get_esios_data_raw(indicator_id, start, end, api_key)

        if data is None:
            print(f"No se pudo obtener datos para el indicador {indicator_id}.")
            continue

        update_geo_ids_table(data)
        update_magnitud_ids_table(data)
        
        df = procesamiento_raw_data(data, geo_ids)
        print(df.head(3))
        print(df.tail(3))

        df_global = pd.concat([df_global, df], ignore_index=True)
    
    return df_global


def obtencion_y_estructura_indicador(indicator_id, start_date_total, end_date_total, api_key, geo_ids=[8741], max_retries=3, retry_wait=15):
    """
    Obtiene los datos del indicador especificado en el rango de fechas especificado y filtrando por geo_ids. Una vez obtenidos los datos de Esios, se realiza una pequeña
    limpieza para facilitar la insercción de los datos en la tabla de bbdd.

    INPUTS
    ------
        indicator_id: id del indicador (int)
        start_date_total:  Inicio del periodo de búsqueda de datos (datetime)
        end_date_total: Fin del periodo de busqueda de datos (datetime)
        api_key: Api key para las llamadas a la api de ESIOS (str)
        geo_ids: Lista que contienen los geo ids en los que obtener el indicador (list[str])
        max_retries: Número máximo de reitentos para obtener los datos del indicador (int)
        retry_wait: Tiempo de espera (en segundos) entre reintentos (int)

    OUTPUTS
    -------
        data_indicador: dataframe que contiene la información del indicador en el rango especificado tras haber aplicado cierto preprocesado para su insercción en bbdd (df) 
    """
    #Obtenemos los valoress del indicador para el rango especificado
    periods = periods_of_time_for_each_api_call(start_date_total, end_date_total)
    data_total = pd.DataFrame()
    print("")
    print(f"Procesando indicador {indicator_id}")
    print("--------------------------------------")
    for period in periods:
        start_date = period[0]
        end_date = period[1]
        success = False
        attempt = 0

        # Añadimos reintentos automáticos
        while not success and attempt < max_retries:
            attempt += 1
            print(f"Intento {attempt}/{max_retries} para el periodo {period}")
            try:
                data = get_esios_data_raw(indicator_id, start_date, end_date, api_key)
                #Caso en el que no se ha creado el diccionario data o se encuentra vací (volverá a intentar la peticción mientras que no hayamos llegado al máximo de intentos)
                if data is None or not data:
                    print(f"No se pudo obtener datos para el indicador {indicator_id} en el periodo {period}.")
                else:
                    print(f"Obtención de datos exitosa del periodo {period}")
                    #print(" ")
                    df_data = pd.DataFrame(data)
                    df_data = procesamiento_raw_data(data, geo_ids)
                    data_total = pd.concat([data_total, df_data], ignore_index=True)
                    if data_total.empty:
                        print(f"No se ha encontrado ningún valor del indicador {indicator_id} para el rango de fechas {start_date_total} - {end_date_total}")
                    else:
                        #Solo consideraremos como obtención exitosa si se ha obtenido valores del indicador, en cualquier otro caso se vuelve a intentar hasta llegar a 3 intentos
                        success = True
                        break
            except Exception as e:
                print(f"Error en intento {attempt} del indicador {indicator_id}: {e}")

            if not success:
                wait_time = retry_wait * attempt  # backoff progresivo
                print(f"Esperando {wait_time} segundos antes del siguiente intento...")
                time_module.sleep(wait_time)

        if not success:
            print(f"No se pudo obtener datos tras {max_retries} intentos para el periodo {period}.")

        time_module.sleep(10)

    if data_total.empty:
        print(f"No se ha encontrado ningún valor del indicador {indicator_id} para el rango de fechas {start_date_total} - {end_date_total}")

    else:
        data_total["value"] = data_total["value"].round(2)
        diferencia_entre_registros = data_total["datetime"].iloc[2] - data_total["datetime"].iloc[1]
        diferencia_en_minutos = diferencia_entre_registros.total_seconds() / 60
        #print(diferencia_en_minutos)
        #print(diferencia_en_minutos < 15)

        #Manejar cambio de hora
        data_total['datetime_str'] = pd.to_datetime(data_total['datetime_utc'], utc=True).dt.tz_convert('Europe/Madrid').astype(str)
        data_total['date_str'] = data_total["datetime"].dt.strftime("%Y-%m-%d")
        data_total["Hour"] = data_total["datetime"].dt.hour + 1
        #print("DATA_TOTAL antes de cambio de hora")
        #print(data_total)
        #print("")
        data_total = ajuste_cambio_hora(data_total, col_date='date_str', col_timestamp_str='datetime_str', col_hour='Hour')
        #print("DATA_TOTAL despues de cambio de hora")
        #print(data_total)
        #print("")
        #print(data_total)
        data_total = data_total.drop(columns = ["datetime_utc", "geo_id", "date_str", "datetime_str"])

        if diferencia_en_minutos == 15: 
            #El indicador es de frecuencia quinceminutal asi que no hay que aplicar ningún preprocesado
            data_total["Date"] = data_total['datetime'].dt.date
            #Convertimos la hora a periodo (1-96)
            data_total["Minute"] = data_total["datetime"].dt.minute
            data_total["Period"] = ((data_total["Hour"] - 1)*4 + data_total["Minute"]//15) + 1 
            #Eliminamos las columnas innecesarias. Nos quedamos con las columnas: datetime, value, indicator_id (geo_id lo elimino porque soo nos interesa la Península, 
            #en el momento que nos interesen más geo_ids, habrá que mantener la columna)
            data_indicador = data_total.drop(columns = ["datetime", "Minute", "magnitud_id"])
            return data_indicador
        
        elif diferencia_en_minutos < 15:
            #Sería el caso de indicador con frecuencia cincominutal. Como nos interesa quinceminutal, agrupamos por periodos de 15 mins
            data_total["Date"] = data_total['datetime'].dt.date
            data_total["Minute"] = data_total["datetime"].dt.minute
            data_total["Period"] = ((data_total["Hour"] - 1)*4 + data_total["Minute"]//15) + 1 
            #Para saber si tenemos que agregar por la suma o media, nos fijamos en magnitud_id
            magnitud_id = data_total["magnitud_id"].iloc[0]
            #Dependiendo del tipo de la columna aplicamos la agregación numérica o categórica
            if magnitud_id == 13: 
                #Energía, tenemos que aplicar la suma
                data_total = data_total.groupby(["Date", "Period"], as_index=False).agg({
                    "value": "sum",
                    **{col: "first" for col in data_total.columns if col not in ["Date", "Hour", "Period", "value"]}
                })
                data_total = data_total.reset_index(drop=True)
            elif magnitud_id == 20 or magnitud_id == 23: 
                #Potencia y precio, calculamos la media
                data_total = data_total.groupby(["Date", "Period"], as_index=False).agg({
                    "value": "mean",
                    **{col: "first" for col in data_total.columns if col not in ["Date", "Period", "value"]}
                })
                data_total = data_total.reset_index(drop=True)
            else:
                raise ValueError(f"Tipo de magnitud del indicador distinto de energía, potencia o precio ({magnitud_id})")
        
            #Eliminamos las columnas innecesarias
            data_indicador = data_total.drop(columns = ["datetime", "Minute", "magnitud_id"])
            return data_indicador

        else:
            #Caso de un indicador de frecuencia horaria
            data_total["Date"] = data_total['datetime'].dt.date
            minutes_df = pd.DataFrame({'Minute': [0, 15, 30, 45]})
            data_total = data_total.merge(minutes_df, how = 'cross')
            data_total['Period'] = (data_total["Hour"] - 1)*4 + data_total["Minute"]//15 + 1 
            #Para saber si tenemos que agregar por la suma o media, nos fijamos en magnitud_id
            magnitud_id = data_total["magnitud_id"].iloc[0]
            #Dependiendo del tipo de la columna aplicamos la agregación numérica o categórica
            if magnitud_id == 13: 
                #Energía, tenemos que dividir el valor de esa hora entre 4 periodos
                print(data_total[(data_total["Date"] == date(2025,11,10)) &(data_total["Hour"] == 6)])
                data_total["value"] = (data_total["value"] / 4).round(2)
            elif magnitud_id == 20 or magnitud_id == 23: 
                #Potencia y precio, el valor se mantiene 
                pass
            else:
                raise ValueError(f"Tipo de magnitud del indicador distinto de energía, potencia o precio ({magnitud_id})")
            data_indicador  = data_total.drop(columns = ["datetime", "Minute", "magnitud_id"])
            return data_indicador
        


def creacion_estructura_tabla(indicadores_ids, start_date_total, end_date_total, api_key, nombre_columnas, nombre_tabla = "", nombre_schema = "", geo_ids = [8741], insertar_bbdd = False):
    """ 
    Dados una serie de indicadores, obtiene sus datos en el rango de fechas especificado. Después, cresa un dataframe con la info de todos los indicadores, siguiendo el esquema
    de la table especificada. En el caso de que el parámetro insertar_bbdd sea True, insertael dataframe en bbdd.

    INPUTS
    ------
        indicadores_ids: listsa con los ids de los indicadores a considerar
        Inicio del periodo de búsqueda de datos (datetime)
        end_date_total: Fin del periodo de busqueda de datos (datetime)
        api_key: Api key para las llamadas a la api de ESIOS (str)
        nombre_columnas: Dicccionario con clave el id del indicador y valor el nombre que queramos dar a la columna asociada (dict)
        nombre_tabla: nombre de la tabla de bbdd donde insertar  el dataframe (str)
        nombre_schema: nombre de esquema donde se encuentra la tabla especificada (str)
        geo_ids: Lista que contienen los geo ids en los que obtener el indicador (list[str])
        insertar_bbdd: Booleano que especifica la insercción en bbdd del dataframe obtenido (Bool)

    OUTPUTS
    -------
        data_table: dataframe con las columnas Date, Periiod y una columna por indicador. Es el dataframe que se insertó en bbddd en caso de que insertar_bbdd = True (df)

    """
    
    data_indicadores = pd.DataFrame()
    for indicador in indicadores_ids:
        data_indicador = obtencion_y_estructura_indicador(indicador, start_date_total, end_date_total, api_key, geo_ids)
        data_indicadores = pd.concat([data_indicadores, data_indicador], ignore_index=True)

    #print(data_indicadores)

    #Comprobación de que tenemos un registro único por Date, Hour, Period, indicator_id
    duplicates = data_indicadores.duplicated(
        subset=["Date", "Hour", "Period", "indicator_id"],
        keep=False  # marca *todas* las filas duplicadas, no solo las segundas
    )

    if duplicates.sum() > 0:
        print(f"Número de filas duplicadas: {duplicates.sum()}")
        # Mostrar las filas duplicadas ordenadas para ver mejor los grupos repetidos
        duplicated_rows = data_indicadores[duplicates].sort_values(
            by=["Date", "Hour", "Period", "indicator_id"]
        )
        print(duplicated_rows) 
        raise ValueError("Hay filas con la misma combinación de Date, Hour, Period, geo_id")
    
    else:
        #Pivotamos por indicator_id para obtener una columna por indicador
        data_tabla = (
        data_indicadores
        .pivot(
            index=["Date", "Hour", "Period"],
            columns="indicator_id",
            values="value"
        )
        .reset_index()
        )
        data_tabla.columns.name = None

        #Renombramos las columnas para quedarnos con el nombre del indicador y no el ID
        #query_nombres_ids = """ SELECT 
        #                            id,
        #                            short_name
        #                        FROM esios.t_mst_esios_listado_indicadores;
        #                            """
        #df_nombres_ids = execute_query(query_nombres_ids, "esios")
        #dict_ids_nombres = dict(zip(df_nombres_ids["id"], df_nombres_ids["short_name"]))
        data_tabla = data_tabla.rename(columns = nombre_columnas)
        #Para que los valores de los indicadores aparezcan con una precisión de dos decimales
        data_tabla = data_tabla.round(2)
        print(data_tabla)
        if insertar_bbdd:
         insertar_dataframe_en_mysql(data_tabla, nombre_tabla, nombre_schema)
        else:
            pass
    
        return data_tabla
