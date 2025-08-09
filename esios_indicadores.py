from math import e
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, time
import requests
import requests
import json
import pandas as pd

pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)  # Sin límite de columnas visibles
pd.set_option('display.width', 0)  # Configurar ancho dinámico para la pantalla
pd.set_option('display.max_rows', 100)

import os, sys
sys.path.append(r"C:\Users\jnavarro\Solaria Energía y Medio Ambiente\00-GEN - Documentos\Base de Datos\python")
from utils.connector import execute_query, insertar_dataframe_en_mysql


def get_esios_data_raw(indicator_id, start_date, end_date, geo_ids, api_key, locale='es', time_agg=None, time_trunc=None, geo_agg=None, geo_trunc=None):
    """
    Obtiene datos RAW de la API de ESIOS para un indicador y una o varias regiones geográficas.

    Args:
        indicator_id (int): ID del indicador.
        start_date (str): Fecha de inicio en formato ISO8601 (YYYY-MM-DD o YYYY-MM-DDThh:mm:ssZ).
        end_date (str): Fecha de fin en formato ISO8601.
        geo_ids (int | list[int]): ID o lista de IDs de las regiones geográficas.
        api_key (str): Clave API de ESIOS.
        locale (str): Idioma de la respuesta ('es' o 'en'). Por defecto, 'es'.
        time_agg (str, opcional): Agregación temporal ('sum' o 'average').
        time_trunc (str, opcional): Truncamiento temporal ('five_minutes', 'ten_minutes', 'fifteen_minutes', 'hour', 'day', 'month', 'year').
        geo_agg (str, opcional): Agregación geográfica ('sum' o 'average').
        geo_trunc (str, opcional): Truncamiento geográfico ('country', 'electric_system', 'autonomous_community',
                                                         'province', 'electric_subsystem', 'town', 'drainage_basin').

    Returns:
        dict | None: Diccionario con la respuesta JSON en caso de éxito, o None si falla.
    """

    # Validación de parámetros obligatorios
    if not all([indicator_id, start_date, end_date, geo_ids]):
        raise ValueError("Los parámetros 'indicator_id', 'start_date', 'end_date' y 'geo_ids' son obligatorios.")


    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    headers = {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": api_key
    }

    # Asegurarnos de que geo_ids sea una lista de strings
    if isinstance(geo_ids, int):
        geo_ids = [str(geo_ids)]
    elif isinstance(geo_ids, list):
        geo_ids = [str(g) for g in geo_ids]
    else:
        raise ValueError("El parámetro 'geo_ids' debe ser un entero o una lista de enteros.")

    # Construcción de parámetros de consulta
    params = {
        "start_date": start_date,
        "end_date": end_date,
        # "geo_ids": ','.join(geo_ids),
        "locale": locale
    }

    if time_agg:
        params["time_agg"] = time_agg
    if time_trunc:
        params["time_trunc"] = time_trunc
    if geo_agg:
        params["geo_agg"] = geo_agg
    if geo_trunc:
        params["geo_trunc"] = geo_trunc

    
    try:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return None


def procesamiento_indicador_data(data, geo_ids):
    values = data.get('indicator', {}).get('values', [])
    if not values:
        print(f"No hay valores para el rango dado.")
        return pd.DataFrame()
    
    df = pd.DataFrame(values)

    df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
    df['fecha'] = df['datetime'].dt.date
    df['Hour'] = df['datetime'].dt.hour

    tiempo = data['indicator']['tiempo'][0]['name']
    if tiempo == 'Hora':
        minutes = pd.DataFrame({'Minute': [0, 15, 30, 45]})
        df = pd.merge(df, minutes, how='cross')
        df['Periodo'] = df['Hour'] * 4 + df['Minute'] // 15 + 1
        df['value'] /= 4

    elif tiempo == 'Cinco minutos' or tiempo == 'Quince minutos':
        df['Minute'] = df['datetime'].dt.minute
        df['Periodo'] = df['Hour'] * 4 + df['Minute'] // 15 + 1
    else:
        print(f"Tiempo no valido: {tiempo}")
        return pd.DataFrame()
    
    df['Hour'] = df['Hour'] + 1
    # Añadir metadatos del indicador
    df['indicator_id'] = data['indicator'].get('id')

    df = df[['indicator_id', 'fecha', 'Hour', 'Periodo', 'geo_id', 'value']]

    df = df.loc[df['geo_id'].isin(geo_ids)]
    return df


def procesamiento_indicador_data_horario(data, geo_ids):
    values = data.get('indicator', {}).get('values', [])
    if not values:
        print(f"No hay valores para el rango dado.")
        return pd.DataFrame()
    
    df = pd.DataFrame(values)

    df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
    df['Date'] = df['datetime'].dt.date
    df['Hour'] = df['datetime'].dt.hour + 1

    tiempo = data['indicator']['tiempo'][0]['name']
    if tiempo != 'Hora':
        print(f"Tiempo no valido: {tiempo}")
        return pd.DataFrame()
    
    df['indicator_id'] = data['indicator'].get('id')

    df = df[['indicator_id', 'Date', 'Hour', 'geo_id', 'value']]
    df['Date'] = pd.to_datetime(df['Date']).dt.normalize()

    df = df.loc[df['geo_id'].isin(geo_ids)]
    return df


def list_geo_ids(data):
    geo_list = data['indicator']['geos']
    df = pd.DataFrame(geo_list)

    return df


def omie_data_mysql(start_date, end_date):
    # Mercado Diario
    query = "SELECT year, month, day, Hour, Spain as value FROM omie.diario"
    df_omie_md = execute_query(query, 'omie')
    df_omie_md[['year', 'month', 'day', 'Hour']] = df_omie_md[['year', 'month', 'day', 'Hour']].astype(int)
    df_omie_md['Date'] = pd.to_datetime(df_omie_md[['year', 'month', 'day']]).dt.normalize()

    df_omie_md['indicator_id'] = 600
    df_omie_md['geo_id'] = 3

    df_omie_md = df_omie_md[['indicator_id', 'Date', 'Hour', 'geo_id', 'value']]
    df_omie_md = df_omie_md.loc[(df_omie_md['Date'] >= start_date) & (df_omie_md['Date'] <= end_date)]

    
    return df_omie_md


def perfil_solar_estandar(year_start, year_end):
    query = "SELECT month, day, Hour, perfil AS value FROM omie.perfil_solar_estandar"
    df_perfil = execute_query(query, 'omie')
    df_perfil = df_perfil.merge(pd.DataFrame({'year':[x for x in range(year_start, year_end+1)]}), how='cross')
    df_perfil[['year', 'month', 'day']] = df_perfil[['year', 'month', 'day']].astype(int)

    df_perfil['Date'] = pd.to_datetime(df_perfil[['year', 'month', 'day']]).dt.normalize()
    df_perfil['indicator_id'] = 'Perfil'
    df_perfil['geo_id'] = 3

    df_perfil = df_perfil[['indicator_id', 'Date', 'Hour', 'geo_id', 'value']]
    return df_perfil




if __name__ == "__main__":

    api_key='80f5d7a72865deb8abcf624050070a6607ad24b6dc5f1ad24ba1cf06327000ef'
    # Lista de indicadores de interés
    indicator_precios = [600, 612, 613, 614, # Diario, Intradiario
                         634, 2130, # Reserva sec
                         682, 683, # Energia sec
                         686, 687, # Desvios
                         1782, 716, 717, 2197]  #  RR, Terciaria
    indicator_volumen = [632, 633, 674, 675, 680, 681, 1783, 1784]
    indicator_demanda = [544, 545, 1293] # Demanda prevista, programada y real
    indicator_generacion = [551, 552]
    indicator_pvpc = [1001]
    indicator_interconexiones = [10026] # Saldo total interconexiones
    indicator_previsiones = [1775, 1777, 1779] # Demanda, Eolica, Fotovoltaica - previsiones D+1
    
    indicator_ids = indicator_demanda + indicator_generacion + indicator_pvpc + indicator_interconexiones + indicator_previsiones

    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    geo_ids = [8741, 2, 3]
    time_trunc = 'fifteen_minutes'
    time_agg = 'sum'


    # query = "SELECT id FROM esios.t_mst_esios_listado_indicadores"
    # df_id = execute_query(query, 'esios')
    # indicator_ids_full = df_id['id'].tolist()




    # PREVISIONES d+1
    indicator_ids = indicator_previsiones + [612, 613]
    time_trunc = 'hour'
    geo_ids = [8741, 3]
    df_previsiones = pd.DataFrame()


    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)

    for indicator_id in indicator_ids:
        data = get_esios_data_raw(indicator_id, start_date, end_date, geo_ids, api_key, time_agg=time_agg, time_trunc=time_trunc)
        # print(data)
        if data is None:
            print(f"No se pudo obtener datos para el indicador {indicator_id}.")
            continue

        df = procesamiento_indicador_data_horario(data, geo_ids)
        print(df.head(5))

        df_previsiones = pd.concat([df_previsiones, df])

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 6, 30)

    for indicator_id in indicator_ids:
        data = get_esios_data_raw(indicator_id, start_date, end_date, geo_ids, api_key, time_agg=time_agg, time_trunc=time_trunc)
        # print(data)
        if data is None:
            print(f"No se pudo obtener datos para el indicador {indicator_id}.")
            continue

        df = procesamiento_indicador_data_horario(data, geo_ids)
        print(df.head(5))

        df_previsiones = pd.concat([df_previsiones, df])    


    df_omie_md = omie_data_mysql(datetime(2022, 1, 1), datetime(2025, 6, 30))
    print(df_omie_md.head(5))

    df_previsiones_omie = pd.concat([df_previsiones, df_omie_md])

    ruta_dir = os.path.dirname(os.path.abspath(__file__))
    df_previsiones_omie.to_csv(f"{ruta_dir}/data_training/esios_previsiones_d+1.csv", index=False, date_format='%Y-%m-%d')


    