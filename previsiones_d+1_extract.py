import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, time
import requests
import time
import requests
import json
import pandas as pd

pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)  # Sin límite de columnas visibles
pd.set_option('display.width', 0)  # Configurar ancho dinámico para la pantalla
pd.set_option('display.max_rows', 100)

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.connector import insertar_dataframe_en_mysql, enviar_email_alerta


def get_esios_data_raw(indicator_id, start_date, end_date, api_key, locale='es'):
    """
    Obtiene datos RAW de la API de ESIOS para un indicador. Dejamos la agregación y truncamiento por defecto.

    Args:
        indicator_id (int): ID del indicador.
        start_date (str): Fecha de inicio en formato ISO8601 (YYYY-MM-DD o YYYY-MM-DDThh:mm:ssZ).
        end_date (str): Fecha de fin en formato ISO8601.
        api_key (str): Clave API de ESIOS.
        locale (str): Idioma de la respuesta ('es' o 'en'). Por defecto, 'es'.

    Returns:
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
        Output: df []
    """

    values = data.get('indicator', {}).get('values', [])
    if not values:
        return pd.DataFrame()
    
    magnitud_id = data['indicator']['magnitud'][0]['id']
    indicator_id = data['indicator']['id']

    
    df = pd.DataFrame(values)

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


api_key='80f5d7a72865deb8abcf624050070a6607ad24b6dc5f1ad24ba1cf06327000ef'

indicator_previsiones = [1775, 1777, 1779]

if __name__ == "__main__":
    end_date = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')


    for indicator_id in indicator_previsiones:
        actualizado = False
        n_intentos = 0
        max_intentos = 15
        t_espera = 60
        while not actualizado and n_intentos < max_intentos:
            n_intentos += 1
            esios_data = get_esios_data_raw(indicator_id, start_date, end_date, api_key=api_key, locale='es')

            update_date = esios_data['indicator']['values_updated_at']
            update_date = datetime.strptime(update_date, "%Y-%m-%dT%H:%M:%S.%f%z")
            print('Update date = ', update_date)

            df_esios = procesamiento_raw_data(esios_data, geo_ids=[8741])
            print(df_esios.tail(5))

            if update_date.date() == datetime.now().date():
                print('Actualizado')
                actualizado = True
                insertar_dataframe_en_mysql(df_esios, 't_api_esios_indicadores_data', 'esios')
            
            else:
                time.sleep(t_espera)
                print(f'No se actualizo el indicador {indicator_id}. Intentando nuevamente en {t_espera} segundos.')

        if not actualizado:
            enviar_email_alerta(
                asunto='ESIOS - Extracción previsiones D+1 fallida',
                mensaje=f"No se pudo extraer la previsión D+1 del indicador {indicator_id} en los últimos días. Ultimo intento: {datetime.now().hour}:{datetime.now().minute}:{datetime.now().second}",
                destinatario='jnavarro@solariaenergia.com'
            )