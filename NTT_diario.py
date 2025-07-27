"""
"Estamos realizando llamadas válidas al endpoint /timeseries/prices/ESP con diferentes entityId como PMD, PRDVPESU, PRDVPEBA y combinaciones con hourly/quarterly y version=A1, pero todas las respuestas son exitosas y vienen con estructura correcta, sin embargo values siempre está vacío. ¿Podrían confirmar si hay datos disponibles para estas combinaciones o si debemos usar otros identificadores, versiones o fechas?"
"""

import requests
import datetime
import pandas as pd
import json
import pytz
import os, sys
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 100)


# === TOKEN ===

def get_token(auth_url, auth_payload):
    res = requests.post(auth_url, json=auth_payload)
    res.raise_for_status()
    return res.json()["access_token"]


# === CONSULTA DE PRECIOS CON ENTITYID = PMD ===
def get_prices_pmd(token, start_dt, end_dt, resolution="Hourly", zone='ESP'):
    headers = {"Authorization": f"Bearer {token}"}
    
    # Fecha de hoy en Europa/Madrid
    tz = pytz.timezone("Europe/Madrid")
    start_dt = tz.localize(datetime.datetime.combine(start_dt, datetime.time(0, 0)))
    end_dt   = tz.localize(datetime.datetime.combine(end_dt, datetime.time(23, 59)))
    
    start = start_dt.isoformat()
    end   = end_dt.isoformat()
    
    url = f"https://solaria-verticalpower-api.emeal.nttdata.com/timeseries/prices/{zone}"
    params = {
        "entityType":   "market",
        "entityId":     "PMD",
        "resolutionId": resolution,
        "startDateTime": start,
        "endDateTime":   end
    }

    res = requests.get(url, headers=headers, params=params, timeout=15)
    
    # Si viene nuevo token, actualizamos y reintentamos UNA sola vez
    if "Authorization" in res.headers:
        nuevo = res.headers["Authorization"].replace("Bearer ", "")
        headers["Authorization"] = f"Bearer {nuevo}"
        res = requests.get(url, headers=headers, params=params, timeout=15)
    
    res.raise_for_status()
    return res.json()


# === Transformar la salida de la API en un DataFrame ===
def precios_to_dataframe(precios):

    # La respuesta puede ser una lista o dict según versión de API
    if isinstance(precios, list):
        data = precios[0].get("Lista", {})
    else:
        data = precios.get("Lista", {})

    header = data.get("header", {})
    valores = data.get("values", [])
    print("=== HEADER DE LA RESPUESTA ===")
    print(json.dumps(header, indent=2, ensure_ascii=False))


    if valores:
        tabla = pd.DataFrame(valores)
        # Convierte dateTime a datetime real
        tabla["dateTime"] = pd.to_datetime(tabla["dateTime"])
        # print("\n=== TABLA DE PRECIOS (PMD, Hourly) PARA HOY ===")
        # print(tabla)

        return tabla
    
    print("\n⚠️ No hay valores disponibles para entityId=PMD en el día de hoy.")
    return None


# === Ajustar el formato de los datos para la tabla omie.diario_qh ===
def tratamiento_dataframe_sql(df, price_column_name='Spain'):
    df_omie_sql = df.copy()

    df_omie_sql['Year'] = df_omie_sql['dateTime'].dt.year
    df_omie_sql['Month'] = df_omie_sql['dateTime'].dt.month
    df_omie_sql['Day'] = df_omie_sql['dateTime'].dt.day
    df_omie_sql['Hour'] = df_omie_sql['dateTime'].dt.hour + 1

    # Agregamos la zona correspondiente
    df_omie_sql['Zone'] = price_column_name
    print(df_omie_sql['updateDate'].unique())

    df_omie_sql.drop(columns=['dateTime', 'unit', 'updateDate'], inplace=True)

    # Actualmente los datos están en formato horario, así que creamos 4 periodos para cada hora
    minutes_df = pd.DataFrame({'Minute': [0, 15, 30, 45]})
    df_omie_sql = df_omie_sql.merge(minutes_df, how='cross')

    df_omie_sql['Periodo'] = (df_omie_sql['Hour']-1)*4 + df_omie_sql['Minute']//15 + 1
    df_omie_sql.drop(columns=['Minute'], inplace=True)
    print(df_omie_sql)

    return df_omie_sql



def proceso_completo_extraccion_modificado(start_dt, end_dt, auth_url, auth_payload):
    token = get_token(auth_url, auth_payload)

    df_omie_global = pd.DataFrame()
    # Lista de códigos de zona
    zone_codes = ['ESP', 'PT']
    # Nombres de las zonas -> se utilizarán para nombrar las columnas
    zone_names = ['Spain', 'Portugal']

    for (zone_code, zone_name) in zip(zone_codes, zone_names):
        # Obtenemos los datos de PMD de cada zona
        precios = get_prices_pmd(token, start_dt, end_dt, zone=zone_code)
        tabla = precios_to_dataframe(precios)
        df_omie = tratamiento_dataframe_sql(tabla, price_column_name=zone_name)

        # Agregamos los datos a la tabla global
        df_omie_global = pd.concat([df_omie_global, df_omie], ignore_index=True)

    # Hacemos pivot -> transformamos los valores de la columna Zone en columnas distintas, asignando el precio como valor
    df_omie_global_pivot = df_omie_global.pivot(index=['Year', 'Month', 'Day', 'Hour', 'Periodo'], columns='Zone', values='val').reset_index()
    df_omie_horario = df_omie_global_pivot.groupby(['Year', 'Month', 'Day', 'Hour'], as_index=False)[['Spain', 'Portugal']].mean()

    return df_omie_horario  


# === EJECUCIÓN ===
if __name__ == "__main__":
    pass

