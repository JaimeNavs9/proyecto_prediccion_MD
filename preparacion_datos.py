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


def get_df_indicadores_raw(start_date, end_date, indicator_ids):
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    query_indicadores = ", ".join([f"'{x}'" for x in indicator_ids])

    query = f"""SELECT Datetime, indicator_id, geo_id, magnitud_id, value FROM t_api_esios_indicadores_data 
                WHERE Datetime BETWEEN '{start_date_str}' AND '{end_date_str}'
                AND indicator_id IN ({query_indicadores})"""
    df = execute_query(query, 'esios')

    return df


def limpieza_df(df):
    global indicator_id_dict
    df_clean = df.copy()

    df_clean['Datetime'] = pd.to_datetime(df_clean['Datetime'])
    df_clean['Datetime_hour'] = df_clean['Datetime'].dt.strftime('%Y-%m-%d %H:00:00')

    df_precios_potencia = df_clean.loc[df_clean['magnitud_id']!=13]
    df_energia = df_clean.loc[df_clean['magnitud_id']==13]

    # Agrupar por horas
    df_energia_hour = df_energia.groupby(['Datetime_hour', 'indicator_id'], as_index=False)['value'].sum()
    df_precios_potencia_hour = df_precios_potencia.groupby(['Datetime_hour', 'indicator_id'], as_index=False)['value'].mean()

    df_clean_hour = pd.concat([df_energia_hour, df_precios_potencia_hour], axis=0)

    # Hacer pivot - 1 columna por indicador
    df_clean_hour_pivot = df_clean_hour.pivot(index='Datetime_hour', columns='indicator_id', values='value').reset_index()
    
    cols_round = [551, 546, 1295, 549, 1293, 10257]
    df_clean_hour_pivot[cols_round] = df_clean_hour_pivot[cols_round].round(1)

    # Renombrar columnas
    df_clean_hour_pivot.rename(columns=indicator_id_dict, inplace=True)
    print(df_clean_hour_pivot)

    return df_clean_hour_pivot



if __name__ == "__main__":
    
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 7, 1)
    indicator_id_dict = {
        551: 'Gen T.Real Eólica',
        546: 'Gen T.Real Hidraulica',
        1295: 'Gen T.Real Fotovoltaica',
        549: 'Gen T.Real Nuclear',
        1293: 'Demanda Real',
        10257: 'Gen P48 Total',
        612: 'IDA1',
        613: 'IDA2'
    }

    indicator_ids = list(indicator_id_dict.keys())
    df = get_df_indicadores_raw(start_date, end_date, indicator_ids)
    print(df)
    
    df_final = limpieza_df(df)
    df_final.to_csv('data_training/esios_dataset_d+7.csv', index=False)




