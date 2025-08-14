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

    query = f"""SELECT * FROM t_api_esios_indicadores_data 
                WHERE Datetime BETWEEN '{start_date_str}' AND '{end_date_str}'
                AND indicator_id IN ({query_indicadores})"""
    df = execute_query(query, 'esios')

    return df


if __name__ == "__main__":
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 6, 30)
    indicator_ids = [
        551, # Gen T.Real Eólica
        546, # Gen T.Real Hidraulica
        1295, # Gen T.Real Solar Fotovoltaica
        549, # Gen T.Real Nuclear
        1293, # Demanda Real
        10257, # Gen P48 Total
        612, 613 # IDA1 y IDA2
    ]

    df = get_df_indicadores_raw(start_date, end_date, indicator_ids)
    df.to_csv('data_training/esios_dataset_d+7.csv', index=False, date_format='%Y-%m-%d')

    
