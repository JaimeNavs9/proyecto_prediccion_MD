import pandas as pd
import re
import datetime as dt
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import calendar
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.path import getDataPath, getLogsPath, getImgPath, getProjectDir
sys.path.append(getLogsPath())
script_name = os.path.splitext(os.path.basename(__file__))[0]
from utils.path import getProjectDir
from utils.connector import insertar_dataframe_en_mysql, execute_query



def cambiar_fecha(cadena, date):
    return cadena.replace('date=2024-06-30', f'date={date}')


def web_scraping_omip(url_FTB_f, url_FTS_f, fecha):
    Dia = []
    B = []
    R = []
    B1 = []
    R1 = []

    # Para el precio base
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Ejecutar en modo headless

    driver = webdriver.Chrome(chrome_options)
    driver.get(url_FTB_f)

    try:
        filas = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'tr')))
        for fila in filas:
            try:
                precio_celda = fila.find_element(By.XPATH, './td[15]').text
                B.append(precio_celda)
                Re = fila.find_element(By.XPATH, './td[1]').text
                R.append(Re)
                Dia.append(fecha)
            except Exception as e:
                # print(f"Error al obtener la celda o atributo rel: {e}")
                continue
    except Exception as e:
        print(f"Error al acceder a las filas: {e}")
    finally:
        driver.quit()

    driver = webdriver.Chrome(chrome_options)
    driver.get(url_FTS_f)

    try:
        filas = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'tr')))
        for fila in filas:
            try:
                precio_celda1 = fila.find_element(By.XPATH, './td[15]').text
                B1.append(precio_celda1)
                Re1 = fila.find_element(By.XPATH, './td[1]').text
                R1.append(Re1)
            except Exception as e:
                # print(f"Error al obtener la celda o atributo rel: {e}")
                continue
    except Exception as e:
        print(f"Error al acceder a las filas: {e}")
    finally:
        driver.quit()

    df0 = pd.DataFrame({'Dia_extraccion': Dia, 'Date': R, 'FTB': B, 'FTS': B1})
    return df0


def obtener_datos_omip(fecha_hoy):
    # table_name = os.path.join(getProjectDir('omip'), 'mercado_futuro')

    if fecha_hoy.weekday() == 5:
        fecha_hoy = fecha_hoy - dt.timedelta(days=1)
    elif fecha_hoy.weekday() == 6:
        fecha_hoy = fecha_hoy - dt.timedelta(days=2)

    fecha = fecha_hoy.strftime('%Y-%m-%d')

    url_FTB_f = f"https://www.omip.pt/es/dados-mercado?date={fecha}&product=EL&zone=ES&instrument=FTB"
    url_FTS_f = f"https://www.omip.pt/es/dados-mercado?date={fecha}&product=EL&zone=ES&instrument=FTS"


    df = web_scraping_omip(url_FTB_f, url_FTS_f, fecha)
    # Verificar si los datos son n.a.
    if df.iloc[1]['FTB'] == 'n.a.':
        print("No hay datos publicados todavía.")
        # obtener_datos_omip(fecha_hoy - dt.timedelta(days=1))
        return False
    else:
        df = df[df['Date'] != 'Contract name']
        print(df)

        insertar_dataframe_en_mysql(df, 't_ext_omip_ref_prices', 'omip')
        df.to_csv(os.path.join(getProjectDir('omip'), 'datos.csv'), index=False)
        return True


def convert_date_format(date_str):
    # Buscar fechas que comienzan por 'D'
    if 'D ' in date_str:
        pattern = r"(\d{2}[A-Za-z]{3}-\d{2})"
        match = re.search(pattern, date_str)
        if match:
            date_fragment = match.group(1)
            converted_date = dt.datetime.strptime(date_fragment, '%d%b-%y').strftime('%Y-%m-%d')
            return 'D ' + converted_date
    else:
        # Eliminar 'FTB' para el resto de las fechas
        clean_date = re.sub(r'^FTB\s*', '', date_str)
        # Si la fecha empieza por 'WE', devolver None para eliminarla del DataFrame
        if clean_date.startswith('WE'):
            return None
        else:
            return clean_date


def year_days_list(year):
    fecha_inicio = dt.datetime(year, 1, 1)
    fecha_fin = dt.datetime(year, 12, 31)

    days_list = []

    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
        days_list.append(fecha_actual.strftime('%Y-%m-%d'))
        fecha_actual += dt.timedelta(days=1)
    return days_list


def generate_calendar(year):
    weeks = []
    months = [[] for _ in range(12)]
    quarters = [[] for _ in range(4)]

    # Obtener el primer día del año
    first_day_of_year = dt.date(year, 1, 1)

    # Determinar cuántos días del año anterior deben incluirse en la primera semana
    days_from_previous_year = first_day_of_year.weekday()  # Lunes es 0, Domingo es 6

    # Crear la primera semana añadiendo los días necesarios del año anterior
    current_week = [
        (first_day_of_year - dt.timedelta(days=x)).strftime('%Y-%m-%d')
        for x in range(days_from_previous_year, 0, -1)
    ]

    date = first_day_of_year

    while date.year == year:
        formatted_date = date.strftime('%Y-%m-%d')
        months[date.month - 1].append(formatted_date)
        quarter_index = (date.month - 1) // 3
        quarters[quarter_index].append(formatted_date)

        current_week.append(formatted_date)

        if date.weekday() == 6:  # Domingo
            if weeks or current_week:  # Asegurarse de no duplicar la primera semana
                weeks.append(current_week)
            current_week = []

        date += dt.timedelta(days=1)

    # Eliminar la última semana si está incompleta
    if len(weeks[-1]) < 7:
        weeks.pop()

    return weeks, months, quarters


def date_expander(date_str, year, weeks, months, quarters):
    if date_str.startswith('D '):
        return [date_str.split()[1]], 'D'
    elif date_str.startswith('Wk'):
        if '-' in date_str and date_str.split('Wk')[1].split('-')[0].isdigit():
            week_num = int(date_str.split('Wk')[1].split('-')[0])
            return weeks[week_num - 1], 'Wk'
        else:
            return [], None
    elif date_str.startswith('M '):
        month_name = date_str.split()[1][:3]
        try:
            month_num = dt.datetime.strptime(month_name, "%b").month
        except ValueError:
            return [], None
        first_day_of_month = dt.date(year, month_num, 1)
        last_day_of_month = dt.date(year, month_num, calendar.monthrange(year, month_num)[1])
        return [str(first_day_of_month + dt.timedelta(days=i)) for i in range((last_day_of_month - first_day_of_month).days + 1)], 'M'
    elif date_str.startswith('Q') and len(date_str) > 1 and date_str[1].isdigit():
        quarter_num = int(date_str[1]) - 1
        if 0 <= quarter_num < 4:
            return quarters[quarter_num], 'Q'
        else:
            return [], None
    else:
        return year_days_list(year), 'Y'



def proceso_completo_extraccion(fecha_extraccion):
    """
        PROCESO COMPLETO: Extrae datos por web scraping, los procesa y los inserta en BBDD
    """
    res = obtener_datos_omip(fecha_extraccion)
    if not res:
        print(f"No se encontraron datos para la fecha: {fecha_extraccion}")
        return

    # Crear un diccionario para almacenar los DataFrames separados
    df = pd.read_csv(os.path.join(getProjectDir('omip'), 'datos.csv'))
    print(df)
    #Ajustamos para días que no hay valores
    ftb_0 = df["FTB"][0]

    if ftb_0 == 'n.a.':
        # Detener la ejecución del programa si es igual a 'n.a.'
        exit()

    # Primer año que aparece
    yr = int(df["Date"][0].split('-')[-1])

    # Extraemos el primer año
    corr = str(20) + str(yr)
    year_0 = int(corr)

    # Separamos el dataframe en los distintos años
    dataframes = {}

    # Bucle para crear DataFrames separados según el año en la columna 'Date'
    for year in range(year_0, year_0 + 10):
        df_year = df[df['Date'].str.endswith(f'-{year % 100:02d}')]
        if not df_year.empty:
            dataframes[f'df_{year}'] = df_year

    # Cogemos el primer df separado, correspondiendte al año df_year_0
    df_0 = dataframes.get('df_' + str(year_0))



    ############################################################
    df_final = pd.DataFrame()
    #Repetido para el año +1
    for year_actual in range(year_0, year_0+10):
        print('Año: ', year_actual)
        df_0 = dataframes.get('df_' + str(year_actual))
        if not df_0.empty:
            dataframes[f'df_{year_actual}'] = df_0
        df_0.loc[:, 'Date'] = df_0['Date'].apply(convert_date_format)
        df_0 = df_0.dropna(subset=['Date'])

        weeks, months, quarters = generate_calendar(year_actual)
        rows = []
        for _, row in df_0.iterrows():
            dates, interval_type = date_expander(row['Date'], year_actual, weeks, months, quarters)
            for date in dates:
                rows.append({
                    'Date': date,
                    'Dia_extraccion': row['Dia_extraccion'],
                    'Interval Type': interval_type,  # Nuevo campo para el tipo de intervalo
                    'FTB': row['FTB'],
                    'FTS': row['FTS']
                })

        expanded_df = pd.DataFrame(rows)

        print('Expanded df:')
        print(expanded_df)
        df_unique = expanded_df.drop_duplicates(subset='Date', keep='first')
        df_unique = df_unique.sort_values('Date')

        df_final = pd.concat([df_final, df_unique], ignore_index=True)


    columnas_a_redondear = ['FTB', 'FTS']
    df_final[columnas_a_redondear] = df_final[columnas_a_redondear].round(2)
    df_final = df_final.reset_index(drop=True)

    print('a la base de datos')
    print(df_final)

    #insertar_dataframe_en_mysql(df_final, 't_omip_forecast_hist', 'omip')



if __name__ == "__main__":
    fecha_inicio = dt.datetime.today() - dt.timedelta(days=5)
    fecha_fin = dt.datetime.today() - dt.timedelta(days=1)

    fecha_inicio = dt.datetime(2022, 1, 1)
    fecha_fin = dt.datetime(2023, 12, 31)

    for fecha_extraccion in pd.date_range(fecha_inicio, fecha_fin, freq="D"):
        #proceso_completo_extraccion(fecha_extraccion)
        obtener_datos_omip(fecha_extraccion)