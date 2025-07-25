api_key = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqbmF2YXJyb0Bzb2xhcmlhZW5lcmdpYS5jb20iLCJqdGkiOiI3NmRhOTgxMi0zOGNlLTRkOWQtODA5Zi1mZjdkYzU0MWM0ZTUiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc1MzQyOTgzMywidXNlcklkIjoiNzZkYTk4MTItMzhjZS00ZDlkLTgwOWYtZmY3ZGM1NDFjNGU1Iiwicm9sZSI6IiJ9.nzV3hE6mRXi9srZC8R6mpqWDrnMAWmJ6kX1uHtz2M64"

import pandas as pd
import requests
import json
import numpy as np

pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)


municipio = "07052413"
url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{municipio}"
querystring = {"api_key":api_key}
headers = {'cache-control': "no-cache"}
response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)

municipios = pd.read_excel('diccionario24.xlsx', header=1)
print(municipios)
