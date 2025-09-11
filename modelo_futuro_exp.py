import pandas as pd
import numpy as np
import datetime as dt
import pytz
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV

import scipy.stats as stats
from sklearn.preprocessing import scale


from tensorflow import keras
from tensorflow.keras import layers # type: ignore
from tensorflow.keras import regularizers # type: ignore


###############################################################################
# Lectura de datos
###############################################################################

df_data = pd.read_csv('data_training/predictores_modelo_futuro.csv')
df_data['Datetime_hour'] = pd.to_datetime(df_data['Datetime_hour'])

df_data['Year'] = df_data['Datetime_hour'].dt.year
df_data['Month'] = df_data['Datetime_hour'].dt.month
df_data['Day_of_Week'] = df_data['Datetime_hour'].dt.dayofweek
df_data['Hour'] = df_data['Datetime_hour'].dt.hour

df_input = df_data.drop(columns=['Datetime_hour'])



###############################################################################
# PARAMETROS
###############################################################################



n_neurons_1 = 64
n_neurons_2 = 64
dropout = 0.25

lr = 0.0005
patience = 12
batch_size = 64
n_capas = 2



nombre_modelo = 'm'


def proceso_completo_prediccion(df_input, n_capas, n_neurons_1, n_neurons_2, dropout, lr, patience, batch_size, nombre_modelo, plot=True):
    global num_test_days, num_val_days, cols_drop

    ###############################################################################
    # Preparación de datos
    ###############################################################################

    df_input = df_input.drop(columns=[x for x in cols_drop if x in df_input.columns])
    df_input.dropna(inplace=True)

    # Division train-val-test
    X_train = df_input.drop('MD', axis=1).iloc[:len(df_input)-num_test_days*24-num_val_days*24]
    X_val = df_input.drop('MD', axis=1).iloc[-num_test_days*24-num_val_days*24:-num_test_days*24]
    X_test = df_input.drop('MD', axis=1).iloc[-num_test_days*24:]

    y_train = df_input['MD'].iloc[:len(df_input)-num_test_days*24-num_val_days*24]
    y_val = df_input['MD'].iloc[-num_test_days*24-num_val_days*24:-num_test_days*24]
    y_test = df_input['MD'].iloc[-num_test_days*24:]

    # Reescalado
    scaler_X = StandardScaler()
    scaler_y = StandardScaler()

    X_train_scaled = scaler_X.fit_transform(X_train)
    X_val_scaled = scaler_X.transform(X_val)
    X_test_scaled = scaler_X.transform(X_test)

    # Para la variable objetivo
    y_train_scaled = scaler_y.fit_transform(y_train.values.reshape(-1, 1)).flatten()
    y_val_scaled = scaler_y.transform(y_val.values.reshape(-1, 1)).flatten()
    y_test_scaled = scaler_y.transform(y_test.values.reshape(-1, 1)).flatten()



    ###############################################################################
    # MODELO
    ###############################################################################

    n_features = X_train.shape[1]

    # Entrada
    inputs = keras.Input(shape=(n_features,))
    # Primera capa interna
    x = layers.Dense(n_neurons_1, activation='relu', kernel_regularizer=regularizers.l2(1e-4))(inputs)
    x = layers.BatchNormalization()(x)
    x = layers.Dropout(dropout)(x)
    # Resto de capas
    for i in range(n_capas-1):
        x = layers.Dense(n_neurons_2, activation='relu', kernel_regularizer=regularizers.l2(1e-4))(x)
        x = layers.BatchNormalization()(x)
        x = layers.Dropout(dropout)(x)

    # Salida
    outputs = layers.Dense(1)(x)
    # Construccion del modelo
    model = keras.Model(inputs=inputs, outputs=outputs)

    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=lr),
        loss='mse',
        metrics=['mae']  # MAE como métrica principal
    )

    callbacks = [
        keras.callbacks.EarlyStopping(
            monitor='val_mae',  # Monitorear MAE de validation
            patience=patience,
            restore_best_weights=True,
            verbose=0
        ),
        keras.callbacks.EarlyStopping(
            monitor='val_mae',
            patience=patience,
            restore_best_weights=True,  # <- restaurará los pesos mejores al final
            verbose=0
        )
    ]
    history = model.fit(
        X_train_scaled, y_train_scaled,
        validation_data=(X_val_scaled, y_val_scaled),  # Usar validation set
        epochs=150,
        batch_size=batch_size,
        callbacks=callbacks,
        verbose=0
    )
    # print(history.history)



    ###############################################################################
    # RESULTADOS
    ###############################################################################

    # loss = history.history["mae"]
    # val_loss = history.history["val_mae"]
    # epochs = range(1, len(loss) + 1)
    # plt.figure()
    # plt.plot(epochs, loss, "bo", label="Training MAE")
    # plt.plot(epochs, val_loss, "b", label="Validation MAE")
    # plt.title("Training and validation MAE")
    # plt.legend()
    # plt.show()

    # Evaluar el modelo con el test set (datos nunca vistos)
    print("\n" + "="*50)
    print("EVALUACIÓN FINAL CON TEST SET")
    print("="*50)

    # Hacer predicciones en el test set
    y_pred_scaled = model.predict(X_test_scaled)
    y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()
    y_real = scaler_y.inverse_transform(y_test_scaled.reshape(-1, 1)).flatten()


    # MAE
    mae_test = mean_absolute_error(y_real, y_pred)
    print(f"MAE en Test Set: {mae_test:.2f} €/MWh")
    print(f"Error absoluto medio: ±{mae_test:.2f} €/MWh")


    if plot:
        fechas_test = df_data['Datetime_hour'].iloc[-num_test_days*24:]

        # Crear un DataFrame con los datos para Plotly
        df_plot = pd.DataFrame({
            'Fecha': fechas_test,
            'Valor Real': y_real,
            'Predicción': y_pred
        })

        # Crear el gráfico interactivo con Plotly Express
        fig = px.line(df_plot, x='Fecha', y=['Valor Real', 'Predicción'],
                    title='Predicciones vs Valores Reales (Test Set)',
                    labels={'value': 'Precio (Escala Original)', 'variable': 'Leyenda'},
                    color_discrete_map={'Valor Real': 'blue', 'Predicción': 'orange'})

        # Personalizar el gráfico
        fig.update_layout(
            title_font_size=16,
            xaxis_title='Fecha',
            xaxis_title_font_size=14,
            yaxis_title='Precio (Escala Original)',
            yaxis_title_font_size=14,
            legend_title='',
            width=1000,
            height=500,
            template='plotly_white'
        )

        # Ajustar la opacidad de la línea de predicción
        fig.update_traces(opacity=0.7, selector={'name': 'Predicción'})

        # Mostrar la cuadrícula
        fig.update_xaxes(showgrid=True)
        fig.update_yaxes(showgrid=True)

        # Mostrar el gráfico
        fig.show()

    return mae_test, history





n_neurons_list = [32, 64, 128]
dropout_list = [0.1, 0.25, 0.4, 0.55]
lr_list = [0.0005, 0.001]
patience_list = [8, 12, 16]
batch_size_list = [32, 64]
n_capas_list = [2, 3]

num_test_days = 30
num_val_days = 30

cols_drop = []

entradas = []
for n_capas in n_capas_list:
    for n_neurons in n_neurons_list:
        for dropout in dropout_list:
            for lr in lr_list:
                for patience in patience_list:
                    for batch_size in batch_size_list:
                        mae_test, history = proceso_completo_prediccion(df_input, n_capas, n_neurons, n_neurons, dropout, lr, patience, batch_size, 'm', plot=False)

                        entrada = {'n_capas': n_capas, 'n_neurons': n_neurons, 'dropout': dropout, 'lr': lr, 'patience': patience, 'batch_size': batch_size, 
                                'RESULTADO': mae_test, 'n_epochs': len(history.history['val_mae'])}
                        entradas.append(entrada)


df_entradas = pd.DataFrame(entradas)
print(df_entradas)
df_entradas.to_csv('resultados_exp.csv')