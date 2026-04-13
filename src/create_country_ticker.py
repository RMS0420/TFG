import os
import pandas as pd

# 1. Definimos la ruta base
ruta_base = r'D:\TFG_ENTREGAR\TFG_ENTREGAR\data\raw\news_raw_por_pais'

# 2. Lista para los datos
datos_mapeo = []

# 3. Recorrido de carpetas
for pais in os.listdir(ruta_base):
    ruta_pais = os.path.join(ruta_base, pais)
    
    if os.path.isdir(ruta_pais):
        for archivo in os.listdir(ruta_pais):
            if archivo.endswith('.csv'):
                # Limpiamos el nombre:
                # Quitamos 'historico_noticias_' y luego '.csv'
                ticker = archivo.replace('historico_noticias_', '').replace('.csv', '')
                
                datos_mapeo.append({'ticker': ticker, 'pais': pais})

# 4. Crear DataFrame
df_mapeo = pd.DataFrame(datos_mapeo)

# 5. Guardar
ruta_guardado = r'D:\TFG_ENTREGAR\TFG_ENTREGAR\data\mapeo_tickers_paises.csv'
df_mapeo.to_csv(ruta_guardado, index=False)

print(f"Mapeo finalizado. Se han encontrado {len(df_mapeo)} bancos.")
print(f"Archivo guardado en: {ruta_guardado}")

# Mostramos el resultado para verificar que el ticker esté limpio
print("\nVista previa del mapeo:")
print(df_mapeo.head())