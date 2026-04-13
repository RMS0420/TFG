import lseg.data as ld
import pandas as pd
import os
import random  # <--- NUEVA LIBRERÍA AÑADIDA
from datetime import datetime, timedelta

# ABRIR SESIÓN
ld.open_session()

# SELECCIÓN DE PAISES
# AMERICA
EEUU = ["MTB.N","AXP","BK","FCNCA.O","JPM","BAC","GS","WFC","MS","LEHMQ.PK^C12","C.N","USB",
        "PNC","COF.N","TFC","SIVBQ.PK^K24","KEY","RF","HBAN.O","FITB.O","CFG","ZION.O","FLG"]

# ASIA
JAPÓN = ["8306.T","8316.T","8411.T","7182.T","8308.T","8604.T","7186.T","8331.T","8354.T","5831.T"]
China = ["601398.SS","601288.SS","601939.SS","601988.SS","600036.SS","601658.SS","601328.SS","601166.SS","000001.SZ","601916.SS"]
India = ["HDBK.NS","ICBK.NS","SBI.NS","AXBK.NS","KTKM.NS","INBK.NS","YESB.NS"]
Korea = ["055550.KS","105560.KS","086790.KS","024110.KS","323410.KS"]

# EUROPA
España = ["SAN.MC","BBVA.MC","CABK.MC","SABE.MC","BKT.MC","UNI.MC","POP.MC^F17","POP_r.MC^K12","LBK.MC^H21"]
Francia = ["BNPP.PA","SOGN.PA","CAGR.PA","DEXI.PA^C10"]
Holanda = ["INGA.AS", "ABNd.AS", "VLAN.AS"]
Alemania = ["DBKGn.DE","CBKG.DE","PBBG.DE","PCZ.DE","ARLn.DE^K23","DPBGn.DE^L15","HRXG.DEU^J09"]
Italia = ["ISP.MI","CRDI.MI","BMPS.MI","MDBI.MI","BAMI.MI","EMII.MI",]
Reino_Unido = ["HSBA.L","BARC.L","LLOY.L","NWG.L","STAN.L","CBRO.L","VMUK.L","MTRO.L"]
Grecia = ["BOPr.AT","NBGr.AT","ACBr.AT","EURBr.AT","CREDIAr.AT","BOCH.CY"]
Portugal = ["BCP.LS","BES.LS1^B16","BBPI.LS^L18"]
Suiza = ["UBSG.S","CSGN.S^F23","EFGN.S"]
Suecia = ["SHBA.ST","SWEDa.ST","SEBa.ST"]
Finlandia = ["NDAFI.HE"]
Dinamarca = ["DANSKE.CO","JYSK.CO"]
Noruega = ["DNB.OL"]
Nordicos = [Suecia,Finlandia,Dinamarca,Noruega]

# AFRICA
Sudafrica = ["SBKJ.J","FSRJ.J","ABGJ.J","NEDJ.J","CPIJ.J"]

# OCEANIA
Australia = ["CBA.AX","NAB.AX","WBC.AX","ANZ.AX","MQG.AX","BEN.AX","BOQ.AX","JDO.AX"]

# Unificar todos los tickers en una lista plana
paises = [EEUU, JAPÓN, China, India, Korea, España, Francia, Holanda, Alemania,
          Italia, Reino_Unido, Grecia, Portugal, Suiza, Suecia, Finlandia,
          Dinamarca, Noruega, Sudafrica, Australia]

#todos_los_bancos = Korea
todos_los_bancos = [banco for sublista in paises for banco in sublista]

# FUNCIÓN AUXILIAR PARA GUARDAR EN DISCO
def volcar_a_disco(buffer, archivo):
    if not buffer:
        return
    df_concat = pd.concat(buffer)
    # Si no existe, crea con cabecera. Si existe, añade debajo sin cabecera.
    if not os.path.exists(archivo):
        df_concat.to_csv(archivo)
    else:
        df_concat.to_csv(archivo, mode='a', header=False) # Añadido header=False por seguridad
    print(f"--> Guardados {len(df_concat)} registros en disco ({archivo}). RAM liberada.")

# BUCLE PRINCIPAL DE EXTRACCIÓN
fecha_tope_historico = "2004-01-01T00:00:00"

for ticker in todos_los_bancos:
    print(f"\n=====================================")
    print(f"Iniciando extracción para: {ticker}")
    print(f"=====================================")
    
    archivo_csv = f"historico_noticias_{ticker.replace('^', '_')}.csv"
    fecha_fin = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    fecha_fin = "2026-03-06T00:00:00"
    
    # LÓGICA DE CONTINUACIÓN (RESUME)
    if os.path.exists(archivo_csv):
        try:
            # Leemos solo la columna de fechas para no cargar todo en memoria
            df_existente = pd.read_csv(archivo_csv, usecols=['versionCreated']) 
            if not df_existente.empty:
                # Buscamos la fecha más antigua extraída hasta ahora para seguir desde ahí hacia atrás
                fecha_fin = df_existente['versionCreated'].min()
                print(f"Archivo detectado. Reanudando desde la fecha más antigua guardada: {fecha_fin}")
        except Exception as e:
            print(f"Error al leer archivo previo: {e}. Se empezará desde hoy.")

    buffer_dfs = []
    iteraciones_ram = 0
    numero_llamada = 0

    while fecha_fin > fecha_tope_historico:
        try:
            # Hacemos la llamada (el parámetro 'query' admite la búsqueda del ticker)
            df_news = ld.news.get_headlines(
                query=ticker, 
                start=fecha_tope_historico, 
                end=fecha_fin, 
                count=2000
            )

            print(df_news)

            numero_llamada = numero_llamada + 1

            print(numero_llamada)

            if df_news is None or df_news.empty:
                print(f"Fin del histórico para {ticker}. No hay más noticias antes de {fecha_fin}.")
                break

            buffer_dfs.append(df_news)
            iteraciones_ram += 1
            
            # --- NUEVA LÓGICA DE COMPROBACIÓN DEL DÍA ---
            
            # Comprobamos si las 100 noticias son del mismo día usando el índice normalizado
            # .normalize() pone las horas a 00:00:00, así que si .nunique() es 1, todas pertenecen al mismo día.
            es_mismo_dia = df_news.index.normalize().nunique() == 1
            
            if es_mismo_dia and len(df_news) == 100:
                # Obtenemos ese día en concreto y le restamos 1 día entero
                dia_actual = df_news.index.min()
                dia_anterior = dia_actual - timedelta(days=1)
                
                # Generamos horas, minutos y segundos aleatorios
                hora_rnd = random.randint(0, 23)
                minuto_rnd = random.randint(0, 59)
                segundo_rnd = random.randint(0, 59)
                
                # Construimos la nueva fecha aplicando el tiempo aleatorio
                nueva_fecha_dt = dia_anterior.replace(hour=hora_rnd, minute=minuto_rnd, second=segundo_rnd)
                fecha_fin = nueva_fecha_dt.strftime("%Y-%m-%dT%H:%M:%S")
                
                print(f"[*] 100 noticias en el mismo día ({dia_actual.strftime('%Y-%m-%d')}). "
                      f"Avanzando al día anterior: {fecha_fin}")
                
            else:
                # --- LÓGICA ORIGINAL (Si NO son del mismo día) ---
                nueva_fecha_fin = df_news.index.min()
                nueva_fecha_fin_str = nueva_fecha_fin.strftime("%Y-%m-%dT%H:%M:%S")
                
                # MEDIDA DE SEGURIDAD: Evitar bucles infinitos si las 100 ocurren en el mismo segundo exacto
                if nueva_fecha_fin_str == fecha_fin:
                    dt_obj = datetime.strptime(fecha_fin, "%Y-%m-%dT%H:%M:%S")
                    fecha_fin = (dt_obj - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
                else:
                    fecha_fin = nueva_fecha_fin_str
                    
            # -----------------------------------------------

            # PROTECCIÓN DE RAM: Volcar a disco cada 20 llamadas
            if iteraciones_ram % 20 == 0:
                volcar_a_disco(buffer_dfs, archivo_csv)
                buffer_dfs = [] # Vaciamos la lista para liberar memoria

        except Exception as e:
            print(f"Error en la API con el ticker {ticker}: {e}")
            break # Rompemos el while para pasar al siguiente banco o investigar el error
            
    # Al final del banco, volcamos lo que haya quedado suelto en el buffer
    if buffer_dfs:
        volcar_a_disco(buffer_dfs, archivo_csv)

print("¡Extracción finalizada para todos los bancos!")