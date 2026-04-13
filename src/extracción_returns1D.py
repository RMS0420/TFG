import lseg.data as ld
import pandas as pd

#ABRIR SESIÓN
ld.open_session()

#SELECCIÓN DE PAISES

#AMERICA

EEUU = ["MTB","AXP","BK","FCNCA.O","JPM","BAC","GS","WFC","MS","LEHMQ.PK^C12","C","USB",
        "PNC","COF","TFC","SIVBQ.PK^K24","KEY","RF","HBAN.O","FITB.O","CFG","ZION.O","FLG"]

#ASIA

JAPÓN = ["8306.T","8316.T","8411.T","7182.T","8308.T","8604.T","7186.T","8331.T","8354.T","5831.T"]
China = ["601398.SS","601288.SS","601939.SS","601988.SS","600036.SS","601658.SS","601328.SS","601166.SS","000001.SZ","601916.SS"]
India = ["HDBK.NS","ICBK.NS","SBI.NS","AXBK.NS","KTKM.NS","INBK.NS","YESB.NS"]
Korea = ["055550.KS","105560.KS","086790.KS","024110.KS","323410.KS"]

#EUROPA

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

#AFRICA

Sudafrica = ["SBKJ.J","FSRJ.J","ABGJ.J","NEDJ.J","CPIJ.J"]

#OCEANIA

Australia = ["CBA.AX","NAB.AX","WBC.AX","ANZ.AX","MQG.AX","BEN.AX","BOQ.AX","JDO.AX"]

# Unificar todos los tickers en una lista plana
paises = [EEUU, JAPÓN, China, India, Korea, España, Francia, Holanda, Alemania,
          Italia, Reino_Unido, Grecia, Portugal, Suiza, Suecia, Finlandia,
          Dinamarca, Noruega, Sudafrica, Australia]

all_tickers = ['BAC', 'BARC.L', 'BBVA.MC', 'BNPP.PA', 'C.N', 'CAGR.PA', 'CBKG.DE', 'CRDI.MI',
 'DBKGn.DE', 'GS', 'HSBA.L', 'JPM', 'LLOY.L', 'MS', 'SAN.MC', 'SOGN.PA', 'STAN.L']

# 2. BUCLE DE DESCARGA INDIVIDUAL
lista_dfs = []  # Lista para ir acumulando los DataFrames individuales

for ticker in all_tickers:
    print(f"Descargando: {ticker}...")
    try:
        # Llamada a la API por cada ticker individual
        df_temp = ld.get_history(
            universe=[ticker], # El universo es un solo ticker
            fields=["TR.BIDPRICE","TR.ASKPRICE","TR.PriceClose","TR.VOLUME"],
            start="2004-01-01",
            end="2026-02-26",
            adjustments="manualCorrection",
            interval="daily"
        )
        
        # Si la API devolvió datos, lo añadimos a nuestra lista
        if df_temp is not None and not df_temp.empty:
            # Aseguramos que el ticker sea una columna para no perder la referencia
            df_temp = df_temp.reset_index()
            if 'Instrument' not in df_temp.columns:
                df_temp['Ticker'] = ticker 
            
            lista_dfs.append(df_temp)
            
    except Exception as e:
        print(f"Error con {ticker}: {e}")

# 3. CONCATENAR TODO AL FINAL
if lista_dfs:
    df_final = pd.concat(lista_dfs, ignore_index=True)
    
    # Ordenar y guardar
    df_final = df_final.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)
    df_final.to_csv("precios_volumen_raw.csv", index=False)
    
    print("\nProceso finalizado.")
    print(f"Total de filas descargadas: {len(df_final)}")
    print(df_final)
else:
    print("No se descargó ningún dato.")

# 4. CERRAR SESIÓN
ld.close_session()