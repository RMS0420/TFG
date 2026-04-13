import os
import shutil

# Configuración de rutas base
ruta_origen = r'C:\Users\Ricardo\Documents\UFV\Cuarto\TFG\TFG_ENTREGAR'
ruta_destino_base = os.path.join(ruta_origen, 'data', 'raw')

# Diccionario que agrupa tus listas por nombre de país
paises_bancos = {
    "EEUU": ["MTB.N","AXP","BK","FCNCA.O","JPM","BAC","GS","WFC","MS","LEHMQ.PK^C12","C.N","USB","PNC","COF.N","TFC","SIVBQ.PK^K24","KEY","RF","HBAN.O","FITB.O","CFG","ZION.O","FLG"],
    "JAPÓN": ["8306.T","8316.T","8411.T","7182.T","8308.T","8604.T","7186.T","8331.T","8354.T","5831.T"],
    "China": ["601398.SS","601288.SS","601939.SS","601988.SS","600036.SS","601658.SS","601328.SS","601166.SS","000001.SZ","601916.SS"],
    "India": ["HDBK.NS","ICBK.NS","SBI.NS","AXBK.NS","KTKM.NS","INBK.NS","YESB.NS"],
    "Korea": ["055550.KS","105560.KS","086790.KS","024110.KS","323410.KS"],
    "España": ["SAN.MC","BBVA.MC","CABK.MC","SABE.MC","BKT.MC","UNI.MC","POP.MC^F17","POP_r.MC^K12","LBK.MC^H21"],
    "Francia": ["BNPP.PA","SOGN.PA","CAGR.PA","DEXI.PA^C10"],
    "Holanda": ["INGA.AS", "ABNd.AS", "VLAN.AS"],
    "Alemania": ["DBKGn.DE","CBKG.DE","PBBG.DE","PCZ.DE","ARLn.DE^K23","DPBGn.DE^L15","HRXG.DEU^J09"],
    "Italia": ["ISP.MI","CRDI.MI","BMPS.MI","MDBI.MI","BAMI.MI","EMII.MI"],
    "Reino_Unido": ["HSBA.L","BARC.L","LLOY.L","NWG.L","STAN.L","CBRO.L","VMUK.L","MTRO.L"],
    "Grecia": ["BOPr.AT","NBGr.AT","ACBr.AT","EURBr.AT","CREDIAr.AT","BOCH.CY"],
    "Portugal": ["BCP.LS","BES.LS1^B16","BBPI.LS^L18"],
    "Suiza": ["UBSG.S","CSGN.S^F23","EFGN.S"],
    "Suecia": ["SHBA.ST","SWEDa.ST","SEBa.ST"],
    "Finlandia": ["NDAFI.HE"],
    "Dinamarca": ["DANSKE.CO","JYSK.CO"],
    "Noruega": ["DNB.OL"],
    "Sudafrica": ["SBKJ.J","FSRJ.J","ABGJ.J","NEDJ.J","CPIJ.J"],
    "Australia": ["CBA.AX","NAB.AX","WBC.AX","ANZ.AX","MQG.AX","BEN.AX","BOQ.AX","JDO.AX"]
}

def organizar_archivos():
    for pais, bancos in paises_bancos.items():
        # 1. Crear la carpeta del país si no existe
        ruta_pais = os.path.join(ruta_destino_base, pais)
        os.makedirs(ruta_pais, exist_ok=True)
        
        for banco in bancos:
            nombre_archivo = f"historico_noticias_{banco}.csv"
            path_fuente = os.path.join(ruta_origen, nombre_archivo)
            path_destino = os.path.join(ruta_pais, nombre_archivo)
            
            # 2. Verificar si el archivo existe antes de moverlo
            if os.path.exists(path_fuente):
                print(f"Moviendo: {nombre_archivo} -> {pais}")
                shutil.move(path_fuente, path_destino)
            else:
                print(f" No se encontró: {nombre_archivo} en la ruta original.")

if __name__ == "__main__":
    organizar_archivos()
    print("--- Proceso finalizado ---")