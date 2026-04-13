#FINALMENTE NO SE HAN USADO LOS TIPOS DE INTERÉS

import lseg.data as ld
import pandas as pd

ld.open_session()

RICS = {
    "Fed_FundsRate": "USFOMC=ECI",
    "ECB_MainRate":  "EUECBR=ECI",
    "BoE_BankRate":  "GBBOEI=ECI",
}

dfs = []
for name, ric in RICS.items():
    df = ld.get_history(
        universe=ric,
        interval="monthly",
        start="2003-12-01",
        end="2026-03-01",
    )
    df.columns = [name]
    dfs.append(df)

rates = pd.concat(dfs, axis=1)
rates.index.name = "Date"
rates = rates.ffill()

print(rates)
print(f"\nShape: {rates.shape}")

rates.to_csv(r"c:\Users\Ricardo\TFG_ENTREGAR\data\raw\tipos_interés.csv")
print("Guardado")

ld.close_session()