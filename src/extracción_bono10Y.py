# import lseg.data as ld

# ld.open_session()

# print(ld.discovery.search(
#     view=ld.discovery.Views.QUOTES,
#     query="US Treasury 1 year yield",
#     select="DocumentTitle,RIC",
#     top=5
# ))

# print(ld.discovery.search(
#     view=ld.discovery.Views.QUOTES,
#     query="UK Gilt 1 year yield",
#     select="DocumentTitle,RIC",
#     top=5
# ))

# print(ld.discovery.search(
#     view=ld.discovery.Views.QUOTES,
#     query="Germany Bund 1 year yield",
#     select="DocumentTitle,RIC",
#     top=5
# ))

# print(ld.discovery.search(
#     view=ld.discovery.Views.QUOTES,
#     query="Italian Bund 1 year yield",
#     select="DocumentTitle,RIC",
#     top=5
# ))

# ld.close_session()

import lseg.data as ld
import pandas as pd

ld.open_session()

RICS = {
    "US_2Y":  "US2YT=RR",
    "US_10Y": "US10YT=RR",
    "GB_2Y":  "GB2YT=RR",
    "GB_10Y": "GB10YT=RR",
    "DE_2Y":  "DE2YT=RR",
    "DE_10Y": "DE10YT=RR",
    "IT_2Y":  "IT2YT=RR",
    "IT_10Y": "IT10YT=RR",
}
dfs = []
for name, ric in RICS.items():
    try:
        df = ld.get_history(
            universe=ric,
            fields=["MID_YLD_1"],
            interval="daily",
            start="2004-01-01",
            end="2026-03-01",
        )
        df.columns = [name]
        dfs.append(df)
        print(f"✅ {name} ({ric}): {len(df)} filas")
    except Exception as e:
        print(f"❌ {name} ({ric}): {e}")
yields = pd.concat(dfs, axis=1)
yields.index.name = "Date"
yields = yields.ffill()

print(yields.tail())
yields.to_csv(r"rendimientos_bonos_estatales.csv")
print("Guardado")

ld.close_session()