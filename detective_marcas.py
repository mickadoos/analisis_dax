import pandas as pd
from datetime import timedelta

# 1. CARGA (Simplificada para el test)
df = pd.read_csv("datos_dax_1min_2Y.csv")
df.columns = [c.capitalize() for c in df.columns]
col_fecha = 'Date' if 'Date' in df.columns else 'Timestamp'
df[col_fecha] = pd.to_datetime(df[col_fecha], utc=True, errors='coerce')
df = df.dropna(subset=[col_fecha])
df[col_fecha] = df[col_fecha].dt.tz_convert('Europe/Madrid')
df.set_index(col_fecha, inplace=True)

# DATOS DEL CASO
P_ENT_IDX = 20747
P_SAL_IDX = 19620
T_SAL_IDX = pd.to_datetime("2025-04-07 15:36").tz_localize('Europe/Madrid')

# EJECUCIÓN DEL DETECTIVE
idx_cierre = df.index.get_indexer([T_SAL_IDX], method='nearest')[0]
p_sal_fut = df.iloc[idx_cierre]['Close']
base_cierre = P_SAL_IDX - p_sal_fut
p_ent_fut_teorico = P_ENT_IDX - base_cierre

inicio_busqueda = T_SAL_IDX - timedelta(days=5)
bloque = df.loc[inicio_busqueda : T_SAL_IDX].copy()
bloque['Distancia'] = bloque.apply(lambda x: min(abs(x['Low']-p_ent_fut_teorico), abs(x['High']-p_ent_fut_teorico)), axis=1)
t_ent_real = bloque.sort_values('Distancia').index[0]
p_ent_real = df.loc[t_ent_real, 'Close']

# PUNTO DE MÁXIMO DOLOR (MAE)
ventana_total = df.loc[t_ent_real : T_SAL_IDX]
t_mae = ventana_total['Low'].idxmin()
p_mae = ventana_total['Low'].min()

print(f"\n--- 📋 HOJA DE RUTA PARA TU CONTRASTE ---")
print(f"Busca estas 3 marcas de tiempo en tu CSV 'datos_dax_1min_2Y.csv':")
print(f"\n1. PUNTO DE ENTRADA (El origen real):")
print(f"   Fecha: {t_ent_real} | Precio Close en CSV: {p_ent_real}")
print(f"\n2. PUNTO DE CIERRE (El ancla):")
print(f"   Fecha: {df.index[idx_cierre]} | Precio Close en CSV: {p_sal_fut}")
print(f"\n3. PUNTO DE MÁXIMO drawdown (El abismo):")
print(f"   Fecha: {t_mae} | Precio Low en CSV: {p_mae}")
print(f"\n----------------------------------------")