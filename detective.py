import pandas as pd
from datetime import timedelta

# 1. CARGA DE DATOS
print("🔍 Iniciando Detective por Aproximación...")
df = pd.read_csv("datos_dax_1min_2Y.csv")
df.columns = [c.capitalize() for c in df.columns]
col_fecha = 'Date' if 'Date' in df.columns else 'Timestamp'

# Convertir y manejar zona horaria correctamente
df[col_fecha] = pd.to_datetime(df[col_fecha], utc=True, errors='coerce')
df = df.dropna(subset=[col_fecha])
# El truco es usar .dt sobre la serie, no sobre el dataframe
df[col_fecha] = df[col_fecha].dt.tz_convert('Europe/Madrid')
df.set_index(col_fecha, inplace=True)
df.sort_index(inplace=True)

# 2. DATOS DEL CULPABLE (SEGÚN EL EXPERTO)
P_ENT_IDX = 20747
P_SAL_IDX = 19620
# Anclamos la búsqueda al cierre del lunes
T_SAL_IDX = pd.to_datetime("2025-04-07 15:36").tz_localize('Europe/Madrid')
PNL_ESPERADO = P_SAL_IDX - P_ENT_IDX  # -1127 puntos (Largos)

# 3. ANALIZAR EL MOMENTO DEL CIERRE EN EL FUTURO
try:
    # Buscamos la vela más cercana al cierre por si no es exacta al segundo
    idx_cierre = df.index.get_indexer([T_SAL_IDX], method='nearest')[0]
    vela_cierre = df.iloc[idx_cierre]
    p_sal_fut = vela_cierre['Close']
    base_cierre = P_SAL_IDX - p_sal_fut
    
    print(f"\n📍 Punto de Anclaje (Cierre):")
    print(f"   Hora efectiva en CSV: {df.index[idx_cierre]}")
    print(f"   Índice: {P_SAL_IDX} | Futuro: {p_sal_fut:.2f} | Base detectada: {base_cierre:.2f} pts")
except Exception as e:
    print(f"❌ Error al anclar el cierre: {e}")
    exit()

# 4. BUSCAR LA ENTRADA POR CORRELACIÓN
# ¿Qué precio debería tener el futuro si la base fuera la misma que al cierre?
p_ent_fut_teorico = P_ENT_IDX - base_cierre
print(f"\n🎯 Buscando huella de entrada cerca de {p_ent_fut_teorico:.2f} en el Futuro...")

# Buscamos en la ventana de 5 días antes del cierre
inicio_busqueda = T_SAL_IDX - timedelta(days=5)
bloque = df.loc[inicio_busqueda : T_SAL_IDX].copy()

if bloque.empty:
    print("❌ No hay datos en el rango de tiempo especificado.")
    exit()

# Calculamos la distancia de cada vela al precio teórico
bloque['Distancia'] = bloque.apply(lambda x: min(abs(x['Low'] - p_ent_fut_teorico), abs(x['High'] - p_ent_fut_teorico)), axis=1)
mejor_vela = bloque.sort_values('Distancia').head(1)

# 5. RESULTADOS Y JUSTIFICACIÓN
t_ent_real = mejor_vela.index[0]
p_ent_real = mejor_vela['Close'].values[0]
distancia_final = mejor_vela['Distancia'].values[0]

# Comparar PnL para ver si la "huella" es la misma
pnl_futuro = p_sal_fut - p_ent_real
error_ajuste = abs(PNL_ESPERADO - pnl_futuro)

print(f"\n✅ POSIBLE CULPABLE LOCALIZADO:")
print(f"   Fecha/Hora real probable: {t_ent_real}")
print(f"   Precio en Futuro: {p_ent_real:.2f}")
print(f"   Desviación de la base (Índice vs Futuro): {distancia_final:.2f} pts")

print(f"\n📊 COMPARATIVA DE MOVIMIENTO (PNL):")
print(f"   PnL Experto: {PNL_ESPERADO} pts")
print(f"   PnL Futuro:  {pnl_futuro:.2f} pts")
print(f"   Margen de error del ajuste: {error_ajuste:.2f} pts")

# 6. CÁLCULO DEL MAE REAL (EL MOMENTO DE LA VERDAD)
ventana_op = df.loc[t_ent_real : T_SAL_IDX]
peor_precio = ventana_op['Low'].min()
# El MAE real es la distancia desde nuestra entrada hasta el punto más bajo
mae_calculado = p_ent_real - peor_precio

print(f"\n🛡️ AUDITORÍA DE SUPERVIVENCIA:")
print(f"   MAE Real (sufrimiento máximo): {mae_calculado:.2f} pts")
if mae_calculado >= 500:
    print(f"   💥 RESULTADO: BARRERA TOCADA. Esta operación no sobrevive a 500 pts.")
else:
    print(f"   ✅ RESULTADO: SUPERVIVENCIA. La cuenta habría aguantado el bache.")