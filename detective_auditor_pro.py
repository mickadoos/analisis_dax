import pandas as pd
import re
from colorama import Fore, Style, init

init(autoreset=True)

# --- CONFIGURACIÓN ---
CSV_VALORES = "dax_1min.csv"
TXT_V3 = "operaciones_V3.txt"
UMBRAL_VERDE = 40

def obtener_color(pts):
    if pts < UMBRAL_VERDE: return Fore.GREEN
    if pts < 70: return Fore.YELLOW
    return Fore.RED

# Carga de mercado
df_m = pd.read_csv(CSV_VALORES)
df_m.columns = [c.capitalize() for c in df_m.columns]
col = 'Date' if 'Date' in df_m.columns else 'Timestamp'
df_m[col] = pd.to_datetime(df_m[col], utc=True).dt.tz_convert('Europe/Madrid')
df_m = df_m.set_index(col).sort_index()

# Procesamiento V3
with open(TXT_V3, 'r') as f:
    contenido = f.read()

# Buscamos pares Abro/Cierro
patron = re.compile(r'\[(\d{2}:\d{2}), (\d{2}/\d{2}/\d{4})\] (Abro|Cierro): (Largos|Cortos) en (\d+)')
eventos = []
for m in patron.finditer(contenido):
    ts = pd.to_datetime(f"{m.group(2)} {m.group(1)}", dayfirst=True).tz_localize('Europe/Madrid')
    eventos.append({'ts': ts, 'tipo': m.group(3), 'dir': m.group(4), 'precio': int(m.group(5))})

print(f"\n📊 REPORTE DE AUDITORÍA V16 (MATEMÁTICA REAL)\n" + "="*115)
print(f"{'TR':<3} {'FECHA':<9} {'DIR':<6} {'PRECIOS (E/S)':<15} {'PNL EXP':<10} {'PNL MKT':<10} {'ERR PTS':<10} {'Δ BASE':<10} {'STATUS'}")
print("-" * 115)

acum_exp, acum_mkt = 0, 0

for i in range(0, len(eventos) - 1, 2):
    e1, e2 = eventos[i], eventos[i+1]
    
    # --- CÁLCULO PNL EXPERTO (Matemático, no leído) ---
    if e1['dir'] == 'Largos':
        pnl_exp = e2['precio'] - e1['precio']
    else: # Cortos
        pnl_exp = e1['precio'] - e2['precio']
    
    # --- CÁLCULO PNL MERCADO ---
    try:
        idx1 = df_m.index.get_indexer([e1['ts']], method='nearest')[0]
        idx2 = df_m.index.get_indexer([e2['ts']], method='nearest')[0]
        
        # Validar si hay datos (margen 2h)
        if abs((df_m.index[idx1] - e1['ts']).total_seconds()) > 7200:
            raise ValueError()

        p_f_e, p_f_s = df_m.iloc[idx1]['Close'], df_m.iloc[idx2]['Close']
        
        if e1['dir'] == 'Largos':
            pnl_mkt = p_f_s - p_f_e
        else:
            pnl_mkt = p_f_e - p_f_s
            
        err_pts = abs(pnl_exp - pnl_mkt)
        delta_base = (e2['precio'] - p_f_s) - (e1['precio'] - p_f_e)
        
        color = obtener_color(err_pts)
        status = "OK" if err_pts < UMBRAL_VERDE else "REVISAR"
        
        print(f"#{i//2+1:<2} {e1['ts'].strftime('%d/%m'):<9} {e1['dir'][:1]:<6} {e1['precio']}/{e2['precio']:<8} "
              f"{pnl_exp:<10.0f} {pnl_mkt:<10.1f} {color}{err_pts:<10.1f}{Style.RESET_ALL} {delta_base:<10.1f} {status}")
        
        acum_exp += pnl_exp
        acum_mkt += pnl_mkt

    except:
        print(f"#{i//2+1:<2} {e1['ts'].strftime('%d/%m'):<9} {e1['dir'][:1]:<6} {e1['precio']}/{e2['precio']:<8} "
              f"{pnl_exp:<10.0f} {'--':<10} {'--':<10} {'--':<10} ⚠️ NO DATA")
        acum_exp += pnl_exp

print("="*115)
print(f"RESULTADO NETO EXPERTO: {acum_exp} pts")
print(f"RESULTADO NETO MERCADO: {acum_mkt:.1f} pts")
print(f"DESVIACIÓN TOTAL:       {abs(acum_exp - acum_mkt):.1f} pts")