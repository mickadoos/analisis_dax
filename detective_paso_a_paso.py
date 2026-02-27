import pandas as pd
import re
from datetime import timedelta

# --- CONFIGURACIÓN ---
CSV_VALORES = "datos_dax_1min_2Y.csv"
TXT_ORIGINAL = "operaciones_V2.txt"
UMBRAL_PUNTOS = 60  # Holgado como pediste, para no ser tiquismiquis

def cargar_datos():
    print("⏳ Cargando mercado de IBKR...")
    df = pd.read_csv(CSV_VALORES)
    df.columns = [c.capitalize() for c in df.columns]
    col = 'Date' if 'Date' in df.columns else 'Timestamp'
    df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert('Europe/Madrid')
    return df.set_index(col).sort_index()

df_mercado = cargar_datos()

with open(TXT_ORIGINAL, 'r', encoding='utf-8', errors='ignore') as f:
    contenido = f.read().replace('\u200e', '').replace('\u200f', '')

patron = re.compile(r'(\[(\d{1,2}:\d{2}).*?(\d{1,2}/\d{1,2}/\d{4}).*?(Abro|Cierro).*?(Largos|Cortos).*?(\d{5}))')
eventos_sueltos = []
for m in patron.finditer(contenido):
    ts = pd.to_datetime(f"{m.group(3)} {m.group(2)}", dayfirst=True).tz_localize('Europe/Madrid')
    eventos_sueltos.append({
        'ts': ts, 'tipo': m.group(4), 'dir': m.group(5), 'precio': int(m.group(6)), 'linea_completa': m.group(1)
    })

eventos = sorted(eventos_sueltos, key=lambda x: x['ts'])

print(f"🕵️ Detective v12 (Veloz & Estructural | Umbral: {UMBRAL_PUNTOS} pts)...\n")

ts_limite_inferior = eventos[0]['ts'] - timedelta(days=1)

for i in range(0, len(eventos) - 1, 2):
    e1, e2 = eventos[i], eventos[i+1]
    pnl_exp = (e2['precio'] - e1['precio']) if e1['dir'] == 'Largos' else (e1['precio'] - e2['precio'])
    ts_limite_superior = eventos[i+2]['ts'] if i+2 < len(eventos) else e2['ts'] + timedelta(days=1)

    try:
        # Búsqueda rápida (O(1)) para el OK
        idx1 = df_mercado.index.get_indexer([e1['ts']], method='nearest')[0]
        idx2 = df_mercado.index.get_indexer([e2['ts']], method='nearest')[0]
        pnl_m_orig = (df_mercado.iloc[idx2]['Close'] - df_mercado.iloc[idx1]['Close']) if e1['dir'] == 'Largos' else (df_mercado.iloc[idx1]['Close'] - df_mercado.iloc[idx2]['Close'])
        
        error = abs(pnl_exp - pnl_m_orig)
        
        if error > UMBRAL_PUNTOS:
            print(f"\n🚨 ERROR CRÍTICO EN TRADE #{(i//2)+1}")
            print(f"   • Desfase detectado: {error:.1f} pts")
            print(f"   • PnL Experto: {pnl_exp} | PnL Mercado Actual: {pnl_m_orig:.1f}")
            
            # Búsqueda de candidatos optimizada (solo si es crítico)
            candidatos = []
            ventana_ent = df_mercado.loc[e1['ts'] - timedelta(hours=4) : e1['ts'] + timedelta(hours=4)]
            ventana_sal = df_mercado.loc[e2['ts'] - timedelta(hours=4) : e2['ts'] + timedelta(hours=4)]
            
            for ts_sal, fila_sal in ventana_sal.iterrows():
                if ts_sal > ts_limite_superior: continue
                for ts_ent, fila_ent in ventana_ent.iterrows():
                    if ts_ent < ts_limite_inferior or ts_ent >= ts_sal: continue
                    
                    pnl_f = (fila_sal['Close'] - fila_ent['Close']) if e1['dir'] == 'Largos' else (fila_ent['Close'] - fila_sal['Close'])
                    if abs(pnl_f - pnl_exp) < 5:
                        b_e, b_s = e1['precio'] - fila_ent['Close'], e2['precio'] - fila_sal['Close']
                        score = abs(b_e - b_s)
                        candidatos.append((score, ts_ent, ts_sal, fila_ent['Close'], fila_sal['Close']))
            
            if candidatos:
                candidatos.sort(key=lambda x: x[0])
                _, t_e, t_s, pf_e, pf_s = candidatos[0]
                
                print(f"\n📑 JUSTIFICACIÓN TÉCNICA:")
                print(f"   Para validar los {pnl_exp} pts con base estable,")
                print(f"   Futuro: {pf_e:.0f} (Entrada) y {pf_s:.0f} (Salida).")
                
                print(f"\n📝 INSTRUCCIONES PARA {TXT_ORIGINAL}:")
                n_e = f"[{t_e.strftime('%H:%M, %d/%m/%Y')}] Operativa DAX: Operativa Dax - Abro {e1['dir']} en {e1['precio']}"
                n_s = f"[{t_s.strftime('%H:%M, %d/%m/%Y')}] Operativa DAX: Operativa Dax - Cierro {e2['dir']} en {e2['precio']}"
                print(f"1. BUSCAR:  {e1['linea_completa']}")
                print(f"   CAMBIAR: {n_e}")
                print(f"\n2. BUSCAR:  {e2['linea_completa']}")
                print(f"   CAMBIAR: {n_s}")
                print(f"--------------------------------------------------")
                ts_limite_inferior = t_s
                input("ENTER para continuar...")
            else:
                print("❌ No se encontró solución en la ventana de tiempo.")
                ts_limite_inferior = e2['ts']
        else:
            print(f"✅ Trade #{(i//2)+1} {e1['ts'].strftime('%d/%m %H:%M')}: OK (Desfase: {error:.1f} pts)")
            ts_limite_inferior = e2['ts']
            
    except Exception as ex:
        print(f"Error: {ex}")
        break