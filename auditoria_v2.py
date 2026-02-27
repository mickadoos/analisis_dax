import pandas as pd
import re
import sys

# ==========================================
# CONFIGURACIÓN DEL USUARIO
# ==========================================
DISTANCIA_BARRERA = 500  # Variable X: Distancia de la Barrera de IG en puntos
ARCHIVO_VELAS = "datos_dax_1min_2Y.csv"
ARCHIVO_OPERACIONES = "operaciones.txt"
ARCHIVO_SALIDA = "resultado_auditoria_google_sheets.csv"

def cargar_datos_maestros(ruta_csv):
    """Carga las velas de IBKR y las sincroniza con Madrid de forma robusta."""
    try:
        print(f"1. Cargando velas desde {ruta_csv}...")
        # Leemos el CSV
        df = pd.read_csv(ruta_csv)
        
        # Estandarizar nombres de columnas a Mayúsculas
        df.columns = [c.capitalize() for c in df.columns]
        col_fecha = 'Date' if 'Date' in df.columns else ('Timestamp' if 'Timestamp' in df.columns else None)
        
        if col_fecha:
            df = df.rename(columns={col_fecha: 'Timestamp'})
        else:
            print("❌ No se encontró columna de fecha (Date o Timestamp)")
            sys.exit()

        # --- CORRECCIÓN CRÍTICA AQUÍ ---
        # Forzamos la conversión a datetime tratando de manejar los errores
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
        
        # Eliminamos filas donde la fecha sea inválida (si las hubiera)
        df = df.dropna(subset=['Timestamp'])
        
        # Convertimos a la zona horaria de Madrid
        df['Timestamp'] = df['Timestamp'].dt.tz_convert('Europe/Madrid')
            
        df.set_index('Timestamp', inplace=True)
        df.sort_index(inplace=True)
        print(f"✅ {len(df)} velas de 1min cargadas correctamente.")
        return df
    except Exception as e:
        print(f"❌ Error crítico en el cargador: {e}")
        sys.exit()

def obtener_trades_de_texto():
    """Lee el archivo de texto y empareja aperturas y cierres."""
    print(f"2. Leyendo historial desde {ARCHIVO_OPERACIONES}...")
    try:
        with open(ARCHIVO_OPERACIONES, 'r', encoding='utf-8') as f:
            texto = f.read()
    except:
        print("❌ No se encontró el archivo operaciones.txt")
        return []

    patron = re.compile(r'\[(\d{2}:\d{2}), (\d{2}/\d{2}/\d{4})\] .*? (Abro|Cierro) (Largos|Cortos)')
    eventos = []
    
    for linea in texto.strip().split('\n'):
        match = patron.search(linea)
        if match:
            hora, fecha, accion, direccion = match.groups()
            dt = pd.to_datetime(f"{fecha} {hora}", format="%d/%m/%Y %H:%M").tz_localize('Europe/Madrid')
            eventos.append({'ts': dt, 'accion': accion, 'dir': direccion})

    trades = []
    i = 0
    while i < len(eventos) - 1:
        if eventos[i]['accion'] == 'Abro' and eventos[i+1]['accion'] == 'Cierro':
            trades.append({'entrada': eventos[i]['ts'], 'salida': eventos[i+1]['ts'], 'dir': eventos[i]['dir']})
            i += 2
        else:
            i += 1
    print(f"✅ {len(trades)} operaciones emparejadas encontradas.")
    return trades

def ejecutar_auditoria(df_velas, trades, d_ko):
    """Motor principal con captura del momento exacto del MAE."""
    print(f"3. Ejecutando auditoría con Barrera a {d_ko} puntos...")
    resultados = []
    
    for t in trades:
        # Extraer ventana de tiempo del trade
        seg = df_velas.loc[t['entrada'] : t['salida']]
        if seg.empty: continue
            
        p_entrada = seg.iloc[0]['Open']
        p_salida = seg.iloc[-1]['Close']
        
        if t['dir'] == 'Largos':
            pnl_exp = p_salida - p_entrada
            # Buscamos el valor mínimo y la fecha en que ocurrió
            peor_p = seg['Low'].min()
            hora_mae = seg['Low'].idxmin() # Captura el Timestamp del mínimo
            mae = p_entrada - peor_p
            toco_ko = peor_p <= (p_entrada - d_ko)
        else: # Cortos
            pnl_exp = p_entrada - p_salida
            # Buscamos el valor máximo y la fecha en que ocurrió
            peor_p = seg['High'].max()
            hora_mae = seg['High'].idxmax() # Captura el Timestamp del máximo
            mae = peor_p - p_entrada
            toco_ko = peor_p >= (p_entrada + d_ko)

        res_cuenta = -d_ko if toco_ko else pnl_exp
        
        resultados.append({
            'Entrada': t['entrada'].strftime('%Y-%m-%d %H:%M'),
            'Salida': t['salida'].strftime('%Y-%m-%d %H:%M'),
            'Direccion': t['dir'],
            'Precio_Entrada': round(p_entrada, 2),
            'PnL_Experto': round(pnl_exp, 2),
            'MAE_Absoluto': round(mae, 2),
            'Hora_MAE': hora_mae.strftime('%Y-%m-%d %H:%M'), # NUEVA COLUMNA
            'KO_en_IG': "SÍ" if toco_ko else "NO",
            'Resultado_Tu_Cuenta': round(res_cuenta, 2),
            'Puntos_Recuperados': round(mae + pnl_exp, 2)
        })
        
    return pd.DataFrame(resultados)

# --- EJECUCIÓN ---
velas = cargar_datos_maestros(ARCHIVO_VELAS)
trades = obtener_trades_de_texto()
df_final = ejecutar_auditoria(velas, trades, DISTANCIA_BARRERA)

# Exportación para Google Sheets
df_final.to_csv(ARCHIVO_SALIDA, index=False, encoding='utf-8-sig')

# --- INFORME FINAL DE CONSOLA ---
print("\n" + "="*60)
print("📊 INFORME FINAL DE AUDITORÍA Y SUPERVIVENCIA")
print("="*60)
if not df_final.empty:
    total = len(df_final)
    kos = (df_final['KO_en_IG'] == "SÍ").sum()
    supervivencia = ((total - kos) / total) * 100
    
    print(f"🔹 TOTAL OPERACIONES:     {total}")
    print(f"🔹 DISTANCIA BARRERA:     {DISTANCIA_BARRERA} pts")
    print(f"🔹 OPERACIONES CON KO:    {kos}")
    print(f"🔹 TASA SUPERVIVENCIA:    {supervivencia:.2f} %")
    print("-" * 30)
    print(f"🔹 MAE MÁXIMO HISTÓRICO:  {df_final['MAE_Absoluto'].max()} pts")
    print(f"🔹 PNL TOTAL EXPERTO:     {df_final['PnL_Experto'].sum():+.2f} pts")
    print(f"🔹 PNL TOTAL TU CUENTA:   {df_final['Resultado_Tu_Cuenta'].sum():+.2f} pts")
    print("="*60)
    print(f"✅ Archivo generado: {ARCHIVO_SALIDA}")
else:
    print("⚠️ No se pudieron procesar operaciones.")