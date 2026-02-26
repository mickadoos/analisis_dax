import pandas as pd
import re
import numpy as np

def parsear_operaciones(historial_texto):
    """
    Parsea el historial de texto usando expresiones regulares para extraer la fecha, acción,
    dirección y precio.
    """
    patron = re.compile(r'\[(\d{2}:\d{2}), (\d{2}/\d{2}/\d{4})\] .*? (Abro|Cierro) (Largos|Cortos) en (\d+\.?\d*)')
    operaciones =[]
    
    for linea in historial_texto.strip().split('\n'):
        match = patron.search(linea)
        if match:
            hora, fecha, accion, direccion, precio = match.groups()
            dt_str = f"{fecha} {hora}"
            dt = pd.to_datetime(dt_str, format="%d/%m/%Y %H:%M").tz_localize('Europe/Madrid')
            operaciones.append({
                'timestamp': dt,
                'accion': accion,
                'direccion': direccion,
                'precio': float(precio)
            })
            
    # Emparejar operaciones (Abro -> Cierro)
    operaciones_emparejadas =[]
    i = 0
    while i < len(operaciones) - 1:
        if operaciones[i]['accion'] == 'Abro' and operaciones[i+1]['accion'] == 'Cierro':
            operaciones_emparejadas.append({
                'entry_time': operaciones[i]['timestamp'],
                'exit_time': operaciones[i+1]['timestamp'],
                'direccion': operaciones[i]['direccion'],
                'entry_price': operaciones[i]['precio'],
                'exit_price': operaciones[i+1]['precio']
            })
            i += 2
        else:
            i += 1
            
    return operaciones_emparejadas

def cargar_y_preparar_velas(ruta_csv):
    """
    Carga el CSV de velas y detecta Gaps tanto de tiempo como de precio.
    """
    # IMPORTANTE: header=0 porque tu CSV tiene títulos en la primera fila
    df = pd.read_csv(ruta_csv, names=['Timestamp', 'Open', 'High', 'Low', 'Close'], header=0)
    
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True).dt.tz_convert('Europe/Madrid')
    df.set_index('Timestamp', inplace=True)
    df.sort_index(inplace=True)
    
    # --- DETECCIÓN DE GAPS (TIEMPO Y PRECIO) ---
    df['TimeDiff'] = df.index.to_series().diff()
    df['IsGap'] = df['TimeDiff'] > pd.Timedelta(minutes=1)
    df['PrevClose'] = df['Close'].shift(1)
    
    # Calcular el salto matemático en puntos del Gap
    df['PriceGap'] = df['Open'] - df['PrevClose']
    df['AbsPriceGap'] = df['PriceGap'].abs()
    
    return df

def exportar_radar_gaps(df_velas, umbral_puntos=30):
    """
    FASE 1: Escanea todo el histórico buscando Gaps mayores a 'umbral_puntos' y los exporta a CSV.
    """
    # Filtrar velas que son Gap Y además su salto en puntos es mayor al umbral
    df_gaps = df_velas[(df_velas['IsGap'] == True) & (df_velas['AbsPriceGap'] >= umbral_puntos)].copy()
    
    # Seleccionar columnas limpias para el analista
    df_export = df_gaps[['PrevClose', 'Open', 'PriceGap', 'TimeDiff']].copy()
    df_export.rename(columns={
        'PrevClose': 'Cierre_Anterior', 
        'Open': 'Apertura_Actual', 
        'PriceGap': 'Tamano_Gap_Puntos',
        'TimeDiff': 'Tiempo_Transcurrido_Cerrado'
    }, inplace=True)
    
    # Guardar en CSV
    nombre_archivo = "historial_gaps_mercado.csv"
    df_export.to_csv(nombre_archivo)
    print(f"✅ FASE 1: Radar de Gaps exportado a '{nombre_archivo}' con {len(df_export)} saltos críticos (> {umbral_puntos} pts).")

def analizar_riesgo_y_barreras(df_velas, trades, distancia_barrera):
    """
    FASE 3: Análisis de Resiliencia del Experto vs Simulación IG.
    """
    resultados =[]
    
    for trade in trades:
        trade_df = df_velas.loc[trade['entry_time'] : trade['exit_time']]
        if trade_df.empty:
            continue
            
        entry_price = trade['entry_price']
        exit_price = trade['exit_price']
        direccion = trade['direccion']
        
        is_ko = False
        ahorro_gap = 0.0
        
        # 1. LÓGICA DE SIMULACIÓN IG (K.O.) Y MAE
        if direccion == 'Largos':
            pnl_final_experto = exit_price - entry_price
            peor_precio_real = trade_df['Low'].min()
            mae_absoluto = max(0, entry_price - peor_precio_real)
            
            barrera = entry_price - distancia_barrera
            ko_mask = trade_df['Low'] <= barrera
            
            if ko_mask.any():
                is_ko = True
                ko_idx = ko_mask.idxmax() 
                ko_row = trade_df.loc[ko_idx]
                # Ahorro de Gap sólo si el mercado saltó limpiamente la barrera
                if ko_row['IsGap'] and ko_row['Open'] <= barrera and ko_row['PrevClose'] > barrera:
                    ahorro_gap = barrera - ko_row['Open']
                    
        elif direccion == 'Cortos':
            pnl_final_experto = entry_price - exit_price
            peor_precio_real = trade_df['High'].max()
            mae_absoluto = max(0, peor_precio_real - entry_price)
            
            barrera = entry_price + distancia_barrera
            ko_mask = trade_df['High'] >= barrera
            
            if ko_mask.any():
                is_ko = True
                ko_idx = ko_mask.idxmax()
                ko_row = trade_df.loc[ko_idx]
                if ko_row['IsGap'] and ko_row['Open'] >= barrera and ko_row['PrevClose'] < barrera:
                    ahorro_gap = ko_row['Open'] - barrera

        # 2. LÓGICA DE RESILIENCIA (FASE 3)
        peor_momento_pts = -mae_absoluto  # Lo pasamos a negativo para que sea visual
        puntos_recuperados = pnl_final_experto - peor_momento_pts
        
        resultados.append({
            'direccion': direccion,
            'is_ko_IG': is_ko,
            'mae_sufrimiento_max': mae_absoluto,
            'peor_momento_pts': peor_momento_pts,
            'resultado_final_experto': pnl_final_experto,
            'puntos_recuperados_experto': puntos_recuperados,
            'ahorro_gap_IG': ahorro_gap
        })
        
    return pd.DataFrame(resultados)

def main():
    # --- 1. ARCHIVOS REALES ---
    archivo_csv = "datos_dax_1min_2Y.csv"
    archivo_txt = "operaciones.txt"
    
    # --- 2. PARÁMETROS CONFIGURABLES ---
    DISTANCIA_BARRERA = 500  # Puntos para el Knock-Out de IG
    UMBRAL_GAP_RADAR = 30    # Considerar "Gap peligroso" si salta más de 30 pts de golpe
    
    # --- 3. LECTURA DE DATOS ---
    print(f"Leyendo operaciones desde {archivo_txt}...")
    with open(archivo_txt, "r", encoding="utf-8") as f:
        historial_texto = f.read()
        
    trades = parsear_operaciones(historial_texto)
    print(f"Se encontraron {len(trades)} operaciones emparejadas válidas.\n")
    
    print(f"Cargando velas (2 años) desde {archivo_csv}... (Esto puede tardar)")
    df_velas = cargar_y_preparar_velas(archivo_csv) 
    print("Velas cargadas correctamente.\n")
    
    # --- 4. FASE 1: RADAR DE GAPS ---
    exportar_radar_gaps(df_velas, UMBRAL_GAP_RADAR)
    
    # --- 5. FASE 3: ANÁLISIS DE RESILIENCIA ---
    print("\nAnalizando riesgo, resiliencia y barreras...")
    df_resultados = analizar_riesgo_y_barreras(df_velas, trades, DISTANCIA_BARRERA)
    
    # Exportar el resultado detallado para que lo audites en Excel
    df_resultados.to_csv("auditoria_operaciones.csv", index=False)
    print("✅ FASE 3: Archivo 'auditoria_operaciones.csv' generado para revisión manual.\n")
    
    # --- 6. ESTADÍSTICAS FINALES ---
    print("="*50)
    print("📈 ESTADÍSTICAS FINALES DEL SISTEMA 📉")
    print("="*50)
    
    if df_resultados.empty:
        print("No se pudieron calcular resultados.")
        return
        
    total_trades = len(df_resultados)
    max_mae = df_resultados['mae_sufrimiento_max'].max()
    avg_mae = df_resultados['mae_sufrimiento_max'].mean()
    total_kos = df_resultados['is_ko_IG'].sum()
    ahorro_total_gaps = df_resultados['ahorro_gap_IG'].sum()
    
    # Nuevas estadísticas de resiliencia
    pnl_promedio = df_resultados['resultado_final_experto'].mean()
    recuperacion_promedio = df_resultados['puntos_recuperados_experto'].mean()
    
    print(f"🔹 Total de operaciones analizadas: {total_trades}")
    print(f"🔹 Máximo flotante negativo soportado: -{max_mae:.2f} pts")
    print(f"🔹 Flotante negativo promedio: -{avg_mae:.2f} pts")
    print(f"🔹 PnL Promedio Final por trade: {pnl_promedio:+.2f} pts")
    print(f"🔹 Capacidad de recuperación promedio: +{recuperacion_promedio:.2f} pts desde el peor momento.")
    print(f"🔹 Simulador IG (Barrera a {DISTANCIA_BARRERA}): Te habrían echado en {total_kos} operaciones ({(total_kos/total_trades)*100:.1f}%).")
    
    if ahorro_total_gaps > 0:
        print(f"✅ Ahorro garantizado por IG vs Slippage IBKR (Gaps nocturnos/bruscos): +{ahorro_total_gaps:.2f} pts")
    else:
        print(f"ℹ️ Ningún Knock-Out sufrió de un Gap inasumible en esta muestra.")
        
if __name__ == "__main__":
    main()