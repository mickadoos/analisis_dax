import pandas as pd
import re
import numpy as np

def parsear_operaciones(historial_texto):
    """
    Parsea el historial para extraer la fecha, acción, dirección y precio.
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
    Carga el CSV de velas y detecta los saltos (Gaps/Agujeros de datos).
    """
    df = pd.read_csv(ruta_csv, names=['Timestamp', 'Open', 'High', 'Low', 'Close'], header=0)
    
    df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_convert('Europe/Madrid')
    df.set_index('Timestamp', inplace=True)
    df.sort_index(inplace=True)
    
    df['TimeDiff'] = df.index.to_series().diff()
    df['IsGap'] = df['TimeDiff'] > pd.Timedelta(minutes=1)
    df['PrevClose'] = df['Close'].shift(1)
    
    df['PriceGap'] = df['Open'] - df['PrevClose']
    df['AbsPriceGap'] = df['PriceGap'].abs()
    
    return df

def exportar_radar_gaps(df_gaps):
    """
    Exporta el radar global de gaps al CSV.
    """
    df_export = df_gaps[['PrevClose', 'Open', 'PriceGap', 'TimeDiff']].copy()
    df_export.rename(columns={
        'PrevClose': 'Cierre_Anterior', 
        'Open': 'Apertura_Actual', 
        'PriceGap': 'Tamano_Gap_Puntos',
        'TimeDiff': 'Tiempo_Transcurrido_Cerrado'
    }, inplace=True)
    
    nombre_archivo = "historial_gaps_mercado.csv"
    df_export.to_csv(nombre_archivo)

def analizar_riesgo_y_barreras(df_velas, df_gaps, trades, distancia_barrera):
    """
    Analiza el riesgo, resiliencia y cruza las operaciones con los gaps (Fase 2 y 3).
    """
    resultados =[]
    
    for trade in trades:
        trade_df = df_velas.loc[trade['entry_time'] : trade['exit_time']]
        
        # FASE 2: Detección de Gaps sufridos DURANTE esta operación
        gaps_durante_trade = df_gaps.loc[trade['entry_time'] : trade['exit_time']]
        sufrio_gap = not gaps_durante_trade.empty
        peor_salto_gap = gaps_durante_trade['AbsPriceGap'].max() if sufrio_gap else 0.0
        
        if trade_df.empty:
            print(f"⚠️ ADVERTENCIA: No se encontraron velas para el trade {direccion} en {trade['entry_time']}. Omitiendo...")
            continue
            
        entry_price = trade['entry_price']
        exit_price = trade['exit_price']
        direccion = trade['direccion']
        
        is_ko = False
        ahorro_gap = 0.0

        # 1. INICIALIZAMOS VARIABLES DE SEGURIDAD
        pnl_final = 0.0
        mae_absoluto = 0.0
        peor_precio_real = entry_price
        
        # LÓGICA DE SIMULACIÓN Y MAE
        if direccion == 'Largos':
            pnl_final = exit_price - entry_price
            peor_precio_real = trade_df['Low'].min()
            mae_absoluto = max(0, entry_price - peor_precio_real)
            barrera = entry_price - distancia_barrera
            ko_mask = trade_df['Low'] <= barrera
            
            if ko_mask.any():
                is_ko = True
                ko_idx = ko_mask.idxmax() 
                # CORRECCIÓN: Forzamos a que, si hay duplicados, solo coja la primera vela (iloc[0])
                # Si no hay duplicados, funciona exactamente igual.
                velas_ko = trade_df.loc[[ko_idx]]
                ko_row = velas_ko.iloc[0]
                if ko_row['IsGap'] and ko_row['Open'] <= barrera and ko_row['PrevClose'] > barrera:
                    ahorro_gap = barrera - ko_row['Open']
                    
        elif direccion == 'Cortos':
            pnl_final = entry_price - exit_price
            peor_precio_real = trade_df['High'].max()
            mae_absoluto = max(0, peor_precio_real - entry_price)
            barrera = entry_price + distancia_barrera
            ko_mask = trade_df['High'] >= barrera
            
            if ko_mask.any():
                is_ko = True
                ko_idx = ko_mask.idxmax()
                # CORRECCIÓN: Forzamos a que, si hay duplicados, solo coja la primera vela (iloc[0])
                # Si no hay duplicados, funciona exactamente igual.
                velas_ko = trade_df.loc[[ko_idx]]
                ko_row = velas_ko.iloc[0]
                if ko_row['IsGap'] and ko_row['Open'] >= barrera and ko_row['PrevClose'] < barrera:
                    ahorro_gap = ko_row['Open'] - barrera

        peor_momento_pts = -mae_absoluto  
        puntos_recuperados = pnl_final - peor_momento_pts
        
        # Añadimos TimeStamps y datos de Gaps
        resultados.append({
            'Fecha_Entrada': trade['entry_time'].strftime('%d/%m/%Y %H:%M'),
            'Fecha_Salida': trade['exit_time'].strftime('%d/%m/%Y %H:%M'),
            'direccion': direccion,
            'is_ko_IG': is_ko,
            'mae_sufrimiento_max': mae_absoluto,
            'peor_momento_pts': peor_momento_pts,
            'resultado_final_experto': pnl_final,
            'puntos_recuperados_experto': puntos_recuperados,
            'sufrio_gap_peligroso': sufrio_gap,
            'peor_salto_gap_sufrido': peor_salto_gap,
            'ahorro_gap_IG': ahorro_gap
        })
        
    return pd.DataFrame(resultados)

def main():
    archivo_csv = "datos_dax_1min_2Y.csv"
    archivo_txt = "operaciones.txt"
    
    DISTANCIA_BARRERA = 500  
    UMBRAL_GAP_RADAR = 300    
    
    print(f"Leyendo operaciones desde {archivo_txt}...")
    with open(archivo_txt, "r", encoding="utf-8") as f:
        historial_texto = f.read()
        
    trades = parsear_operaciones(historial_texto)
    print(f"Se encontraron {len(trades)} operaciones emparejadas válidas.\n")
    
    print(f"Cargando velas desde {archivo_csv}... (Esto puede tardar)")
    df_velas = cargar_y_preparar_velas(archivo_csv) 
    
    # Pre-calculamos los gaps peligrosos para usarlos en ambas fases
    df_gaps_peligrosos = df_velas[(df_velas['IsGap'] == True) & (df_velas['AbsPriceGap'] >= UMBRAL_GAP_RADAR)].copy()
    
    exportar_radar_gaps(df_gaps_peligrosos)
    
    print("\nAnalizando riesgo, resiliencia y cruce de Gaps...")
    df_resultados = analizar_riesgo_y_barreras(df_velas, df_gaps_peligrosos, trades, DISTANCIA_BARRERA)
    
    df_resultados.to_csv("auditoria_operaciones.csv", index=False)
    print("✅ Archivo 'auditoria_operaciones.csv' generado para revisión manual.\n")
    
    print("="*50)
    print("📈 ESTADÍSTICAS FINALES DEL SISTEMA 📉")
    print("="*50)
    
    if df_resultados.empty:
        print("No se pudieron calcular resultados.")
        return
        
    total_trades = len(df_resultados)
    max_mae = df_resultados['mae_sufrimiento_max'].max()
    pnl_promedio = df_resultados['resultado_final_experto'].mean()
    recuperacion_promedio = df_resultados['puntos_recuperados_experto'].mean()
    trades_con_gap = df_resultados['sufrio_gap_peligroso'].sum()
    
    print(f"🔹 Total de operaciones analizadas: {total_trades}")
    print(f"🔹 Operaciones que sufrieron Gaps/Agujeros de datos en su contra: {trades_con_gap} operaciones")
    print(f"🔹 PnL Promedio Final por trade: {pnl_promedio:+.2f} pts")
    print(f"🔹 Resiliencia: El experto recupera de media +{recuperacion_promedio:.2f} pts desde el peor momento.")
    print(f"🔹 Knock-Out (Barrera a {DISTANCIA_BARRERA}): Te habrían echado en {df_resultados['is_ko_IG'].sum()} ocasiones.")
    print(f"⚠️ NOTA: El historial de Gaps incluye 'Agujeros de Datos' del proveedor del CSV (saltos de varios días).")
        
if __name__ == "__main__":
    main()