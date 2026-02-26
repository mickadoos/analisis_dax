import pandas as pd
import re
import numpy as np

def parsear_operaciones(historial_texto):
    """
    Parsea el historial de texto usando expresiones regulares para extraer la fecha, acción,
    dirección y precio. Convierte la hora a la zona horaria de Europa/Madrid para alinear con DAX.
    """
    patron = re.compile(r'\[(\d{2}:\d{2}), (\d{2}/\d{2}/\d{4})\] .*? (Abro|Cierro) (Largos|Cortos) en (\d+\.?\d*)')
    operaciones =[]
    
    for linea in historial_texto.strip().split('\n'):
        match = patron.search(linea)
        if match:
            hora, fecha, accion, direccion, precio = match.groups()
            dt_str = f"{fecha} {hora}"
            # Asumimos que la hora de WhatsApp/Telegram está en huso horario local (Madrid)
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
    Carga el CSV de velas, optimizado para grandes volúmenes.
    Sincroniza la zona horaria y detecta gaps mayores a 1 minuto.
    """
    # Usamos names ya que el formato de muestra no incluye headers
    df = pd.read_csv(ruta_csv, names=['Timestamp', 'Open', 'High', 'Low', 'Close'], header=0)
    
    # Parsear ISO8601 con zona horaria y unificar a Europe/Madrid
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True).dt.tz_convert('Europe/Madrid')
    df.set_index('Timestamp', inplace=True)
    df.sort_index(inplace=True)
    
    # ---------------------------------------------------------
    # Detección de Gaps (Saltos > 1 min)
    # ---------------------------------------------------------
    df['TimeDiff'] = df.index.to_series().diff()
    df['IsGap'] = df['TimeDiff'] > pd.Timedelta(minutes=1)
    df['PrevClose'] = df['Close'].shift(1)
    
    return df

def analizar_riesgo_y_barreras(df_velas, trades, distancia_barrera):
    """
    Evalúa cada trade vectorizando las búsquedas para máximo rendimiento.
    Calcula MAE y simula el Knock-out (Barreras).
    """
    resultados =[]
    
    for trade in trades:
        # Filtrar el dataframe exacto durante la operación
        trade_df = df_velas.loc[trade['entry_time'] : trade['exit_time']]
        
        if trade_df.empty:
            continue
            
        entry_price = trade['entry_price']
        direccion = trade['direccion']
        
        is_ko = False
        mae_absoluto = 0
        ahorro_gap = 0.0
        
        if direccion == 'Largos':
            barrera = entry_price - distancia_barrera
            # Buscar dónde el Low perfora la barrera (si lo hace)
            ko_mask = trade_df['Low'] <= barrera
            
            if ko_mask.any():
                is_ko = True
                ko_idx = ko_mask.idxmax() # Encuentra el PRIMER índice donde es True
                ko_row = trade_df.loc[ko_idx]
                
                # El MAE real de mercado antes de que te eche IG (en IBKR seguiría bajando)
                mae_absoluto = entry_price - trade_df['Low'].min()
                
                # Evaluar si el KO ocurrió por culpa de un Gap
                if ko_row['IsGap'] and ko_row['Open'] <= barrera and ko_row['PrevClose'] > barrera:
                    # En IBKR te ejecutarían al Open (peor precio), en IG te garantizan la barrera
                    slippage = barrera - ko_row['Open']
                    ahorro_gap = slippage
            else:
                mae_absoluto = max(0, entry_price - trade_df['Low'].min())
                
        elif direccion == 'Cortos':
            barrera = entry_price + distancia_barrera
            ko_mask = trade_df['High'] >= barrera
            
            if ko_mask.any():
                is_ko = True
                ko_idx = ko_mask.idxmax()
                ko_row = trade_df.loc[ko_idx]
                
                mae_absoluto = trade_df['High'].max() - entry_price
                
                if ko_row['IsGap'] and ko_row['Open'] >= barrera and ko_row['PrevClose'] < barrera:
                    slippage = ko_row['Open'] - barrera
                    ahorro_gap = slippage
            else:
                mae_absoluto = max(0, trade_df['High'].max() - entry_price)
                
        resultados.append({
            'direccion': direccion,
            'is_ko': is_ko,
            'mae': mae_absoluto,
            'ahorro_gap': ahorro_gap
        })
        
    return pd.DataFrame(resultados)

def main():
    # --- 1. ARCHIVOS REALES ---
    archivo_csv = "datos_dax_1min_2Y.csv"
    archivo_txt = "operaciones.txt"
    
    # --- 2. PARÁMETROS ---
    DISTANCIA_BARRERA = 50  # Aquí puedes cambiar la distancia a 30, 100, etc.
    
    # --- 3. LECTURA DE DATOS Y EJECUCIÓN ---
    print(f"Leyendo operaciones desde {archivo_txt}...")
    with open(archivo_txt, "r", encoding="utf-8") as f:
        historial_texto = f.read()
        
    trades = parsear_operaciones(historial_texto)
    print(f"Se encontraron {len(trades)} operaciones emparejadas válidas.")
    
    print(f"Cargando y procesando velas desde {archivo_csv}... (Esto puede tardar unos segundos)")
    df_velas = cargar_y_preparar_velas(archivo_csv) 
    
    print("Analizando riesgo y barreras...")
    df_resultados = analizar_riesgo_y_barreras(df_velas, trades, DISTANCIA_BARRERA)
    
    # --- 4. ESTADÍSTICAS FINALES ---
    print("\n" + "="*40)
    print("📈 ESTADÍSTICAS FINALES DEL SISTEMA 📉")
    print("="*40)
    
    if df_resultados.empty:
        print("No se pudieron calcular resultados.")
        return
        
    total_trades = len(df_resultados)
    max_mae = df_resultados['mae'].max()
    avg_mae = df_resultados['mae'].mean()
    total_kos = df_resultados['is_ko'].sum()
    ahorro_total_gaps = df_resultados['ahorro_gap'].sum()
    
    print(f"🔹 Total de operaciones analizadas: {total_trades}")
    print(f"🔹 Máxima pérdida latente histórica (MAE Absoluto): {max_mae:.2f} pts")
    print(f"🔹 MAE Promedio por operación: {avg_mae:.2f} pts")
    print(f"🔹 Eficiencia de Barrera: {total_kos} de {total_trades} operaciones ({total_kos/total_trades*100:.1f}%) habrían tocado Knock-Out.")
    
    if ahorro_total_gaps > 0:
        print(f"✅ Ahorro garantizado por IG vs Slippage IBKR (Gaps): +{ahorro_total_gaps:.2f} pts")
    else:
        print(f"ℹ️ Ningún Knock-Out sufrió de un Gap inasumible en esta muestra.")
        
if __name__ == "__main__":
    main()
