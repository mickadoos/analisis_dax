import pandas as pd
import re
from datetime import datetime, time, timedelta
from colorama import init, Fore, Style

init(autoreset=True)

FILE_OPERACIONES = 'operaciones_V3.txt'
FILE_MERCADO = 'dax_1min.csv'
HORA_INICIO_OP = time(9, 0, 0)
HORA_FIN_OP = time(17, 36, 0)

def cargar_mercado():
    print(f"{Fore.CYAN}Cargando mercado... ", end="", flush=True)
    df = pd.read_csv(FILE_MERCADO, low_memory=False)
    df['dt'] = pd.to_datetime(df[df.columns[0]].astype(str).str.split('+').str[0].str.strip())
    df['close'] = pd.to_numeric(df[df.columns[4]], errors='coerce')
    return df.dropna(subset=['dt', 'close']).set_index('dt').sort_index()

def obtener_todos_los_trades():
    with open(FILE_OPERACIONES, 'r', encoding='utf-8') as f:
        contenido = f.read()
    pattern = r"\[(\d{2}:\d{2}),\s*(\d{2}/\d{2}/\d{4})\]\s*(Abro|Cierro):\s*(\w+)\s*en\s*(\d+)"
    matches = re.findall(pattern, contenido)
    trades = []
    temp_open = None
    for m in matches:
        dt = datetime.strptime(f"{m[1]} {m[0]}", "%d/%m/%Y %H:%M")
        if m[2] == "Abro":
            temp_open = {'dt': dt, 'p': int(m[4]), 's': m[3]}
        elif m[2] == "Cierro" and temp_open:
            trades.append({'open_dt': temp_open['dt'], 'open_p': temp_open['p'], 
                           'close_dt': dt, 'close_p': int(m[4]), 's': temp_open['s']})
            temp_open = None
    return trades

def generar_horquilla():
    df_mkt = cargar_mercado()
    trades = obtener_todos_los_trades()
    
    while True:
        print(f"\n{Style.BRIGHT}{'='*70}")
        entrada = input(f"{Fore.YELLOW}Línea del Detective o número de Trade: ")
        if entrada.lower() in ['exit', 'q']: break
        
        match_num = re.search(r"#?(\d+)", entrada)
        if not match_num: continue
        idx = int(match_num.group(1)) - 1
        
        # 1. IDENTIFICAR MUROS (Anterior y Posterior)
        t_act = trades[idx]
        t_ant = trades[idx-1] if idx > 0 else None
        t_sig = trades[idx+1] if idx < len(trades)-1 else None
        
        muro_inf = t_ant['close_dt'] if t_ant else t_act['open_dt'] - timedelta(days=1)
        muro_sup = t_sig['open_dt'] if t_sig else t_act['close_dt'] + timedelta(days=1)
        
        pnl_objetivo = (t_act['close_p'] - t_act['open_p']) if 'largo' in t_act['s'].lower() else (t_act['open_p'] - t_act['close_p'])

        print(f"\n{Fore.CYAN}--- INFORME DE HORRIQUILLA PARA TRADE #{idx+1} ---")
        print(f"Límite Anterior: {muro_inf} | Límite Posterior: {muro_sup}")
        print(f"Movimiento buscado en señal: {pnl_objetivo} puntos")

        # 2. FILTRAR MERCADO EN LA VENTANA CRONOLÓGICA Y HORARIA
        df_v = df_mkt[(df_mkt.index >= muro_inf) & (df_mkt.index <= muro_sup)].copy()
        df_v = df_v.between_time(HORA_INICIO_OP, HORA_FIN_OP)

        if df_v.empty:
            print(f"{Fore.RED}No hay datos de mercado en la ventana de servicio entre trades.")
            continue

        # 3. ENCONTRAR TODOS LOS MATCHES (Umbral de tolerancia de 10 puntos para la horquilla)
        # Probamos cada minuto de la ventana como posible apertura
        resultados_viables = []
        
        # Simplificación: Buscamos dónde el precio Close - mkt_open == pnl_objetivo
        for start_dt, row in df_v.iterrows():
            mkt_open_val = row['close']
            df_posibles_cierres = df_v[df_v.index > start_dt].copy()
            
            if 'largo' in t_act['s'].lower():
                df_posibles_cierres['diff'] = (df_posibles_cierres['close'] - mkt_open_val) - pnl_objetivo
            else:
                df_posibles_cierres['diff'] = (mkt_open_val - df_posibles_cierres['close']) - pnl_objetivo
            
            # Filtramos los que tengan un error menor a 5 puntos (paralelismo casi exacto)
            matches = df_posibles_cierres[df_posibles_cierres['diff'].abs() <= 5]
            if not matches.empty:
                for end_dt, match_row in matches.iterrows():
                    resultados_viables.append((start_dt, end_dt))

        if not resultados_viables:
            print(f"{Fore.RED}No se encontró ningún periodo que cumpla el PNL de {pnl_objetivo} puntos.")
        else:
            first_match = resultados_viables[0]
            last_match = resultados_viables[-1]
            
            print(f"\n{Fore.GREEN}🎯 HORQUILLA DE PROBABILIDAD DETECTADA:")
            print(f"Primer escenario posible:  Desde {first_match[0]} hasta {first_match[1]}")
            print(f"Último escenario posible:  Desde {last_match[0]} hasta {last_match[1]}")
            print(f"\n{Fore.WHITE}Esto significa que tu operación de {pnl_objetivo} pts ocurrió")
            print(f"entre el {first_match[0].strftime('%d/%m')} y el {last_match[1].strftime('%d/%m')}.")
            print(f"{Fore.YELLOW}Usa este rango para calcular el MAE real en el gráfico.")

if __name__ == "__main__":
    generar_horquilla()