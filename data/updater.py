import os
import time
import statistics
from datetime import datetime, timedelta, timezone
import requests
import statsapi
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

def obtener_consenso_mediana(odds_list):
    if not odds_list: return None, None
    precios = [o['price'] for o in odds_list]
    puntos = [o.get('point') for o in odds_list]
    
    precios_seguros = [p for p in precios if not (-99 < p < 99)]
    if not precios_seguros: return None, None
    
    puntos_puros = [p for p in puntos if p is not None]
    pt_med = statistics.median(puntos_puros) if puntos_puros else None
    
    return int(statistics.median(precios_seguros)), pt_med

class DailyUpdater:
    """
    Clase orquestadora para la ingesta y automatización diaria de datos 
    hacia Supabase (resultados de ayer y momios de hoy).
    """
    
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.odds_key = os.getenv("ODDS_API_KEY")
        
        if not self.url or not self.key:
            raise ValueError("Error: Faltan credenciales de Supabase en el .env")
            
        self.client: Client = create_client(self.url, self.key)

    def actualizar_resultados_ayer(self):
        """
        Calcula la fecha de ayer y descarga el calendario/resultados usando statsapi.
        Luego hace upsert a Supabase en la tabla 'mlb_games_history'.
        """
        print("Iniciando extracción de resultados de ayer...")
        
        # Calcular dinámicamente la fecha de ayer (MM/DD/YYYY)
        ayer = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y')
        
        schedule = statsapi.schedule(start_date=ayer, end_date=ayer)
        juegos_unicos = {}
        
        for game in schedule:
            if game.get('game_type') == 'R': # Temporada Regular
                game_pk = game['game_id']
                juegos_unicos[game_pk] = {
                    "game_pk": game_pk,
                    "game_date": game['game_date'],
                    "home_team_id": game['home_id'],
                    "home_team_name": game['home_name'],
                    "away_team_id": game['away_id'],
                    "away_team_name": game['away_name'],
                    "venue_id": game.get('venue_id'),
                    "home_score": game.get('home_score', 0),
                    "away_score": game.get('away_score', 0),
                    "status": game['status']
                }
                
        if not juegos_unicos:
            print("No se encontraron juegos regulares para la fecha de ayer.")
            return
            
        # Preparar data para Supabase
        records = list(juegos_unicos.values())
        
        # Upsert a la base de datos
        self.client.table('mlb_games_history').upsert(records).execute()
        print(f"Éxito: {len(records)} juegos de ayer cargados en 'mlb_games_history'.")

    def extraer_calendario_hoy(self):
        """
        Descarga el fixture (horario) de juegos pautados para el día de hoy y lo inyecta 
        ciego (sin resultados) en la tabla mlb_games_history.
        Esto permite que descargar_momios_hoy() encuentre los IDs para hacer match,
        manteniendo ciego al modelo predictivo si el juego ya arrancó.
        """
        print("Iniciando inyección de calendario Ciego para hoy...")
        
        hoy = datetime.now(timezone.utc).strftime('%m/%d/%Y')
        schedule = statsapi.schedule(start_date=hoy, end_date=hoy)
        juegos_hoy = {}
        
        for game in schedule:
            if game.get('game_type') == 'R':
                game_pk = game['game_id']
                juegos_hoy[game_pk] = {
                    "game_pk": game_pk,
                    "game_date": game['game_date'],
                    "home_team_id": game['home_id'],
                    "home_team_name": game['home_name'],
                    "away_team_id": game['away_id'],
                    "away_team_name": game['away_name'],
                    "venue_id": game.get('venue_id'),
                    "home_score": 0, # Sombreado preventivo cero Data Leakage
                    "away_score": 0, # Sombreado preventivo cero Data Leakage
                    "status": "Scheduled" # Forzamos estado pre-juego
                }
                
        if not juegos_hoy:
            print("No hay juegos regulares programados para hoy en el calendario.")
            return
            
        records = list(juegos_hoy.values())
        self.client.table('mlb_games_history').upsert(records).execute()
        print(f"Éxito: {len(records)} juegos blindados agregados al calendario de hoy.")

    def descargar_momios_hoy(self):
        """
        Descarga momios de The Odds API, calcula un consenso real promediando las casas de apuestas,
        y cruza los datos con el game_pk de la tabla mlb_games_history para evitar duplicados.
        """
        print(" -> Conectando a The Odds API para extraer momios multi-mercado de hoy...")
        
        # 1. Obtener los juegos de hoy de Supabase para poder hacer el match del game_pk
        hoy_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        res_games = self.client.table('mlb_games_history').select('game_pk, game_date, home_team_name, away_team_name').eq('game_date', hoy_str).execute()
        
        if not res_games.data:
            print(" -> No hay juegos programados en la BD para hoy. No se pueden cruzar momios.")
            return

        df_juegos_hoy = pd.DataFrame(res_games.data)
        
        # 2. Llamada a The Odds API
        url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?apiKey={self.odds_key}&regions=us&markets=h2h,spreads,totals&oddsFormat=american"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f" -> Error The Odds API: {response.text}")
            return
            
        odds_data = response.json()
        registros_limpios = []
        
        # 3. Procesamiento y Consenso Real
        ahora_utc = datetime.now(timezone.utc)
        
        for game in odds_data:
            # BLINDAJE: Ignorar momios live de juegos que ya empezaron para proteger linea pre-partido
            commence_str = game.get('commence_time', '')
            if commence_str:
                com_dt = datetime.fromisoformat(commence_str.replace('Z', '+00:00'))
                if com_dt < ahora_utc:
                    continue # Saltarse juegos en curso
                    
            # Match de nombres de equipos (Odds API usa nombres completos)
            home_api = game['home_team']
            away_api = game['away_team']
            
            # Buscar el game_pk correspondiente en nuestra BD (Usar & estricto)
            match = df_juegos_hoy[
                (df_juegos_hoy['home_team_name'].str.contains(home_api.split()[-1], case=False, na=False)) &
                (df_juegos_hoy['away_team_name'].str.contains(away_api.split()[-1], case=False, na=False))
            ]
            
            if match.empty:
                continue
                
            # Asignación Segura Anti-Doubleheaders
            game_pk = None
            for _, row in match.iterrows():
                pk_val = int(row['game_pk'])
                # Verificamos si este PK ya existe en los registros acumulados
                ya_existe = any(r['game_pk'] == pk_val for r in registros_limpios)
                if not ya_existe:
                    game_pk = pk_val
                    break
                    
            if game_pk is None:
                continue
            # Extraer cuotas F5 con una sub-llamada por partido (Exclusivo de este endpoint)
            event_id = game.get('id')
            url_f5 = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds?apiKey={self.odds_key}&regions=us&markets=h2h_1st_5_innings,spreads_1st_5_innings,totals_1st_5_innings&oddsFormat=american"
            
            try:
                # 1 token quemado per game
                res_f5 = requests.get(url_f5)
                f5_game = res_f5.json() if res_f5.status_code == 200 else {}
            except Exception:
                f5_game = {}

            # Extraer cuotas Multi-Market para este partido
            mercados_dict = {
                'h2h': {'Home': [], 'Away': []},
                'spreads': {'Home': [], 'Away': []},
                'totals': {'Over': [], 'Under': []},
                'h2h_1st_5_innings': {'Home': [], 'Away': []},
                'spreads_1st_5_innings': {'Home': [], 'Away': []},
                'totals_1st_5_innings': {'Over': [], 'Under': []}
            }
            
            def parsear_bookmakers(bookmakers_list):
                for bookmaker in bookmakers_list:
                    for market in bookmaker.get('markets', []):
                        mk = market.get('key')
                        if mk not in mercados_dict: continue
                        
                        for outcome in market.get('outcomes', []):
                            price = int(outcome.get('price', 0))
                            point = outcome.get('point')
                            
                            if 'h2h' in mk or 'spreads' in mk:
                                out_name = 'Home' if outcome.get('name') == home_api else 'Away'
                            elif 'totals' in mk:
                                out_name = outcome.get('name')
                            else:
                                continue
                                
                            if out_name in mercados_dict[mk]:
                                mercados_dict[mk][out_name].append({'price': price, 'point': point})

            # Compilar Juego Completo
            parsear_bookmakers(game.get('bookmakers', []))
            
            # Compilar Juego F5
            if isinstance(f5_game, dict) and 'bookmakers' in f5_game:
                parsear_bookmakers(f5_game.get('bookmakers', []))
            
            timestamp_ahora = datetime.now(timezone.utc).isoformat()
            fecha_partido = match.iloc[0]['game_date']
            
            for mk, outcomes in mercados_dict.items():
                keys = list(outcomes.keys())
                if len(keys) != 2: continue
                
                price_1, pt_1 = obtener_consenso_mediana(outcomes[keys[0]])
                price_2, pt_2 = obtener_consenso_mediana(outcomes[keys[1]])
                
                if price_1 is not None and price_2 is not None:
                    registros_limpios.extend([
                        {
                            "game_pk": game_pk,
                            "game_date": fecha_partido,
                            "market_key": mk,
                            "outcome_name": keys[0],
                            "price": price_1,
                            "point": pt_1,
                            "bookmaker": "Consensus_Avg",
                            "timestamp_recorded": timestamp_ahora
                        },
                        {
                            "game_pk": game_pk,
                            "game_date": fecha_partido,
                            "market_key": mk,
                            "outcome_name": keys[1],
                            "price": price_2,
                            "point": pt_2,
                            "bookmaker": "Consensus_Avg",
                            "timestamp_recorded": timestamp_ahora
                        }
                    ])

        # 4. Inyección en Supabase
        if registros_limpios:
            try:
                self.client.table('historical_odds').upsert(registros_limpios, on_conflict='game_pk,market_key,outcome_name').execute()
                print(f" -> EXITO: {len(registros_limpios)} registros de momios consenso inyectados correctamente.")
            except Exception as e:
                print(f" -> Error al insertar momios: {e}")

    def auditar_y_rellenar_huecos(self, start_date: str, end_date: str):
        """
        Descarga y rellena resultados históricos perdidos para un rango de fechas específico.
        Utiliza el mismo formato de la actualización diaria para mantener la continuidad en Supabase.
        
        :param start_date: Fecha de inicio en formato 'MM/DD/YYYY'
        :param end_date: Fecha de cierre en formato 'MM/DD/YYYY'
        """
        print(f"Iniciando auditoría y reconstrucción desde {start_date} hasta {end_date}...")
        
        schedule = statsapi.schedule(start_date=start_date, end_date=end_date)
        juegos_unicos = {}
        
        for game in schedule:
            if game.get('game_type') == 'R': # Filtrar únicamente Temporada Regular
                game_pk = game['game_id']
                juegos_unicos[game_pk] = {
                    "game_pk": game_pk,
                    "game_date": game['game_date'],
                    "home_team_id": game['home_id'],
                    "home_team_name": game['home_name'],
                    "away_team_id": game['away_id'],
                    "away_team_name": game['away_name'],
                    "venue_id": game.get('venue_id'),
                    "home_score": game.get('home_score', 0),
                    "away_score": game.get('away_score', 0),
                    "status": game['status']
                }
                
        if not juegos_unicos:
            print(f"No se detectaron juegos de temporada regular entre {start_date} y {end_date}.")
            return
            
        records = list(juegos_unicos.values())
        
        # Inyectar el lote masivo a Supabase
        self.client.table('mlb_games_history').upsert(records).execute()
        print(f"Backfill 100% completado: {len(records)} juegos inyectados/actualizados correctamente.")

    def run_daily_update(self):
        """
        Método Orquestador: Corre un flujo controlado de actualizaciones.
        """
        print(">> Ejecutando Orquestador Diario de Ingesta (DailyUpdater) <<")
        
        # Bloque de Resultados (Ayer)
        try:
            self.actualizar_resultados_ayer()
        except Exception as e:
            print(f"[FAILED] Error capturado actualizando resultados: {e}")
            
        # Bloque de Calendario Ciego (Hoy)
        try:
            self.extraer_calendario_hoy()
        except Exception as e:
            print(f"[FAILED] Error armando calendario del día: {e}")
            
        # Bloque de Momios (Hoy)
        try:
            self.descargar_momios_hoy()
        except Exception as e:
            print(f"[FAILED] Error capturado descargando momios: {e}")
            
        print(">> Ingesta Finalizada <<")

    def auditar_y_rellenar_momios_historicos(self, dry_run=True, force_overwrite=False):
        """
        Bloque 1: Diagnóstico de juegos de 2026 sin cuotas históricas.
        Escanea estrictamente desde el 2026-03-20.
        """
        print("\n--- INICIANDO AUDITORÍA HISTÓRICA (MODO SEGURO) ---")
        
        # 1. Obtener TODOS los juegos de resultados (Scope abierto para buscar cualquier hueco del pasado)
        res_games = self.client.table('mlb_games_history').select('game_pk, game_date, home_team_name, away_team_name').execute()
        
        if not res_games.data:
            print("No se encontraron juegos en la base de datos de resultados.")
            return
            
        # 2. Obtener los game_pk que YA poseen momios en la tabla historical_odds
        res_odds = self.client.table('historical_odds').select('game_pk').execute()
        
        # 3. Mapear conjuntos para localizar la intersección de vacío (diferencia de conjuntos)
        juegos_dict = {g['game_pk']: g for g in res_games.data}
        odds_pks = {o['game_pk'] for o in res_odds.data}
        
        # 4. Encontrar los huérfanos (A diferencia de B, o Forzar Sobrescritura Total)
        if force_overwrite:
            print(">> MODO DE SOBRESCRITURA TOTAL ACTIVADO: Purgando y reemplazando todos los momios 2026 <<")
            huerfanos = list(juegos_dict.values())
        else:
            huerfanos = [g for pk, g in juegos_dict.items() if pk not in odds_pks]
        
        if not huerfanos:
            print("¡Tu bóveda está perfectamente sellada! No hay juegos del 2026 huérfanos de momios.")
            return
            
        # 5. Agrupar la eficiencia por fechas para minimizar llamadas a The Odds API
        fechas_huerfanas = {}
        for h in huerfanos:
            fecha = h['game_date']
            if fecha not in fechas_huerfanas:
                fechas_huerfanas[fecha] = []
            fechas_huerfanas[fecha].append(h)
            
        dias_distintos = len(fechas_huerfanas)
        juegos_totales = len(huerfanos)
        costo_creditos = dias_distintos * 10
        
        # Imprimir el reporte de Consola según las directrices 
        print("\n[RESULTADO DEL DIAGNÓSTICO PASIVO]")
        print(f"Se encontraron {juegos_totales} juegos sin momios, agrupados en {dias_distintos} días distintos.")
        print(f"Se requerirán {dias_distintos} llamadas al endpoint histórico (consumiendo {dias_distintos} * 10 = {costo_creditos} créditos totales).")
        print("Estrategia de Snapshot pactada: 08:00:00Z (Madrugada/Mañana para evitar juegos empezados).")
        
        if dry_run:
            print("\nMODO DRY RUN: Finalizado. Análisis seguro concluido. Cero llamadas ejecutadas a The Odds API.")
            return
            
        print("\n>> DESBLOQUEANDO SEGURO: MODO EJECUCIÓN ACTIVO <<")
        url_historical = "https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds"
        
        for fecha, juegos_del_dia in fechas_huerfanas.items():
            # Construir el Snapshot exacto (8 AM UTC para blindaje pre-juego)
            snapshot_date = f"{fecha}T08:00:00Z"
            print(f"\n[CONECTANDO] {fecha} -> Solicitando snapshot histórico de las {snapshot_date}")
            
            params = {
                "apiKey": self.odds_key,
                "regions": "us",
                "markets": "h2h,spreads,totals",
                "oddsFormat": "american",
                "date": snapshot_date
            }
            
            response = requests.get(url_historical, params=params)
            
            if response.status_code != 200:
                print(f" -> [ERROR API] Código {response.status_code} en fecha {fecha}. Mensaje: {response.text}")
                continue
                
            # La API histórica anida los juegos dentro de 'data' al nivel raíz
            odds_data = response.json().get('data', [])
            
            if not odds_data:
                print(f" -> Advertencia: La API respondió correctamente pero la lista 'data' vino vacía.")
                continue
                
            df_juegos_dia = pd.DataFrame(juegos_del_dia)
            registros_limpios = []
            
            for game in odds_data:
                home_api = game.get('home_team', '')
                away_api = game.get('away_team', '')
                
                # Match estricto AND 
                match = df_juegos_dia[
                    (df_juegos_dia['home_team_name'].str.contains(home_api.split()[-1], case=False, na=False)) &
                    (df_juegos_dia['away_team_name'].str.contains(away_api.split()[-1], case=False, na=False))
                ]
                
                if match.empty:
                    continue
                    
                # Blindaje Anti-Doubleheaders / PK duplicados dentro del mismo payload
                game_pk = None
                for _, row in match.iterrows():
                    pk_val = int(row['game_pk'])
                    ya_existe = any(r['game_pk'] == pk_val for r in registros_limpios)
                    if not ya_existe:
                        game_pk = pk_val
                        break
                        
                if game_pk is None:
                    continue
                mercados_dict = {
                    'h2h': {'Home': [], 'Away': []},
                    'spreads': {'Home': [], 'Away': []},
                    'totals': {'Over': [], 'Under': []}
                }
                
                for bookmaker in game.get('bookmakers', []):
                    for market in bookmaker.get('markets', []):
                        mk = market.get('key')
                        if mk not in mercados_dict: continue
                        
                        for outcome in market.get('outcomes', []):
                            price = int(outcome.get('price', 0))
                            point = outcome.get('point')
                            
                            if mk in ['h2h', 'spreads']:
                                out_name = 'Home' if outcome.get('name') == home_api else 'Away'
                            elif mk == 'totals':
                                out_name = outcome.get('name')
                            else:
                                continue
                                
                            if out_name in mercados_dict[mk]:
                                mercados_dict[mk][out_name].append({'price': price, 'point': point})
                                
                timestamp_ahora = datetime.now(timezone.utc).isoformat()
                fecha_partido = match.iloc[0]['game_date']
                
                for mk, outcomes in mercados_dict.items():
                    keys = list(outcomes.keys())
                    if len(keys) != 2: continue
                    
                    price_1, pt_1 = obtener_consenso_mediana(outcomes[keys[0]])
                    price_2, pt_2 = obtener_consenso_mediana(outcomes[keys[1]])
                    
                    if price_1 is not None and price_2 is not None:
                        registros_limpios.extend([
                            {
                                "game_pk": game_pk,
                                "game_date": fecha_partido,
                                "market_key": mk,
                                "outcome_name": keys[0],
                                "price": price_1,
                                "point": pt_1,
                                "bookmaker": "Consensus_Avg",
                                "timestamp_recorded": timestamp_ahora
                            },
                            {
                                "game_pk": game_pk,
                                "game_date": fecha_partido,
                                "market_key": mk,
                                "outcome_name": keys[1],
                                "price": price_2,
                                "point": pt_2,
                                "bookmaker": "Consensus_Avg",
                                "timestamp_recorded": timestamp_ahora
                            }
                        ])
                    
            if registros_limpios:
                try:
                    self.client.table('historical_odds').upsert(
                        registros_limpios, 
                        on_conflict="game_pk,market_key,outcome_name"
                    ).execute()
                    print(f"   -> EXITO: {len(registros_limpios)} registros consenso minados y guardados en la BD.")
                except Exception as e:
                    print(f"   -> [ERROR DB] Falló la inserción en Supabase: {e}")
            else:
                print(f"   -> No se formaron líneas viables para {fecha}.")
                
            # Cortesía Rate-Limit (The Odds API limit behavior)
            time.sleep(1)
            
        print("\n>> MISIÓN CUMPLIDA: AUDITORÍA HISTÓRICA 100% COMPLETADA <<")
