import statsapi
import pandas as pd
from supabase import create_client, Client
import os
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# --- CONFIGURACIÓN ---
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

if not supabase_url or not supabase_key:
    raise ValueError("Error: Variables de entorno SUPABASE_URL o SUPABASE_KEY no encontradas.")

supabase: Client = create_client(supabase_url, supabase_key)

# --- FECHAS DINÁMICAS (HOY - 1 DÍA) ---
hoy = datetime.now()
ayer = hoy - timedelta(days=1)
fecha_str = ayer.strftime('%m/%d/%Y')
fecha_sql = ayer.strftime('%Y-%m-%d')

print(f"\n{'='*60}")
print(f" 🚜 SINDICATO MLB: SINCRONIZACIÓN DIARIA TOTAL ({fecha_str})")
print(f"{'='*60}")

# Diccionarios de almacenamiento en memoria
db_games = {}
db_weather = {}
db_umpires = {}
db_innings = {}
db_pitchers = {}
db_batters = {}
db_pbp = {}
db_statcast = {}
db_lineups = {}

print(" -> Consultando calendario de MLB...")
schedule = statsapi.schedule(date=fecha_str)
juegos_validos = [g['game_id'] for g in schedule if g['game_type'] == 'R' and g['status'] in ['Final', 'Completed Early']]

if not juegos_validos:
    print(" ⚠️ No se encontraron juegos de temporada regular finalizados para ayer. Abortando sincronización.")
    exit()

print(f" -> Encontrados {len(juegos_validos)} juegos. Extrayendo Boxscores, Clima, Umpires y PBP...")

for i, pk in enumerate(juegos_validos):
    try:
        # LLAMADA MAESTRA
        game_data = statsapi.get('game', {'gamePk': pk})
        
        game_date = str(game_data['gameData']['datetime'].get('originalDate'))
        status = str(game_data['gameData']['status']['detailedState'])
        
        # 1. GAMES HISTORY
        teams_info = game_data['gameData']['teams']
        linescore = game_data['liveData']['linescore']
        
        db_games[pk] = {
            "game_pk": int(pk),
            "game_date": game_date,
            "home_team_id": int(teams_info['home']['id']),
            "home_team_name": str(teams_info['home']['name']),
            "away_team_id": int(teams_info['away']['id']),
            "away_team_name": str(teams_info['away']['name']),
            "venue_id": int(game_data['gameData']['venue']['id']),
            "home_score": int(linescore['teams']['home'].get('runs', 0)),
            "away_score": int(linescore['teams']['away'].get('runs', 0)),
            "status": status
        }

        # 2. WEATHER
        weather_info = game_data['gameData'].get('weather', {})
        temp_str = weather_info.get('temp', '')
        db_weather[pk] = {
            "game_pk": int(pk),
            "temp_f": int(temp_str) if str(temp_str).isdigit() else None,
            "wind_speed_mph": None,
            "wind_direction": str(weather_info.get('wind', '')),
            "condition": str(weather_info.get('condition', '')),
            "is_dome_open": None
        }

        # 3. UMPIRES
        officials = game_data['liveData']['boxscore'].get('officials', [])
        for ump in officials:
            if ump.get('officialType') == 'Home Plate':
                db_umpires[pk] = {
                    "game_pk": int(pk),
                    "umpire_id": int(ump['official']['id']),
                    "umpire_name": str(ump['official']['fullName']),
                    "home_plate": True
                }
                break

        # 4. INNINGS
        innings_data = linescore.get('innings', [])
        for inn in innings_data:
            inn_num = int(inn['num'])
            db_innings[f"{pk}_{inn_num}"] = {
                "game_pk": int(pk),
                "inning_number": inn_num,
                "away_runs": int(inn['away'].get('runs', 0)) if 'away' in inn else 0,
                "home_runs": int(inn['home'].get('runs', 0)) if 'home' in inn else 0,
                "away_hits": int(inn['away'].get('hits', 0)) if 'away' in inn else 0,
                "home_hits": int(inn['home'].get('hits', 0)) if 'home' in inn else 0
            }

        # 5. BOXSCORES Y LINEUPS
        box = game_data['liveData']['boxscore']['teams']
        for side in ['home', 'away']:
            team_id = int(box[side]['team']['id'])
            players = box[side].get('players', {})
            
            pitchers_ids = box[side].get('pitchers', [])
            starter_id = pitchers_ids[0] if pitchers_ids else None
            
            for order_idx, pid in enumerate(box[side].get('battingOrder', [])):
                db_lineups[f"{pk}_{pid}"] = {
                    "game_pk": int(pk),
                    "team_id": team_id,
                    "batter_id": int(pid),
                    "batting_order": int(order_idx + 1),
                    "field_position": str(box[side]['players'].get(f'ID{pid}', {}).get('position', {}).get('abbreviation', ''))
                }
            
            for pid_key, p_info in players.items():
                pid = int(p_info['person']['id'])
                pname = str(p_info['person']['fullName'])
                stats_p = p_info.get('stats', {}).get('pitching', {})
                stats_b = p_info.get('stats', {}).get('batting', {})
                
                if stats_p:
                    db_pitchers[f"{pk}_{pid}"] = {
                        'game_pk': int(pk), 'game_date': game_date, 'pitcher_id': pid,
                        'pitcher_name': pname, 'team_id': team_id, 'is_starter': bool(pid == starter_id),
                        'innings_pitched': float(stats_p.get('inningsPitched', 0)),
                        'strikeouts': int(stats_p.get('strikeOuts', 0)), 'walks': int(stats_p.get('baseOnBalls', 0)),
                        'hits_allowed': int(stats_p.get('hits', 0)), 'earned_runs': int(stats_p.get('earnedRuns', 0)),
                        'pitches_thrown': int(stats_p.get('numberOfPitches', 0)), 'batters_faced': int(stats_p.get('battersFaced', 0)),
                        'strikes': int(stats_p.get('strikes', 0))
                    }
                
                if stats_b and 'battingOrder' in p_info:
                    db_batters[f"{pk}_{pid}"] = {
                        'game_pk': int(pk), 'game_date': game_date, 'batter_id': pid,
                        'batter_name': pname, 'team_id': team_id,
                        'batting_order': int(p_info.get('battingOrder', 0)) // 100,
                        'at_bats': int(stats_b.get('atBats', 0)), 'runs': int(stats_b.get('runs', 0)),
                        'hits': int(stats_b.get('hits', 0)), 'home_runs': int(stats_b.get('homeRuns', 0)),
                        'rbi': int(stats_b.get('rbi', 0)), 'walks': int(stats_b.get('baseOnBalls', 0)),
                        'strikeouts': int(stats_b.get('strikeOuts', 0))
                    }

        # 6. PLAY-BY-PLAY Y STATCAST
        all_plays = game_data.get('liveData', {}).get('plays', {}).get('allPlays', [])
        for play in all_plays:
            at_bat_idx = play.get('atBatIndex')
            result = play.get('result', {})
            about = play.get('about', {})
            matchup = play.get('matchup', {})
            hit_data = play.get('hitData', {})
            
            p_id = matchup.get('pitcher', {}).get('id')
            b_id = matchup.get('batter', {}).get('id')
            
            if p_id and b_id:
                pbp_key = f"{pk}_{at_bat_idx}"
                db_pbp[pbp_key] = {
                    "game_pk": int(pk),
                    "at_bat_index": int(at_bat_idx),
                    "pitcher_id": int(p_id),
                    "batter_id": int(b_id),
                    "inning": int(about.get('inning', 0)),
                    "bat_side": str(matchup.get('batSide', {}).get('code', '')),
                    "pitch_hand": str(matchup.get('pitchHand', {}).get('code', '')),
                    "event_type": str(result.get('event', '')),
                    "is_k": bool(result.get('event') == 'Strikeout'),
                    "is_bb": bool(result.get('event') in ['Walk', 'Intent Walk'])
                }

                if hit_data:
                    db_statcast[pbp_key] = {
                        "game_pk": int(pk),
                        "batter_id": int(b_id),
                        "pitcher_id": int(p_id),
                        "inning": int(about.get('inning', 0)),
                        "exit_velocity": float(hit_data.get('launchSpeed', 0)) if hit_data.get('launchSpeed') else None,
                        "launch_angle": float(hit_data.get('launchAngle', 0)) if hit_data.get('launchAngle') else None,
                        "hit_distance": int(hit_data.get('totalDistance', 0)) if hit_data.get('totalDistance') else None,
                        "event_result": str(result.get('event', ''))
                    }

        time.sleep(0.3)
    except Exception as e:
        print(f"    ⚠️ Error procesando PK {pk}: {e}")

# --- INYECCIÓN A SUPABASE ---
print("\n 🚀 INYECTANDO A SUPABASE...")
def upsert_lote(tabla, datos_dict, on_conflict=None):
    lista = list(datos_dict.values())
    if not lista: return
    for i in range(0, len(lista), 1000):
        lote = lista[i:i+1000]
        try:
            if on_conflict:
                supabase.table(tabla).upsert(lote, on_conflict=on_conflict).execute()
            else:
                supabase.table(tabla).upsert(lote).execute()
        except Exception as e:
            print(f"     Error en {tabla}: {e}")

upsert_lote('mlb_games_history', db_games, 'game_pk')
upsert_lote('game_weather', db_weather, 'game_pk')
upsert_lote('game_umpires', db_umpires, 'game_pk')
upsert_lote('game_innings', db_innings)
upsert_lote('game_lineups', db_lineups)
upsert_lote('pitcher_game_logs', db_pitchers)
upsert_lote('batter_game_logs', db_batters)
upsert_lote('game_plate_appearances', db_pbp)
upsert_lote('statcast_batted_balls', db_statcast)

# --- CAPA INTELECTUAL: BULLPEN & VIAJES ---
print("\n ⚙️ CALCULANDO INTELIGENCIA (BULLPEN & VIAJES)...")

# Bullpen
res_p = supabase.table('pitcher_game_logs').select('*').eq('game_date', fecha_sql).eq('is_starter', False).execute()
df_rel = pd.DataFrame(res_p.data)
bullpen_lote = []
if not df_rel.empty:
    for _, row in df_rel.iterrows():
        bullpen_lote.append({
            "game_date": str(row['game_date']),
            "pitcher_id": int(row['pitcher_id']),
            "team_id": int(row['team_id']),
            "pitches_l1": int(row['pitches_thrown']),
            "is_burned": bool(row['pitches_thrown'] > 30)
        })
    if bullpen_lote:
        supabase.table('bullpen_availability').upsert(bullpen_lote).execute()

# Viajes (Requiere buscar los últimos 10 días para ver cuándo jugaron por última vez)
limite_rest = (ayer - timedelta(days=10)).strftime('%Y-%m-%d')
res_g = supabase.table('mlb_games_history').select('*').gte('game_date', limite_rest).execute()
df_g = pd.DataFrame(res_g.data).sort_values('game_date')
travel_lote = []
if not df_g.empty:
    for tid in pd.concat([df_g['home_team_id'], df_g['away_team_id']]).unique():
        t_games = df_g[(df_g['home_team_id'] == tid) | (df_g['away_team_id'] == tid)].copy()
        t_games['prev_date'] = t_games['game_date'].shift(1)
        
        # Solo procesamos el juego de ayer
        game_ayer = t_games[t_games['game_date'] == fecha_sql]
        if not game_ayer.empty:
            row = game_ayer.iloc[0]
            rest = 3 if pd.isna(row['prev_date']) else (pd.to_datetime(row['game_date']) - pd.to_datetime(row['prev_date'])).days
            travel_lote.append({
                "game_pk": int(row['game_pk']),
                "team_id": int(tid),
                "game_date": str(row['game_date']),
                "is_home_team": bool(row['home_team_id'] == tid),
                "rest_days": int(rest)
            })
    if travel_lote:
        supabase.table('team_travel_logs').upsert(travel_lote).execute()

print("\n 🏁 ACTUALIZACIÓN DIARIA COMPLETADA. SINDICATO AL 100%.")
