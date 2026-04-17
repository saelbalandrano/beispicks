import os
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import xgboost as xgb

# Importaciones de nuestros módulos
from data.updater import DailyUpdater
from models.montecarlo import simular_juego_mc_legal

load_dotenv()
supabase: Client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
pd.set_option('future.no_silent_downcasting', True)

def extraer_tabla(tabla):
    print(f" -> Extrayendo {tabla}...")
    datos = []
    offset, limit = 0, 1000
    while True:
        try:
            res = supabase.table(tabla).select('*').range(offset, offset + limit - 1).execute()
            if not res.data: break
            datos.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
            time.sleep(0.05)
        except Exception as e:
            print(f"    [Error] Fallo inminente al extraer '{tabla}': {e}")
            break
    return pd.DataFrame(datos)

def obtener_probabilidad_vegas(cuota):
    if pd.isna(cuota) or cuota == 0: return 50.0
    return (abs(cuota) / (abs(cuota) + 100)) * 100 if cuota < 0 else (100 / (cuota + 100)) * 100

def calcular_kelly_stake(confianza_pct, cuota_americana, bankroll, fraction=0.25, cap=0.05):
    """
    Quarter-Kelly con techo del 5% del bankroll.
    confianza_pct: probabilidad del modelo en % (ej: 62.5)
    cuota_americana: odds americanas (ej: -150 o +130)
    bankroll: capital actual
    """
    p = confianza_pct / 100.0
    if cuota_americana < 0:
        decimal = 1 + (100 / abs(cuota_americana))
    else:
        decimal = 1 + (cuota_americana / 100)
    
    kelly_full = (p * (decimal - 1) - (1 - p)) / (decimal - 1)
    kelly_full = max(0, kelly_full)
    kelly_adj = kelly_full * fraction
    stake = round(bankroll * kelly_adj, 2)
    stake = max(10, min(stake, bankroll * cap))
    return stake

def run_sindicato():
    print("\n" + "="*50)
    print(" INICIANDO OPERACIÓN SINDICATO (CONSENSO 5 CAPAS)")
    print("="*50)
    
    # FASE 0: LIQUIDACIÓN DE BANKROLL Y AUDITORÍA HISTÓRICA PENDIENTE
    print("\n[FASE 0] Liquidando apuestas pendientes en el Ledger de Supabase...")
    from data.liquidator import SindicatoLiquidator
    liq = SindicatoLiquidator()
    liq.liquidar_juegos_pendientes()

    # FASE 1: ACTUALIZACIÓN DIARIA
    print("\n[FASE 1] Ejecutando Updater Diario...")
    updater = DailyUpdater()
    updater.run_daily_update()
    
    # Preparar memoria para el Ledger de hoy y evitar duplicados
    hoy_dt = datetime.now(timezone.utc).date()
    try:
        res_ledger = supabase.table('sindicato_ledger').select('game_pk').eq('game_date', str(hoy_dt)).execute()
        existing_ledger_pks = [r['game_pk'] for r in res_ledger.data] if res_ledger.data else []
    except Exception:
        existing_ledger_pks = []
    ledger_records = []
    
    # FASE 2: CONSTRUCCIÓN DE LA MATRIZ
    print("\n[FASE 2] Ensamblando Matriz Quant...")
    df_games = extraer_tabla('mlb_games_history').dropna(subset=['home_score', 'away_score'])
    df_travel = extraer_tabla('team_travel_logs')
    df_pitchers = extraer_tabla('pitcher_pregame_features')
    df_logs_pitchers = extraer_tabla('pitcher_game_logs')
    df_batters = extraer_tabla('batter_pregame_features')
    df_logs_batters = extraer_tabla('batter_game_logs')
    df_odds = extraer_tabla('historical_odds')
    df_bullpen = extraer_tabla('bullpen_availability')

    df_odds = df_odds[(df_odds['price'] <= -100) | (df_odds['price'] >= 100)]
    df_bullpen['game_date'] = pd.to_datetime(df_bullpen['game_date']).dt.date
    bullpen_agg = df_bullpen.groupby(['game_date', 'team_id'])['is_burned'].sum().reset_index()

    df_batters_full = df_batters.merge(df_logs_batters[['game_pk', 'batter_id', 'team_id']], on=['game_pk', 'batter_id'], how='inner')
    team_offense = df_batters_full.groupby(['game_pk', 'team_id']).agg(
        team_avg_exit_velo=('avg_exit_velo_recent', 'mean'), 
        team_hard_hit_pct=('hard_hit_pct_recent', 'mean'), 
        team_batter_k_pct=('k_pct_recent', 'mean')
    ).reset_index()

    df_games['target_home_win'] = (df_games['home_score'] > df_games['away_score']).astype(int)
    df_games['game_date_dt'] = pd.to_datetime(df_games['game_date']).dt.date
    
    home_odds = df_odds[df_odds['outcome_name'].str.lower() == 'home'][['game_pk', 'price']].rename(columns={'price': 'home_moneyline'}).drop_duplicates(subset=['game_pk'])
    away_odds = df_odds[df_odds['outcome_name'].str.lower() == 'away'][['game_pk', 'price']].rename(columns={'price': 'away_moneyline'}).drop_duplicates(subset=['game_pk'])

    df_travel_home = df_travel[df_travel['is_home_team'] == True].add_prefix('home_')
    df_travel_away = df_travel[df_travel['is_home_team'] == False].add_prefix('away_')
    pitcher_stats = df_logs_pitchers[df_logs_pitchers['is_starter'] == True][['game_pk', 'pitcher_id', 'team_id']].merge(df_pitchers, on=['game_pk', 'pitcher_id'], how='left')

    matriz = df_games[['game_pk', 'game_date', 'game_date_dt', 'target_home_win', 'home_team_id', 'away_team_id', 'home_team_name', 'away_team_name']].copy()
    matriz = matriz.merge(home_odds, on='game_pk', how='left').merge(away_odds, on='game_pk', how='left')
    matriz = matriz.merge(bullpen_agg.rename(columns={'team_id': 'home_team_id', 'is_burned': 'home_bp_burned'}), left_on=['game_date_dt', 'home_team_id'], right_on=['game_date', 'home_team_id'], how='left').drop(columns=['game_date_y']).rename(columns={'game_date_x': 'game_date'})
    matriz = matriz.merge(bullpen_agg.rename(columns={'team_id': 'away_team_id', 'is_burned': 'away_bp_burned'}), left_on=['game_date_dt', 'away_team_id'], right_on=['game_date', 'away_team_id'], how='left').drop(columns=['game_date_y']).rename(columns={'game_date_x': 'game_date'})
    matriz = matriz.merge(df_travel_home[['home_game_pk', 'home_rest_days', 'home_travel_distance_km']], left_on='game_pk', right_on='home_game_pk', how='left')
    matriz = matriz.merge(df_travel_away[['away_game_pk', 'away_rest_days', 'away_travel_distance_km']], left_on='game_pk', right_on='away_game_pk', how='left')
    matriz = matriz.merge(pitcher_stats.add_prefix('home_pitcher_'), left_on=['game_pk', 'home_team_id'], right_on=['home_pitcher_game_pk', 'home_pitcher_team_id'], how='left')
    matriz = matriz.merge(pitcher_stats.add_prefix('away_pitcher_'), left_on=['game_pk', 'away_team_id'], right_on=['away_pitcher_game_pk', 'away_pitcher_team_id'], how='left')
    matriz = matriz.merge(team_offense.add_prefix('home_offense_'), left_on=['game_pk', 'home_team_id'], right_on=['home_offense_game_pk', 'home_offense_team_id'], how='left')
    matriz = matriz.merge(team_offense.add_prefix('away_offense_'), left_on=['game_pk', 'away_team_id'], right_on=['away_offense_game_pk', 'away_offense_team_id'], how='left')

    features = [
        'home_travel_distance_km', 'away_travel_distance_km', 'home_rest_days', 'away_rest_days',
        'home_pitcher_k_pct_recent', 'home_pitcher_walk_pct_recent', 'home_pitcher_avg_fastball_velo',
        'away_pitcher_k_pct_recent', 'away_pitcher_walk_pct_recent', 'away_pitcher_avg_fastball_velo',
        'home_offense_team_avg_exit_velo', 'home_offense_team_hard_hit_pct', 'home_offense_team_batter_k_pct',
        'away_offense_team_avg_exit_velo', 'away_offense_team_hard_hit_pct', 'away_offense_team_batter_k_pct'
    ]
    matriz[features + ['home_moneyline', 'away_moneyline', 'home_bp_burned', 'away_bp_burned']] = matriz[features + ['home_moneyline', 'away_moneyline', 'home_bp_burned', 'away_bp_burned']].fillna(0)

    # FASE 3: SEGMENTACIÓN DEL TIEMPO Y ENTRENAMIENTO
    print("\n[FASE 3] Entrenando Cerebro Quant...")
    hoy_dt = datetime.now(timezone.utc).date()
    
    train_data = matriz[matriz['game_date_dt'] < hoy_dt].copy()
    juegos_hoy = matriz[(matriz['game_date_dt'] == hoy_dt) & (matriz['home_moneyline'] != 0)].copy()
    
    if juegos_hoy.empty:
        print("\n>>> NO HAY JUEGOS CON MOMIOS PARA HOY. EL SINDICATO DESCANSA.")
        return
        
    modelo_xgb = xgb.XGBClassifier(subsample=0.8, n_estimators=200, max_depth=5, learning_rate=0.01, colsample_bytree=0.8, random_state=42)
    modelo_xgb.fit(train_data[features], train_data['target_home_win'])

    # Reconstruir Memorias (YTD y L15) para los equipos que juegan hoy
    print("[FASE 4] Calculando Memorias YTD y L15...")
    anio_actual = hoy_dt.year
    datos_anio_actual = train_data[pd.to_datetime(train_data['game_date']).dt.year == anio_actual].sort_values(by='game_date')
    
    memoria_YTD = {}
    memoria_L15 = {}
    
    for _, row in datos_anio_actual.iterrows():
        tid_home, tid_away = row['home_team_id'], row['away_team_id']
        if tid_home not in memoria_YTD: memoria_YTD[tid_home] = []; memoria_L15[tid_home] = []
        if tid_away not in memoria_YTD: memoria_YTD[tid_away] = []; memoria_L15[tid_away] = []
        
        res_home_bin = 1 if row['target_home_win'] == 1 else 0
        res_away_bin = 1 if row['target_home_win'] == 0 else 0
        
        memoria_YTD[tid_home].append(res_home_bin); memoria_YTD[tid_away].append(res_away_bin)
        memoria_L15[tid_home].append(res_home_bin); memoria_L15[tid_away].append(res_away_bin)
        
        if len(memoria_L15[tid_home]) > 15: memoria_L15[tid_home].pop(0)
        if len(memoria_L15[tid_away]) > 15: memoria_L15[tid_away].pop(0)

    # FASE 5: EJECUCIÓN DEL CONSENSO
    print("\n[FASE 5] Simulando (10,000 Iters) y Auditando Partidos de Hoy...")
    print("-" * 75)
    
    import json
    import os
    import statsapi
    reporte_diario = []
    
    df_pitchers_sorted = df_pitchers.sort_values('game_date', ascending=False)
    team_off_sorted = team_offense.sort_values('game_pk', ascending=False)

    for _, row_orig in juegos_hoy.iterrows():
        row = row_orig.to_dict()
        tid_home, tid_away = int(row['home_team_id']), int(row['away_team_id'])
        
        try:
            sch = statsapi.schedule(game_id=int(row['game_pk']))
            if sch:
                hp_name = sch[0].get('home_probable_pitcher', '')
                ap_name = sch[0].get('away_probable_pitcher', '')
                
                if hp_name and hp_name.lower() != 'tbd':
                    lookup = statsapi.lookup_player(hp_name)
                    if lookup:
                        hp_hist = df_pitchers_sorted[df_pitchers_sorted['pitcher_id'] == lookup[0]['id']]
                        if not hp_hist.empty:
                            row['home_pitcher_k_pct_recent'] = hp_hist.iloc[0]['k_pct_recent']
                            row['home_pitcher_walk_pct_recent'] = hp_hist.iloc[0]['walk_pct_recent']
                            row['home_pitcher_avg_fastball_velo'] = hp_hist.iloc[0]['avg_fastball_velo']
                
                if ap_name and ap_name.lower() != 'tbd':
                    lookup = statsapi.lookup_player(ap_name)
                    if lookup:
                        ap_hist = df_pitchers_sorted[df_pitchers_sorted['pitcher_id'] == lookup[0]['id']]
                        if not ap_hist.empty:
                            row['away_pitcher_k_pct_recent'] = ap_hist.iloc[0]['k_pct_recent']
                            row['away_pitcher_walk_pct_recent'] = ap_hist.iloc[0]['walk_pct_recent']
                            row['away_pitcher_avg_fastball_velo'] = ap_hist.iloc[0]['avg_fastball_velo']

            ho_hist = team_off_sorted[team_off_sorted['team_id'] == tid_home]
            if not ho_hist.empty:
                row['home_offense_team_avg_exit_velo'] = ho_hist.iloc[0]['team_avg_exit_velo']
                row['home_offense_team_hard_hit_pct'] = ho_hist.iloc[0]['team_hard_hit_pct']
                row['home_offense_team_batter_k_pct'] = ho_hist.iloc[0]['team_batter_k_pct']

            ao_hist = team_off_sorted[team_off_sorted['team_id'] == tid_away]
            if not ao_hist.empty:
                row['away_offense_team_avg_exit_velo'] = ao_hist.iloc[0]['team_avg_exit_velo']
                row['away_offense_team_hard_hit_pct'] = ao_hist.iloc[0]['team_hard_hit_pct']
                row['away_offense_team_batter_k_pct'] = ao_hist.iloc[0]['team_batter_k_pct']
        except Exception as e:
            print(f"Error Live Features game_pk {row['game_pk']}: {e}")

        df_pred = pd.DataFrame([row])
        prob_xgb = modelo_xgb.predict_proba(df_pred[features]) [0][1] * 100
        prob_mc = simular_juego_mc_legal(
            row['home_pitcher_k_pct_recent'], row['home_pitcher_walk_pct_recent'], row['home_bp_burned'],
            row['away_pitcher_k_pct_recent'], row['away_pitcher_walk_pct_recent'], row['away_bp_burned'], 
            simulaciones=10000
        )
        
        pick_xgb = 'HOME' if prob_xgb > 50.0 else 'AWAY'
        pick_mc = 'HOME' if prob_mc > 50.0 else 'AWAY'
        
        partido_info = {
            "game_pk": int(row['game_pk']),
            "game_date": str(hoy_dt),
            "home_id": int(tid_home),
            "away_id": int(tid_away),
            "home_team_name": row.get('home_team_name', ''),
            "away_team_name": row.get('away_team_name', ''),
            "home_prob": float(round(prob_xgb, 2)),
            "away_prob": float(round(100 - prob_xgb, 2)),
            "status": "IGNORAR",
            "pick": None,
            "tentative_pick": None,
            "edge": 0.0,
            "odds": 0,
            "confianza": 0.0,
            "motivo": "Sin Edge Matemático",
            "model_stats": {
                "home_k_pct": float(row.get('home_pitcher_k_pct_recent', 0)),
                "away_k_pct": float(row.get('away_pitcher_k_pct_recent', 0)),
                "home_bp": int(row.get('home_bp_burned', 0)),
                "away_bp": int(row.get('away_bp_burned', 0)),
                "home_exit_velo": float(row.get('home_offense_team_avg_exit_velo', 0)),
                "away_exit_velo": float(row.get('away_offense_team_avg_exit_velo', 0))
            }
        }
        
        if pick_xgb == pick_mc:
            tid_apuesta = int(tid_home if pick_xgb == 'HOME' else tid_away)
            cuota = float(row['home_moneyline'] if pick_xgb == 'HOME' else row['away_moneyline'])
            confianza = float((prob_xgb + prob_mc) / 2)
            prob_v = float(obtener_probabilidad_vegas(cuota))
            edge = float(confianza - prob_v)
            
            partido_info["odds"] = int(cuota)
            partido_info["edge"] = float(round(edge, 2))
            partido_info["confianza"] = float(round(confianza, 2))
            partido_info["tentative_pick"] = pick_xgb
            
            # --- NUEVA CAPA: FILTRO DE LA TRAMPA DE ABRIL ---
            mes_actual = hoy_dt.month
            edge_minimo_requerido = 4.0 if mes_actual <= 5 else 0.0
            
            if edge > edge_minimo_requerido:
                # Auditar Escudos
                hist_YTD = memoria_YTD.get(tid_apuesta, [])
                racha_YTD = 0
                if len(hist_YTD) > 0:
                    for res in reversed(hist_YTD):
                        if res == 0: racha_YTD -= 1
                        elif res == 1: racha_YTD += 1
                        else: break
                ytd_autoriza = (racha_YTD > -3)
                
                hist_L15 = memoria_L15.get(tid_apuesta, [])
                wr_L15 = sum(hist_L15) / len(hist_L15) if len(hist_L15) > 1 else 0.5
                l15_autoriza = (len(hist_L15) < 5 or wr_L15 >= 0.350)
                
                if ytd_autoriza and l15_autoriza:
                    # KELLY CRITERION: Quarter-Kelly con cap 5%
                    BANKROLL = 5000.0  # Capital base de referencia
                    kelly_stake = calcular_kelly_stake(confianza, cuota, BANKROLL)
                    
                    partido_info["status"] = "APROBADO"
                    partido_info["pick"] = pick_xgb
                    partido_info["kelly_stake"] = float(kelly_stake)
                    partido_info["motivo"] = "Consenso de 5 Capas"
                    print(f"[APROBADO] DISPARAR: {pick_xgb} (ID: {tid_apuesta}) | Cuota: {cuota} | Edge: +{edge:.2f}% | Confianza: {confianza:.2f}% | Kelly: ${kelly_stake:.2f}")
                else:
                    motivo = "Racha YTD (-3)" if not ytd_autoriza else "WinRate L15 (<0.350)"
                    partido_info["motivo"] = f"Bloqueado por Escudo: {motivo}"
                    print(f"[ESCUDO] BLOQUEADO por Escudo ({motivo}): {pick_xgb} (ID: {tid_apuesta}) | Edge: +{edge:.2f}%")
            else:
                partido_info["motivo"] = f"Trampa de Abril (< {edge_minimo_requerido}%)"
                print(f"[IGNORAR]: Edge de {edge:.2f}% no supera el mínimo de {edge_minimo_requerido}% exigido para el mes {mes_actual}.")
        else:
            partido_info["motivo"] = "Sin Consenso XGB/MC"
            print(f"[IGNORAR] (Sin Consenso): XGB={pick_xgb}, MC={pick_mc}")
            
        reporte_diario.append(partido_info)
        
        # INYECCIÓN: Si el pick fue Aprobado y NO ESTÁ previamente en el Ledger de Supabase, lo formamos
        if partido_info["status"] == "APROBADO":
            if int(row['game_pk']) not in existing_ledger_pks:
                ledger_records.append({
                    "game_pk": int(row['game_pk']),
                    "game_date": str(hoy_dt),
                    "pick_team": partido_info["tentative_pick"],
                    "market_type": "h2h",
                    "odds": int(partido_info["odds"]),
                    "stake": float(partido_info.get("kelly_stake", 100.0)),
                    "status": "PENDING",
                    "profit_loss": 0.00
                })
        

    os.makedirs('frontend/data', exist_ok=True)
    # Sanitizar tipos numpy (float32, int64) para JSON
    import numpy as np
    def sanitize(obj):
        if isinstance(obj, dict): return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list): return [sanitize(v) for v in obj]
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        return obj
    with open('frontend/data/picks.json', 'w') as f:
        json.dump(sanitize(reporte_diario), f, indent=4)
        
    try:
        supabase.table('daily_picks').upsert(reporte_diario).execute()
        print("\n -> [NUBE] Predicciones sincronizadas en la tabla 'daily_picks' de Supabase.")
    except Exception as e:
        print(f"\n -> [WARNING] No se pudo guardar en Supabase. ¿Ya creaste la tabla 'daily_picks'?: {e}")
        
    try:
        if ledger_records:
            supabase.table('sindicato_ledger').insert(ledger_records).execute()
            print(f" -> [LEDGER] {len(ledger_records)} picks incondicionales INMOVILIZADOS con estatus PENDING.")
            
        # Al extraer todo, construimos y publicamos el tracker JSON para el frontend
        res_ledger_all = supabase.table('sindicato_ledger').select('*').execute()
        if res_ledger_all.data:
            # Enriquecer con nombres de equipos cruzando contra mlb_games_history
            ledger_pks = list(set(r['game_pk'] for r in res_ledger_all.data))
            res_games_ledger = supabase.table('mlb_games_history').select('game_pk, home_team_name, away_team_name').in_('game_pk', ledger_pks).execute()
            games_map = {g['game_pk']: g for g in res_games_ledger.data} if res_games_ledger.data else {}
            
            for record in res_ledger_all.data:
                g = games_map.get(record['game_pk'], {})
                record['home_team_name'] = g.get('home_team_name', '')
                record['away_team_name'] = g.get('away_team_name', '')
            
            with open('frontend/data/ledger.json', 'w') as f:
                json.dump(res_ledger_all.data, f, indent=4)
            print(" -> [LEDGER] frontend/data/ledger.json exportado correctamente con historial íntegro.")
    except Exception as e:
        print(f"\n -> [WARNING] Carga de Ledger fallida. Verifica credenciales y tabla 'sindicato_ledger'. Error: {e}")
        
    print("\n -> [WEB] Pipeline Cerrado. Proyecciones exportadas a frontend/data/picks.json exitosamente.")

if __name__ == "__main__":
    run_sindicato()
