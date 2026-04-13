import os
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client, Client
import xgboost as xgb
from models.montecarlo import simular_juego_mc_legal
from data.liquidator import SindicatoLiquidator
from main import extraer_tabla, obtener_probabilidad_vegas

def run_backfill():
    print("\n" + "="*50)
    print(" INICIANDO BACKFILL DE LA TEMPORADA 2026")
    print("="*50)

    load_dotenv()
    supabase: Client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
    pd.set_option('future.no_silent_downcasting', True)

    print("\n[FASE 1] Ensamblando Matriz Cuántica Base...")
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

    print("\n[FASE 2] Iterando Temporada 2026 hacia adelante...")
    
    fechas_2026 = sorted(matriz[matriz['game_date'].str.startswith('2026')]['game_date_dt'].unique())
    today_dt = datetime.now(timezone.utc).date()
    
    ledger_records = []
    
    for current_date in fechas_2026:
        if current_date >= today_dt:
            break
            
        print(f"\n[BACKTEST] Entrenando y Prediciendo para Date: {current_date}")
        train_data = matriz[matriz['game_date_dt'] < current_date].copy()
        juegos_dia = matriz[(matriz['game_date_dt'] == current_date) & (matriz['home_moneyline'] != 0)].copy()
        
        if juegos_dia.empty or train_data.empty:
            continue
            
        # 1. Fit local del pasado
        modelo_xgb = xgb.XGBClassifier(subsample=0.8, n_estimators=200, max_depth=5, learning_rate=0.01, colsample_bytree=0.8, random_state=42)
        try:
            modelo_xgb.fit(train_data[features], train_data['target_home_win'])
        except Exception as e:
            print(f"Skipping training (Not enough classes/data): {e}")
            continue
            
        # 2. Computar Historial de Escudos (YTD/L15) exactamente hasta esa fecha
        anio_actual = current_date.year
        datos_anio_actual = train_data[pd.to_datetime(train_data['game_date']).dt.year == anio_actual].sort_values(by='game_date')
        memoria_YTD, memoria_L15 = {}, {}
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
            
        # 3. Predicciones
        for _, row_orig in juegos_dia.iterrows():
            row = row_orig.to_dict()
            tid_home, tid_away = int(row['home_team_id']), int(row['away_team_id'])
            
            df_pred = pd.DataFrame([row])
            prob_xgb = modelo_xgb.predict_proba(df_pred[features])[0][1] * 100
            prob_mc = simular_juego_mc_legal(
                row['home_pitcher_k_pct_recent'], row['home_pitcher_walk_pct_recent'], row['home_bp_burned'],
                row['away_pitcher_k_pct_recent'], row['away_pitcher_walk_pct_recent'], row['away_bp_burned'], 
                simulaciones=2000 # bajamos sim para speed
            )
            
            pick_xgb = 'HOME' if prob_xgb > 50.0 else 'AWAY'
            pick_mc = 'HOME' if prob_mc > 50.0 else 'AWAY'
            
            if pick_xgb == pick_mc:
                tid_apuesta = int(tid_home if pick_xgb == 'HOME' else tid_away)
                cuota = float(row['home_moneyline'] if pick_xgb == 'HOME' else row['away_moneyline'])
                confianza = float((prob_xgb + prob_mc) / 2)
                prob_v = float(obtener_probabilidad_vegas(cuota))
                edge = float(confianza - prob_v)
                
                mes_actual = current_date.month
                edge_minimo_requerido = 4.0 if mes_actual <= 5 else 0.0
                
                if edge > edge_minimo_requerido:
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
                        ledger_records.append({
                            "game_pk": int(row['game_pk']),
                            "game_date": str(current_date),
                            "pick_team": pick_xgb,
                            "market_type": "h2h",
                            "odds": int(cuota),
                            "status": "PENDING",
                            "profit_loss": 0.00
                        })
                        print(f" -> Back-Picked: {pick_xgb} {current_date} (Edge: {edge:.2f}%)")
                        
    if ledger_records:
        try:
            print("\n[FASE 3] Enviando Backfill a Sindicato Ledger...")
            supabase.table('sindicato_ledger').upsert(ledger_records, on_conflict='game_pk,market_type').execute()
        except Exception as e:
            print(f"Falla insertando backfill: {e}")
            
    print("\n[FASE 4] Disparando Liquidador sobre el Ledger congelado...")
    SindicatoLiquidator().liquidar_juegos_pendientes()
    
    # Refrescar y exportar el JSON actualizado
    import json
    res_ledger_all = supabase.table('sindicato_ledger').select('*').execute()
    if res_ledger_all.data:
        with open('frontend/data/ledger.json', 'w') as f:
            json.dump(res_ledger_all.data, f, indent=4)
        print("\n -> [HECHO] frontend/data/ledger.json reconstruido correctamente con toda la temporada.")
    print("BACKFILL EXITOSO!")

if __name__ == '__main__':
    run_backfill()
