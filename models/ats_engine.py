"""
MOTOR ATS V12.1 - Against The Spread (Runline)
Modelo XGBoost especializado en coberturas de spread/runline.
Diseñado para integrarse al pipeline Sindicato como segunda capa de picks.
"""
import os
import pandas as pd
import numpy as np
import xgboost as xgb
import warnings
from datetime import date
warnings.filterwarnings('ignore')

mlb_teams = {
    108: 'Angels', 109: 'Diamondbacks', 110: 'Orioles', 111: 'Red Sox',
    112: 'Cubs', 113: 'Reds', 114: 'Guardians', 115: 'Rockies',
    116: 'Tigers', 117: 'Astros', 118: 'Royals', 119: 'Dodgers',
    120: 'Nationals', 121: 'Mets', 133: 'Athletics', 134: 'Pirates',
    135: 'Padres', 136: 'Mariners', 137: 'Giants', 138: 'Cardinals',
    139: 'Rays', 140: 'Rangers', 141: 'Blue Jays', 142: 'Twins',
    143: 'Phillies', 144: 'Braves', 145: 'White Sox', 146: 'Marlins',
    147: 'Yankees', 158: 'Brewers'
}

def odds_to_decimal(odds):
    if odds < 0: return 1 + (100 / abs(odds))
    else: return 1 + (odds / 100)

def obtener_prob_vegas(cuota):
    if pd.isna(cuota) or cuota == 0: return 50.0
    return (abs(cuota) / (abs(cuota) + 100)) * 100 if cuota < 0 else (100 / (cuota + 100)) * 100

def calc_tier(win_pct, games_played):
    if games_played < 10: return 3
    if win_pct >= 0.58: return 1
    elif win_pct >= 0.53: return 2
    elif win_pct >= 0.48: return 3
    elif win_pct >= 0.43: return 4
    else: return 5

def calcular_kelly_ats(confianza_pct, cuota_americana, bankroll, fraction=0.25, cap=0.05):
    """Quarter-Kelly con techo del 5% del bankroll para ATS."""
    p = confianza_pct / 100.0
    dec = odds_to_decimal(cuota_americana)
    kelly_full = (p * (dec - 1) - (1 - p)) / (dec - 1)
    kelly_full = max(0, kelly_full)
    kelly_adj = kelly_full * fraction
    stake = round(bankroll * kelly_adj, 2)
    stake = max(10, min(stake, bankroll * cap))
    return stake


def run_ats_engine(supabase, extraer_tabla_fn, hoy_dt, bankroll=5000.0):
    """
    Ejecuta el motor ATS V12.1 y devuelve:
    - ats_by_game_pk: dict {game_pk: {ats_data}} para merge con picks.json
    - ats_ledger_records: list de records para insertar en sindicato_ledger
    """
    print("\n[ATS V12.1] Iniciando motor Against The Spread...")

    df_games = extraer_tabla_fn('mlb_games_history').dropna(subset=['home_score', 'away_score'], how='all')
    df_pitchers = extraer_tabla_fn('pitcher_pregame_features')
    df_logs_pitchers = extraer_tabla_fn('pitcher_game_logs')
    df_batters = extraer_tabla_fn('batter_pregame_features')
    df_logs_batters = extraer_tabla_fn('batter_game_logs')
    df_odds = extraer_tabla_fn('historical_odds')
    df_bullpen = extraer_tabla_fn('bullpen_availability')

    # Filtrar solo spreads
    df_odds = df_odds[(df_odds['market_key'] == 'spreads') & ((df_odds['price'] <= -100) | (df_odds['price'] >= 100))]

    if df_odds.empty:
        print("[ATS V12.1] No hay datos de spreads disponibles. Saltando.")
        return {}, []

    df_bullpen['game_date'] = pd.to_datetime(df_bullpen['game_date']).dt.date
    bullpen_agg = df_bullpen.groupby(['game_date', 'team_id'])['is_burned'].sum().reset_index()

    df_batters_full = df_batters.merge(df_logs_batters[['game_pk', 'batter_id', 'team_id']], on=['game_pk', 'batter_id'], how='inner')
    team_offense = df_batters_full.groupby(['game_pk', 'team_id']).agg(team_batter_k_pct=('k_pct_recent', 'mean')).reset_index()

    df_games['game_date_dt'] = pd.to_datetime(df_games['game_date']).dt.date

    home_odds = df_odds[df_odds['outcome_name'].str.lower() == 'home'][['game_pk', 'price', 'point']].rename(
        columns={'price': 'home_rl_price', 'point': 'home_rl_point'}).drop_duplicates(subset=['game_pk'])
    away_odds = df_odds[df_odds['outcome_name'].str.lower() == 'away'][['game_pk', 'price', 'point']].rename(
        columns={'price': 'away_rl_price', 'point': 'away_rl_point'}).drop_duplicates(subset=['game_pk'])

    pitcher_stats = df_logs_pitchers[df_logs_pitchers['is_starter'] == True][['game_pk', 'pitcher_id', 'team_id']].merge(
        df_pitchers, on=['game_pk', 'pitcher_id'], how='left')

    matriz = df_games[['game_pk', 'game_date', 'game_date_dt', 'home_team_id', 'away_team_id', 'home_score', 'away_score']].copy()
    matriz = matriz.merge(home_odds, on='game_pk', how='left').merge(away_odds, on='game_pk', how='left')
    matriz = matriz.dropna(subset=['home_rl_price', 'home_rl_point']).sort_values('game_date')

    matriz = matriz.merge(bullpen_agg.rename(columns={'team_id': 'home_team_id', 'is_burned': 'home_bp_burned'}),
                          left_on=['game_date_dt', 'home_team_id'], right_on=['game_date', 'home_team_id'], how='left')
    if 'game_date_y' in matriz.columns:
        matriz = matriz.drop(columns=['game_date_y']).rename(columns={'game_date_x': 'game_date'})

    matriz = matriz.merge(bullpen_agg.rename(columns={'team_id': 'away_team_id', 'is_burned': 'away_bp_burned'}),
                          left_on=['game_date_dt', 'away_team_id'], right_on=['game_date', 'away_team_id'], how='left')
    if 'game_date_y' in matriz.columns:
        matriz = matriz.drop(columns=['game_date_y']).rename(columns={'game_date_x': 'game_date'})

    matriz = matriz.merge(pitcher_stats.add_prefix('home_pitcher_'), left_on=['game_pk', 'home_team_id'],
                          right_on=['home_pitcher_game_pk', 'home_pitcher_team_id'], how='left')
    matriz = matriz.merge(pitcher_stats.add_prefix('away_pitcher_'), left_on=['game_pk', 'away_team_id'],
                          right_on=['away_pitcher_game_pk', 'away_pitcher_team_id'], how='left')
    matriz = matriz.merge(team_offense.add_prefix('home_offense_'), left_on=['game_pk', 'home_team_id'],
                          right_on=['home_offense_game_pk', 'home_offense_team_id'], how='left')
    matriz = matriz.merge(team_offense.add_prefix('away_offense_'), left_on=['game_pk', 'away_team_id'],
                          right_on=['away_offense_game_pk', 'away_offense_team_id'], how='left')

    # Target: home covers spread
    matriz['target_home_covers'] = np.where(
        pd.notna(matriz['home_score']),
        ((matriz['home_score'] - matriz['away_score']) + matriz['home_rl_point'] > 0).astype(float),
        np.nan
    )

    # --- Rachas ATS ---
    historial_coberturas = {team: [] for team in mlb_teams.values()}
    racha_actual = {team: 0 for team in mlb_teams.values()}

    feat_op3 = [
        'h_ats_ytd', 'h_ats_l10', 'a_ats_ytd', 'a_ats_l10', 'h_ats_tier', 'a_ats_tier',
        'home_pitcher_k_pct_recent', 'away_pitcher_k_pct_recent', 'home_bp_burned', 'away_bp_burned',
        'home_offense_team_batter_k_pct', 'away_offense_team_batter_k_pct'
    ]

    features_ml = []
    for _, row in matriz.iterrows():
        ht = mlb_teams.get(row['home_team_id'])
        at = mlb_teams.get(row['away_team_id'])
        if not ht or not at: continue

        h_h = historial_coberturas[ht]
        a_h = historial_coberturas[at]
        h_ytd = sum(h_h) / len(h_h) if h_h else 0.5
        a_ytd = sum(a_h) / len(a_h) if a_h else 0.5

        features_ml.append({
            'game_pk': row['game_pk'],
            'h_ats_ytd': h_ytd, 'h_ats_l10': sum(h_h[-10:]) / len(h_h[-10:]) if h_h else 0.5,
            'h_ats_tier': calc_tier(h_ytd, len(h_h)), 'h_racha_actual': racha_actual[ht],
            'a_ats_ytd': a_ytd, 'a_ats_l10': sum(a_h[-10:]) / len(a_h[-10:]) if a_h else 0.5,
            'a_ats_tier': calc_tier(a_ytd, len(a_h)), 'a_racha_actual': racha_actual[at]
        })

        if pd.notna(row['target_home_covers']):
            h_won = row['target_home_covers'] == 1.0
            historial_coberturas[ht].append(1 if h_won else 0)
            historial_coberturas[at].append(0 if h_won else 1)
            for team, won in [(ht, h_won), (at, not h_won)]:
                if won: racha_actual[team] = 1 if racha_actual[team] < 0 else racha_actual[team] + 1
                else: racha_actual[team] = -1 if racha_actual[team] > 0 else racha_actual[team] - 1

    matriz = matriz.merge(pd.DataFrame(features_ml), on='game_pk').fillna(0)

    train_data = matriz[(matriz['game_date_dt'] < hoy_dt) & pd.notna(matriz['target_home_covers'])]
    juegos_hoy = matriz[matriz['game_date_dt'] == hoy_dt]

    if len(train_data) < 50:
        print("[ATS V12.1] Datos insuficientes para entrenar. Saltando.")
        return {}, []

    mod = xgb.XGBClassifier(n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42)
    mod.fit(train_data[feat_op3], train_data['target_home_covers'])
    print(f"[ATS V12.1] Modelo entrenado con {len(train_data)} registros historicos.")

    if juegos_hoy.empty:
        print("[ATS V12.1] No hay juegos de hoy con spreads. Saltando.")
        return {}, []

    ats_by_game_pk = {}
    ats_ledger_records = []

    # Obtener existing ledger PKs para ATS
    try:
        res_existing = supabase.table('sindicato_ledger').select('game_pk').eq('market_type', 'spreads').execute()
        existing_ats_pks = set(r['game_pk'] for r in res_existing.data) if res_existing.data else set()
    except:
        existing_ats_pks = set()

    for _, row in juegos_hoy.iterrows():
        gpk = int(row['game_pk'])
        prob_h = mod.predict_proba(pd.DataFrame([row.to_dict()])[feat_op3])[0][1] * 100
        prob_a = 100.0 - prob_h

        c_h, c_a = float(row['home_rl_price']), float(row['away_rl_price'])
        pt_h = float(row['home_rl_point'])
        pt_a = float(row['away_rl_point']) if pd.notna(row.get('away_rl_point')) else -pt_h
        v_h, v_a = obtener_prob_vegas(c_h), obtener_prob_vegas(c_a)
        edge_h, edge_a = prob_h - v_h, prob_a - v_a

        ats_entry = {
            'ats_prob_home': round(prob_h, 1),
            'ats_prob_away': round(prob_a, 1),
            'ats_home_odds': int(c_h),
            'ats_away_odds': int(c_a),
            'ats_home_point': pt_h,
            'ats_away_point': pt_a,
            'ats_status': 'IGNORAR',
            'ats_pick': None,
            'ats_edge': 0.0,
            'ats_confianza': 0.0,
            'ats_kelly_stake': 0.0,
            'ats_motivo': ''
        }

        apuesta = None
        # Motor V12.1: Tier 3 normal + Buy the Dip (T4/T5 en racha perdedora)
        if edge_h > 1.5 and ((row['h_ats_tier'] == 3 and row['h_racha_actual'] > -2) or
                             (row['h_ats_tier'] in [4, 5] and row['h_racha_actual'] <= -2)):
            apuesta = 'HOME'
        elif edge_a > 1.5 and ((row['a_ats_tier'] == 3 and row['a_racha_actual'] > -2) or
                               (row['a_ats_tier'] in [4, 5] and row['a_racha_actual'] <= -2)):
            apuesta = 'AWAY'

        if apuesta:
            odds = c_h if apuesta == 'HOME' else c_a
            conf = prob_h if apuesta == 'HOME' else prob_a
            edge = edge_h if apuesta == 'HOME' else edge_a
            point = pt_h if apuesta == 'HOME' else pt_a
            tier = int(row['h_ats_tier'] if apuesta == 'HOME' else row['a_ats_tier'])
            racha = int(row['h_racha_actual'] if apuesta == 'HOME' else row['a_racha_actual'])

            kelly_stake = calcular_kelly_ats(conf, odds, bankroll)

            ats_entry['ats_status'] = 'APROBADO'
            ats_entry['ats_pick'] = apuesta
            ats_entry['ats_edge'] = round(edge, 2)
            ats_entry['ats_confianza'] = round(conf, 2)
            ats_entry['ats_kelly_stake'] = float(kelly_stake)
            ats_entry['ats_motivo'] = f"Tier {tier} | Racha: {racha}"

            team_name = mlb_teams.get(row['home_team_id'], '?') if apuesta == 'HOME' else mlb_teams.get(row['away_team_id'], '?')
            estrategia = "Motor Tier 3" if tier == 3 else "Buy the Dip (T4/T5)"
            print(f"[ATS APROBADO] {team_name} {point:+.1f} | Odds: {int(odds)} | Edge: +{edge:.2f}% | Kelly: ${kelly_stake:.2f} | {estrategia}")

            # Ledger injection
            if gpk not in existing_ats_pks:
                ats_ledger_records.append({
                    "game_pk": gpk,
                    "game_date": str(hoy_dt),
                    "pick_team": apuesta,
                    "market_type": "spreads",
                    "odds": int(odds),
                    "stake": float(kelly_stake),
                    "status": "PENDING",
                    "profit_loss": 0.00
                })

        ats_by_game_pk[gpk] = ats_entry

    approved_count = sum(1 for v in ats_by_game_pk.values() if v['ats_status'] == 'APROBADO')
    print(f"[ATS V12.1] Analisis completo: {len(ats_by_game_pk)} juegos, {approved_count} picks ATS aprobados.")

    return ats_by_game_pk, ats_ledger_records
