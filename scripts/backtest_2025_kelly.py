"""
BACKTEST COMPLETO 2025 - Simulación Kelly Criterion (GOOGLE COLAB)
Replica exactamente la lógica del pipeline Sindicato (XGBoost + Monte Carlo + Escudos)
iterando día a día por toda la temporada 2025 para validar la estrategia de sizing.
"""
import os
import pandas as pd
import numpy as np
import random
import xgboost as xgb
import warnings
warnings.filterwarnings('ignore')
from supabase import create_client, Client
from google.colab import userdata

# --- CONFIGURACIÓN ---
supabase: Client = create_client(userdata.get('SUPABASE_URL'), userdata.get('SUPABASE_KEY'))

# --- MONTE CARLO INLINED ---
def simular_juego_mc_legal(k_home, bb_home, bp_home_burned, k_away, bb_away, bp_away_burned, simulaciones=1000):
    def simular_turno(inning, starter_k, starter_bb, bullpen_quemado):
        if inning >= 6:
            k_prob, bb_prob = (0.18, 0.12) if bullpen_quemado >= 3 else (0.24, 0.08)
        else:
            k_prob, bb_prob = starter_k, starter_bb
            if inning >= 4:
                k_prob -= 0.02
                bb_prob += 0.01
        k_prob = k_prob if k_prob > 0 else 0.22
        bb_prob = bb_prob if bb_prob > 0 else 0.08
        dado = random.random()
        if dado < k_prob:
            return 'OUT'
        elif dado < (k_prob + bb_prob):
            return 'ON_BASE'
        else:
            return 'ON_BASE' if random.random() < 0.30 else 'OUT'

    victorias_home = 0
    for _ in range(simulaciones):
        s_home, s_away = 0, 0
        for inn in range(1, 10):
            outs, bases = 0, 0
            while outs < 3:
                if simular_turno(inn, k_home, bb_home, bp_home_burned) == 'OUT':
                    outs += 1
                else:
                    bases += 1
                    s_away += (1 if bases >= 3 else 0)
                    bases = min(bases, 2)
            outs, bases = 0, 0
            while outs < 3:
                if simular_turno(inn, k_away, bb_away, bp_away_burned) == 'OUT':
                    outs += 1
                else:
                    bases += 1
                    s_home += (1 if bases >= 3 else 0)
                    bases = min(bases, 2)
        if s_home > s_away:
            victorias_home += 1
        elif s_home == s_away:
            victorias_home += 0.5
    return (victorias_home / simulaciones) * 100

# --- HELPERS ---
def extraer_tabla(tabla):
    datos = []
    offset, limit = 0, 1000
    while True:
        try:
            res = supabase.table(tabla).select('*').range(offset, offset + limit - 1).execute()
            if not res.data: break
            datos.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
        except Exception as e:
            print(f"Error extrayendo {tabla}: {e}")
            break
    return pd.DataFrame(datos)

def obtener_probabilidad_vegas(cuota):
    if pd.isna(cuota) or cuota == 0: return 50.0
    return (abs(cuota) / (abs(cuota) + 100)) * 100 if cuota < 0 else (100 / (cuota + 100)) * 100

def odds_to_decimal(odds):
    if odds < 0:
        return 1 + (100 / abs(odds))
    else:
        return 1 + (odds / 100)

# ============================================================
#  FASE 1: CARGAR TODA LA DATA
# ============================================================
print(f"\n{'='*60}")
print(f" BACKTEST TEMPORADA 2025 - VALIDACIÓN KELLY CRITERION")
print(f"{'='*60}")

print("\n[FASE 1] Cargando matrices de Supabase...")
df_games = extraer_tabla('mlb_games_history').dropna(subset=['home_score', 'away_score'])
df_travel = extraer_tabla('team_travel_logs')
df_pitchers = extraer_tabla('pitcher_pregame_features')
df_logs_pitchers = extraer_tabla('pitcher_game_logs')
df_batters = extraer_tabla('batter_pregame_features')
df_logs_batters = extraer_tabla('batter_game_logs')
df_odds = extraer_tabla('historical_odds')
df_bullpen = extraer_tabla('bullpen_availability')

print(f"   Games: {len(df_games)} | Odds: {len(df_odds)} | Pitchers: {len(df_pitchers)}")

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

matriz = df_games[['game_pk', 'game_date', 'game_date_dt', 'target_home_win', 'home_team_id', 'away_team_id', 'home_score', 'away_score']].copy()
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

# ============================================================
#  FASE 2: ITERAR DIA A DIA POR TODA LA TEMPORADA 2025
# ============================================================
print("\n[FASE 2] Iterando temporada 2025 dia a dia...")

fechas_2025 = sorted(matriz[matriz['game_date'].str.startswith('2025')]['game_date_dt'].unique())
fechas_2025 = [f for f in fechas_2025 if f.month >= 4]

all_picks = []

for i, current_date in enumerate(fechas_2025):
    train_data = matriz[matriz['game_date_dt'] < current_date].copy()
    juegos_dia = matriz[(matriz['game_date_dt'] == current_date) & (matriz['home_moneyline'] != 0)].copy()

    if juegos_dia.empty or len(train_data) < 50:
        continue

    try:
        modelo_xgb = xgb.XGBClassifier(subsample=0.8, n_estimators=200, max_depth=5, learning_rate=0.01, colsample_bytree=0.8, random_state=42)
        modelo_xgb.fit(train_data[features], train_data['target_home_win'])
    except Exception:
        continue

    # Computar Escudos YTD y L15
    anio = current_date.year
    datos_anio = train_data[pd.to_datetime(train_data['game_date']).dt.year == anio].sort_values('game_date')
    memoria_YTD, memoria_L15 = {}, {}
    for _, row in datos_anio.iterrows():
        th, ta = row['home_team_id'], row['away_team_id']
        if th not in memoria_YTD: memoria_YTD[th] = []; memoria_L15[th] = []
        if ta not in memoria_YTD: memoria_YTD[ta] = []; memoria_L15[ta] = []
        rh = 1 if row['target_home_win'] == 1 else 0
        ra = 1 - rh
        memoria_YTD[th].append(rh); memoria_YTD[ta].append(ra)
        memoria_L15[th].append(rh); memoria_L15[ta].append(ra)
        if len(memoria_L15[th]) > 15: memoria_L15[th].pop(0)
        if len(memoria_L15[ta]) > 15: memoria_L15[ta].pop(0)

    for _, row in juegos_dia.iterrows():
        tid_home, tid_away = int(row['home_team_id']), int(row['away_team_id'])

        df_pred = pd.DataFrame([row.to_dict()])
        prob_xgb = modelo_xgb.predict_proba(df_pred[features])[0][1] * 100
        prob_mc = simular_juego_mc_legal(
            row['home_pitcher_k_pct_recent'], row['home_pitcher_walk_pct_recent'], row['home_bp_burned'],
            row['away_pitcher_k_pct_recent'], row['away_pitcher_walk_pct_recent'], row['away_bp_burned'],
            simulaciones=1000
        )

        pick_xgb = 'HOME' if prob_xgb > 50.0 else 'AWAY'
        pick_mc = 'HOME' if prob_mc > 50.0 else 'AWAY'

        if pick_xgb != pick_mc:
            continue

        tid_apuesta = int(tid_home if pick_xgb == 'HOME' else tid_away)
        cuota = float(row['home_moneyline'] if pick_xgb == 'HOME' else row['away_moneyline'])
        confianza = float((prob_xgb + prob_mc) / 2)
        prob_v = float(obtener_probabilidad_vegas(cuota))
        edge = float(confianza - prob_v)

        mes = current_date.month
        edge_min = 4.0 if mes <= 5 else 0.0

        if edge <= edge_min:
            continue

        # Escudos
        hist_YTD = memoria_YTD.get(tid_apuesta, [])
        racha_YTD = 0
        if hist_YTD:
            for res in reversed(hist_YTD):
                if res == 0: racha_YTD -= 1
                elif res == 1: racha_YTD += 1
                else: break
        if racha_YTD <= -3:
            continue

        hist_L15 = memoria_L15.get(tid_apuesta, [])
        wr_L15 = sum(hist_L15) / len(hist_L15) if len(hist_L15) > 1 else 0.5
        if len(hist_L15) >= 5 and wr_L15 < 0.350:
            continue

        # PICK APROBADO - Determinar resultado real
        home_won = row['home_score'] > row['away_score']
        picked_home = (pick_xgb == 'HOME')
        ganador = (home_won and picked_home) or (not home_won and not picked_home)

        all_picks.append({
            'date': str(current_date),
            'month': mes,
            'pick': pick_xgb,
            'odds': int(cuota),
            'edge': round(edge, 2),
            'confianza': round(confianza, 2),
            'won': ganador
        })

    if (i + 1) % 30 == 0:
        print(f"   Procesados {i+1}/{len(fechas_2025)} dias | Picks acumulados: {len(all_picks)}")

# ============================================================
#  FASE 3: RESULTADOS
# ============================================================
print(f"\n[FASE 3] RESULTADOS DE LA TEMPORADA 2025")
print(f"   Total de Picks Aprobados: {len(all_picks)}")
if not all_picks:
    print("   SIN PICKS - Verifica que tu base de datos tenga datos de 2025.")
else:
    wins = sum(1 for p in all_picks if p['won'])
    losses = len(all_picks) - wins
    print(f"   WON: {wins} | LOST: {losses} | WinRate: {wins/len(all_picks)*100:.1f}%")

    print("\n   --- Desglose Mensual ---")
    by_month = {}
    for p in all_picks:
        m = p['month']
        if m not in by_month:
            by_month[m] = {'w': 0, 'l': 0}
        if p['won']:
            by_month[m]['w'] += 1
        else:
            by_month[m]['l'] += 1

    month_names = {4:'ABR', 5:'MAY', 6:'JUN', 7:'JUL', 8:'AGO', 9:'SEP', 10:'OCT'}
    for m in sorted(by_month.keys()):
        d = by_month[m]
        total = d['w'] + d['l']
        wr = d['w'] / total * 100 if total else 0
        print(f"   {month_names.get(m, m)}: {d['w']}W-{d['l']}L ({wr:.0f}%) | {total} picks")

    # ============================================================
    #  FASE 4: SIMULACIÓN KELLY CRITERION
    # ============================================================
    print(f"\n{'='*60}")
    print(f" SIMULACIÓN KELLY CRITERION - TEMPORADA 2025 COMPLETA")
    print(f"{'='*60}")

    BANKROLL = 5000.0

    for label, fraction in [('FLAT $100', 0), ('QUARTER-KELLY (25%)', 0.25), ('HALF-KELLY (50%)', 0.50), ('FULL KELLY (100%)', 1.0)]:
        bank = BANKROLL
        max_bank = bank
        min_bank = bank
        stakes = []

        for p in all_picks:
            odds = p['odds']
            dec = odds_to_decimal(odds)

            if fraction == 0:
                stake = 100.0
            else:
                p_modelo = p['confianza'] / 100.0
                kelly_full = (p_modelo * (dec - 1) - (1 - p_modelo)) / (dec - 1)
                kelly_full = max(0, kelly_full)
                kelly_adj = kelly_full * fraction
                stake = round(bank * kelly_adj, 2)
                stake = max(10, min(stake, bank * 0.15))

            if p['won']:
                profit = stake * (dec - 1)
            else:
                profit = -stake

            stakes.append(stake)
            bank += profit
            max_bank = max(max_bank, bank)
            min_bank = min(min_bank, bank)

        roi = ((bank - BANKROLL) / BANKROLL) * 100
        max_dd = ((max_bank - min_bank) / max_bank) * 100
        avg_stake = sum(stakes) / len(stakes) if stakes else 0

        print(f"\n=== {label} ===")
        print(f"  Bankroll Final:    ${bank:,.2f}")
        print(f"  Profit Neto:       ${bank - BANKROLL:,.2f}")
        print(f"  ROI:               {roi:.1f}%")
        print(f"  Max Drawdown:      {max_dd:.1f}%")
        print(f"  Piso mas bajo:     ${min_bank:,.2f}")
        print(f"  Apuesta Promedio:  ${avg_stake:,.2f}")
