import json

ledger = json.load(open('frontend/data/ledger.json'))

resueltos = [r for r in ledger if r['status'] in ['WON', 'LOST']]
print(f"Picks resueltos: {len(resueltos)}")
won = sum(1 for r in resueltos if r['status'] == 'WON')
lost = sum(1 for r in resueltos if r['status'] == 'LOST')
print(f"WON: {won} | LOST: {lost} | WinRate: {won/(won+lost)*100:.1f}%\n")

def prob_vegas(odds):
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    else:
        return 100 / (odds + 100)

def odds_to_decimal(odds):
    if odds < 0:
        return 1 + (100 / abs(odds))
    else:
        return 1 + (odds / 100)

BANKROLL_INICIAL = 5000.0

for label, fraction in [('FLAT $100', 0), ('QUARTER-KELLY (25%)', 0.25), ('HALF-KELLY (50%)', 0.50), ('FULL KELLY (100%)', 1.0)]:
    bank = BANKROLL_INICIAL
    max_bank = bank
    min_bank = bank
    apuestas = []
    
    for r in sorted(resueltos, key=lambda x: x['game_date']):
        odds = r['odds']
        pv = prob_vegas(odds)
        dec = odds_to_decimal(odds)
        
        # Reconstruir edge conservador del modelo (~60% confianza promedio)
        edge_estimado = max(0.05, 0.60 - pv)
        
        if fraction == 0:
            stake = 100.0
        else:
            p_modelo = pv + edge_estimado
            kelly_full = (p_modelo * (dec - 1) - (1 - p_modelo)) / (dec - 1)
            kelly_full = max(0, kelly_full)
            kelly_adj = kelly_full * fraction
            stake = round(bank * kelly_adj, 2)
            stake = max(10, min(stake, bank * 0.15))  # piso $10, techo 15% del bank
        
        if r['status'] == 'WON':
            profit = stake * (dec - 1)
        else:
            profit = -stake
        
        apuestas.append(stake)
        bank += profit
        max_bank = max(max_bank, bank)
        min_bank = min(min_bank, bank)
    
    roi = ((bank - BANKROLL_INICIAL) / BANKROLL_INICIAL) * 100
    max_dd = ((max_bank - min_bank) / max_bank) * 100
    avg_stake = sum(apuestas) / len(apuestas) if apuestas else 0
    
    print(f"=== {label} ===")
    print(f"  Bankroll Final:    ${bank:,.2f}")
    print(f"  Profit Neto:       ${bank - BANKROLL_INICIAL:,.2f}")
    print(f"  ROI:               {roi:.1f}%")
    print(f"  Max Drawdown:      {max_dd:.1f}%")
    print(f"  Piso mas bajo:     ${min_bank:,.2f}")
    print(f"  Apuesta Promedio:  ${avg_stake:,.2f}")
    print()
