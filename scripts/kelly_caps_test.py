"""
COMPARATIVA DE TECHOS (CAPS) PARA QUARTER-KELLY
Pega esta celda en Colab DESPUÉS de haber corrido el backtest completo.
Utiliza la variable all_picks que ya está en memoria.
"""

BANKROLL = 5000.0

print(f"\n{'='*70}")
print(f" QUARTER-KELLY: COMPARATIVA DE TECHO POR APUESTA (5% vs 10% vs 15%)")
print(f"{'='*70}")

for cap_pct in [0.05, 0.10, 0.15]:
    bank = BANKROLL
    max_bank = bank
    min_bank = bank
    stakes = []
    monthly_pnl = {}

    for p in all_picks:
        odds = p['odds']
        dec = odds_to_decimal(odds)
        
        p_modelo = p['confianza'] / 100.0
        kelly_full = (p_modelo * (dec - 1) - (1 - p_modelo)) / (dec - 1)
        kelly_full = max(0, kelly_full)
        kelly_adj = kelly_full * 0.25  # Quarter Kelly fijo
        stake = round(bank * kelly_adj, 2)
        stake = max(10, min(stake, bank * cap_pct))  # TECHO VARIABLE
        
        if p['won']:
            profit = stake * (dec - 1)
        else:
            profit = -stake
        
        month = p['month']
        if month not in monthly_pnl:
            monthly_pnl[month] = 0.0
        monthly_pnl[month] += profit
        
        stakes.append(stake)
        bank += profit
        max_bank = max(max_bank, bank)
        min_bank = min(min_bank, bank)

    roi = ((bank - BANKROLL) / BANKROLL) * 100
    max_dd = ((max_bank - min_bank) / max_bank) * 100
    capital_dd = ((BANKROLL - min_bank) / BANKROLL) * 100
    avg_stake = sum(stakes) / len(stakes) if stakes else 0
    max_stake = max(stakes) if stakes else 0
    
    print(f"\n{'─'*50}")
    print(f"  TECHO: {int(cap_pct*100)}% del Bankroll por Apuesta")
    print(f"{'─'*50}")
    print(f"  Bankroll Final:      ${bank:,.2f}")
    print(f"  Profit Neto:         ${bank - BANKROLL:,.2f}")
    print(f"  ROI Total:           {roi:.1f}%")
    print(f"  Max Drawdown (pico): {max_dd:.1f}%")
    print(f"  Caída vs Capital:    {capital_dd:.1f}% (piso: ${min_bank:,.2f})")
    print(f"  Apuesta Promedio:    ${avg_stake:,.2f}")
    print(f"  Apuesta Máxima:      ${max_stake:,.2f}")
    print(f"  ---")
    month_names = {4:'ABR', 5:'MAY', 6:'JUN', 7:'JUL', 8:'AGO', 9:'SEP'}
    for m in sorted(monthly_pnl.keys()):
        pnl = monthly_pnl[m]
        color = "✅" if pnl > 0 else "🔴"
        print(f"  {color} {month_names.get(m, m)}: {'+' if pnl>0 else ''}${pnl:,.2f}")
