import json
d = json.load(open('frontend/data/picks.json'))
print(f"Total: {len(d)} picks")
for p in d:
    print(f"  {p['away_team_name']} @ {p['home_team_name']} | ML:{p['status']} | Edge: {p.get('edge', 0)}%")

print("\n--- LEDGER ---")
ld = json.load(open('frontend/data/ledger.json'))
print(f"Total registros: {len(ld)}")
for r in ld[-5:]:
    print(f"  {r['game_date']} | {r['pick_team']} | {r['market_type']} | odds:{r['odds']} | stake:{r.get('stake',100)} | {r['status']}")
