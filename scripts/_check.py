import json
d = json.load(open('frontend/data/picks.json'))
print(f"Total picks hoy: {len(d)}")
print(f"Fecha: {d[0]['game_date']}")
for p in d[:5]:
    print(f"  {p['away_team_name']} @ {p['home_team_name']} | Status: {p['status']} | Edge: {p.get('edge','N/A')}")
