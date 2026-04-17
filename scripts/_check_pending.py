from dotenv import load_dotenv
import os, json
load_dotenv()
from supabase import create_client
c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Check PENDING records
r = c.table('sindicato_ledger').select('*').eq('status', 'PENDING').execute()
print(f"Total PENDING: {len(r.data)}")
for d in r.data:
    print(json.dumps({k: d[k] for k in ['game_pk','pick_team','market_type','odds','stake','game_date']}, indent=None))
    # Print ALL keys to see the spread field
    print(f"  ALL KEYS: {list(d.keys())}")
    break  # Just show one to see structure
print("\n--- All PENDING ---")
for d in r.data:
    sp = d.get('spread_point', d.get('point', d.get('spread', 'NO_FIELD')))
    print(f"  {d['game_date']} | {d['pick_team']} | {d['market_type']} | odds:{d['odds']} | spread:{sp}")
