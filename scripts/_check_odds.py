from dotenv import load_dotenv
import os, json
load_dotenv()
from supabase import create_client
c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Get one row to see all columns
r = c.table('historical_odds').select('*').limit(1).execute()
print("=== historical_odds columns ===")
if r.data:
    print(json.dumps(r.data[0], indent=2, default=str))
    print(f"\nKeys: {list(r.data[0].keys())}")

# Get spreads rows
r2 = c.table('historical_odds').select('*').limit(5).execute()
print("\n=== All market types found ===")
markets = set()
for d in r2.data:
    for k in d.keys():
        if 'market' in k.lower() or 'type' in k.lower() or 'spread' in k.lower() or 'point' in k.lower():
            print(f"  {k}: {d[k]}")
    markets.add(str({k: d[k] for k in d.keys() if 'market' in k.lower() or 'type' in k.lower()}))

# Wider search - get 20 rows to find spread data
r3 = c.table('historical_odds').select('*').limit(20).execute()
print(f"\n=== Sample of 20 rows ===")
for d in r3.data[:3]:
    print(json.dumps(d, indent=2, default=str))
    print("---")
