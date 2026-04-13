import os
from dotenv import load_dotenv
from supabase import create_client
load_dotenv()
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
res = sb.table('sindicato_ledger').select('game_pk, game_date, pick_team, odds, stake, status, profit_loss').limit(3).execute()
for r in res.data:
    print(r)
total = sb.table('sindicato_ledger').select('id', count='exact').execute()
print(f"\nColumna 'stake' detectada. Total registros: {total.count}")
