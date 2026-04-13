import os
from dotenv import load_dotenv
from supabase import create_client, Client

class SindicatoLiquidator:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.client: Client = create_client(self.url, self.key)

    def calcular_payout(self, odds: int, stake: float) -> float:
        """
        Calcula el profit para una apuesta con stake dinámico (Kelly).
        Favoritos (-): Ganas stake * (100 / |odds|).
        Underdogs (+): Ganas stake * (odds / 100).
        """
        if odds < 0:
            return round(stake * (100.0 / abs(odds)), 2)
        else:
            return round(stake * (odds / 100.0), 2)

    def liquidar_juegos_pendientes(self):
        print("\n -> [LIQUIDATOR] Iniciando auditoría de fondos y liquidación de pendientes...")
        
        try:
            # 1. Obtener ledger PENDING
            res_pendientes = self.client.table('sindicato_ledger').select('*').eq('status', 'PENDING').execute()
            if not res_pendientes.data:
                print(" -> [LIQUIDATOR] No hay transacciones pendientes en el libro mayor.")
                return
            
            pendientes = res_pendientes.data
            print(f" -> [LIQUIDATOR] {len(pendientes)} juegos pendientes encontrados. Correlacionando resultados...")
            
            # 2. Extraer los partidos correspondientes de mlb_games_history
            pks = [p['game_pk'] for p in pendientes]
            res_history = self.client.table('mlb_games_history').select('game_pk, status, home_score, away_score').in_('game_pk', pks).execute()
            
            if not res_history.data:
                print(" -> [LIQUIDATOR] Resultados históricos no disponibles aún.")
                return
            
            history_dict = {h['game_pk']: h for h in res_history.data}
            lote_upsert = []
            
            for bet in pendientes:
                pk = bet['game_pk']
                if pk not in history_dict:
                    continue
                    
                match = history_dict[pk]
                
                # MLB API Status = 'Final' u otros marcadores
                m_status = match.get('status', '').lower()
                if 'final' not in m_status and 'completed' not in m_status:
                    continue
                    
                home_score = match.get('home_score', 0)
                away_score = match.get('away_score', 0)
                
                # Stake dinámico (Kelly) o fallback a $100 flat si no existe
                stake = float(bet.get('stake', 100.0))
                
                # Lógica H2H
                if bet['market_type'] == 'h2h':
                    if home_score == away_score:
                        bet['status'] = 'PUSH'
                        bet['profit_loss'] = 0.00
                    else:
                        home_won = home_score > away_score
                        picked_home = (bet['pick_team'] == 'HOME')
                        
                        ganador = (home_won and picked_home) or (not home_won and not picked_home)
                        if ganador:
                            bet['status'] = 'WON'
                            bet['profit_loss'] = self.calcular_payout(bet['odds'], stake)
                        else:
                            bet['status'] = 'LOST'
                            bet['profit_loss'] = -stake
                            
                elif bet['market_type'] == 'spreads' or bet['market_type'] == 'totals':
                    # Lógica de spreads/runlines se expandirá aquí cuando el modelo los apoye
                    pass
                
                if bet['status'] != 'PENDING':
                    lote_upsert.append(bet)
                    
            if lote_upsert:
                # Actualizar Supabase Ledger
                self.client.table('sindicato_ledger').upsert(lote_upsert, on_conflict='game_pk,market_type').execute()
                wins = sum(1 for b in lote_upsert if b['status'] == 'WON')
                losses = sum(1 for b in lote_upsert if b['status'] == 'LOST')
                pushes = sum(1 for b in lote_upsert if b['status'] == 'PUSH')
                total_profit = sum(b['profit_loss'] for b in lote_upsert)
                print(f" -> [LIQUIDATOR] Liquidación exitosa: {wins} GANADAS, {losses} PERDIDAS, {pushes} PUSH.")
                print(f" -> [LIQUIDATOR] Movimiento Neto de Capital: {'+' if total_profit>0 else ''}${total_profit:.2f}")
            else:
                print(" -> [LIQUIDATOR] Partidos pendientes aún no finalizados en vida real.")
                
        except Exception as e:
            print(f" -> [WARNING] Error masivo en Liquidator: {e}")

if __name__ == "__main__":
    liq = SindicatoLiquidator()
    liq.liquidar_juegos_pendientes()
