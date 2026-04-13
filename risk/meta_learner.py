import pandas as pd
from typing import Tuple, List, Any

class OmniEscudo:
    """
    Gestor de Riesgo Meta-Learner (Omni-Escudo).
    Autoadapta el riesgo de las apuestas basado en dinámicas de rachas sin hardcodear IDs,
    previniendo el overfitting a nombres de equipos o años pasados.
    """
    
    def __init__(self):
        # Listas dinámicas para la clasificación diaria
        self.resilientes: set = set()
        self.cristal: set = set()
        self.espejismo: set = set()
        self.trenes: set = set()

    def perfilar_equipos(self, df_resultados: pd.DataFrame, col_team='team_id', col_win='win', col_date='date'):
        """
        Calcula la frecuencia de rompimiento de rachas y clasifica dinámicamente
        a los equipos basándose en el historial de los últimos 45 días.
        
        :param df_resultados: DataFrame con resultados históricos.
        """
        # Limpiar listas
        self.resilientes.clear()
        self.cristal.clear()
        self.espejismo.clear()
        self.trenes.clear()
        
        # Agrupar por equipo
        for tid, group in df_resultados.sort_values(col_date).groupby(col_team):
            wins = group[col_win].tolist()
            
            # Arrays para almacenar la longitud de las rachas rotas
            losing_streaks_lengths = []
            winning_streaks_lengths = []
            
            c_streak = 0
            is_winning = None 
            
            for w in wins:
                # w es 1 si ganó, 0 si perdió
                if is_winning is None:
                    is_winning = bool(w)
                    c_streak = 1
                elif is_winning == bool(w):
                    c_streak += 1
                else:
                    # Racha rota, la guardamos
                    if is_winning:
                        winning_streaks_lengths.append(c_streak)
                    else:
                        losing_streaks_lengths.append(c_streak)
                    # Reiniciar con la nueva racha
                    is_winning = bool(w)
                    c_streak = 1
                    
            # Evaluar matemáticas del equipo para perfilar
            cortas_perdidas = sum(1 for x in losing_streaks_lengths if x <= 2)
            largas_perdidas = sum(1 for x in losing_streaks_lengths if x >= 4)
            
            medias_ganadas = sum(1 for x in winning_streaks_lengths if 3 <= x <= 4)
            largas_ganadas = sum(1 for x in winning_streaks_lengths if x >= 5)
            
            # Clasificación de rachas perdedoras
            # Resilientes: Rompen rápido las rachas perdedoras la mayoría de las veces
            if cortas_perdidas > 0 and cortas_perdidas > largas_perdidas:
                self.resilientes.add(tid)
            # Cristal: Sufren constantes rachas perdedoras prolongadas
            elif largas_perdidas >= 2 or largas_perdidas > cortas_perdidas:
                self.cristal.add(tid)
                
            # Clasificación de rachas ganadoras
            # Espejismo: Acumulan rachas de 3~4 juegos ganados pero se mueren rápido
            if medias_ganadas >= 2 and largas_ganadas == 0:
                self.espejismo.add(tid)
            # Trenes: Consiguen rachas ganadoras verdaderamente largas de 5+
            elif largas_ganadas > 0 and largas_ganadas >= medias_ganadas:
                self.trenes.add(tid)

    def calcular_edge(self, prob_modelo: float, cuota_americana: int) -> float:
        """
        Calcula el Edge frente a la casa de apuestas (Vegas).
        Convierte la cuota americana a probabilidad implícita.
        
        :param prob_modelo: Probabilidad calculada por nuestro modelo (en %).
        :param cuota_americana: Cuota del moneyline (ej. -150 o +130).
        :return: Edge (Ventaja) = prob_modelo - prob_vegas.
        """
        if cuota_americana < 0:
            prob_vegas = (abs(cuota_americana) / (abs(cuota_americana) + 100)) * 100
        else:
            prob_vegas = (100 / (cuota_americana + 100)) * 100
            
        return prob_modelo - prob_vegas

    def evaluar_apuesta(self, tid: Any, prob_xgb: float, prob_mc: float, cuota: int, racha_perdedora: int, racha_ganadora: int) -> Tuple[str, float, float]:
        """
        Pasa la apuesta por 3 filtros principales para decidir su viabilidad.
        Devuelve una tupla uniforme (Decisión, Edge, Confianza Sindicato)
        """
        # Paso 1 (Francotirador): Promedio del Sindicato
        confianza_sindicato = (prob_xgb + prob_mc) / 2
        edge = self.calcular_edge(confianza_sindicato, cuota)
        
        if confianza_sindicato < 62.0 and edge < 3.0:
            return ("IGNORAR", edge, confianza_sindicato)
            
        # Paso 2 (Escudo de Rachas)
        if tid in self.cristal and racha_perdedora >= 3:
            return ("FADE", edge, confianza_sindicato)
            
        if tid in self.resilientes and racha_perdedora > 0:
            return ("DESCANSAR", edge, confianza_sindicato)
            
        # Paso 3 (Escudo de Burbujas)
        if tid in self.espejismo and racha_ganadora >= 3:
            return ("FADE", edge, confianza_sindicato)
            
        # Por defecto
        return ("NORMAL", edge, confianza_sindicato)
