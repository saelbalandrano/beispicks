import random

def simular_juego_mc_legal(k_home, bb_home, bp_home_burned, k_away, bb_away, bp_away_burned, simulaciones=5000):
    """
    Simula un juego de béisbol mediante el método de Montecarlo para proyectar % de victoria del equipo local.

    Parámetros:
    - k_home: Tasa de ponches (K%) del abridor local.
    - bb_home: Tasa de boletos (BB%) del abridor local.
    - bp_home_burned: Relivistas no disponibles (quemados) del bullpen local.
    - k_away: Tasa de ponches (K%) del abridor visitante.
    - bb_away: Tasa de boletos (BB%) del abridor visitante.
    - bp_away_burned: Relivistas no disponibles (quemados) del bullpen visitante.
    - simulaciones: Iteraciones a correr (Por defecto 5000).

    Retorna:
    - float: Porcentaje de probabilidad de victoria del equipo local (0-100).
    """

    def simular_turno(inning, starter_k, starter_bb, bullpen_quemado):
        """
        Simula el resultado de cada turno al bate, validando el inning actual para evaluar cansancio 
        o la entrada de un relevista.
        """
        # A partir del 6to inning asumimos que entra el relevo
        if inning >= 6:
            # Si el bullpen está muy usado, penaliza las métricas. Si es fresco, las mejora.
            k_prob, bb_prob = (0.18, 0.12) if bullpen_quemado >= 3 else (0.24, 0.08)
        else:
            # Utilizamos los stats del abridor
            k_prob, bb_prob = starter_k, starter_bb
            # A partir de la 4ta se castiga un poco su efectividad (Times Through the Order Penalty)
            if inning >= 4: 
                k_prob -= 0.02
                bb_prob += 0.01

        # Limitar valores atípicos protegiendo el suelo de los porcentajes
        k_prob = k_prob if k_prob > 0 else 0.22
        bb_prob = bb_prob if bb_prob > 0 else 0.08
        
        # Tirada de "dado" de probabilidad
        dado = random.random()
        
        if dado < k_prob: 
            return 'OUT'
        elif dado < (k_prob + bb_prob): 
            return 'ON_BASE'
        else: 
            # Empíricamente en bolas en juego (BIP), el promedio de embazarse es cerca de un 30% (BABIP normal)
            return 'ON_BASE' if random.random() < 0.30 else 'OUT'

    victorias_home = 0
    
    # Bucle principal para N simulaciones 
    for _ in range(simulaciones):
        s_home, s_away = 0, 0
        
        # Iteración inning por inning (1 al 9)
        for inn in range(1, 10):
            # Parte alta de la entrada (visita batea contra local)
            outs, bases = 0, 0
            while outs < 3:
                if simular_turno(inn, k_home, bb_home, bp_home_burned) == 'OUT': 
                    outs += 1
                else: 
                    bases += 1
                    s_away += (1 if bases >= 3 else 0)
                    bases = min(bases, 2)
                    
            # Parte baja de la entrada (local batea contra visita)
            outs, bases = 0, 0
            while outs < 3:
                if simular_turno(inn, k_away, bb_away, bp_away_burned) == 'OUT': 
                    outs += 1
                else: 
                    bases += 1
                    s_home += (1 if bases >= 3 else 0)
                    bases = min(bases, 2)
                    
        # Evaluar resultado final y asignar
        if s_home > s_away: 
            victorias_home += 1
        elif s_home == s_away: 
            # Los empates son medios puntos en la expectativa
            victorias_home += 0.5 
            
    # Entregar el % directo
    return (victorias_home / simulaciones) * 100
