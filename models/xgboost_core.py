import xgboost as xgb
import pandas as pd

class QuantModel:
    """
    Modelo predictivo central usando XGBoost 
    para evaluar la probabilidad de vitoria en los juegos de la MLB.
    """

    def __init__(self):
        """
        Inicializa modelo con hiperparámetros estrictos previamente definidos para su estabilidad.
        """
        self.model = xgb.XGBClassifier(
            subsample=0.8, 
            n_estimators=200, 
            max_depth=5, 
            learning_rate=0.01, 
            colsample_bytree=0.8, 
            random_state=42
        )

    def train(self, X: pd.DataFrame, y: pd.Series):
        """
        Ajusta/Entrena el modelo usando los features y labels.
        
        :param X: Matriz de características del histórico.
        :param y: Label (1 victoria del local, 0 derrota/empate)
        """
        self.model.fit(X, y)

    def predict_proba(self, X: pd.DataFrame):
        """
        Devuelve el porcentaje de confianza / probabilidad de victoria 
        del equipo local (clase 1).
        
        :param X: DataFrame con la data del evento a predecir.
        :return: Array de valores float con las probabilidades de vitoria local.
        """
        probabilidades = self.model.predict_proba(X)
        
        # En clasificación binaria, XGBoost devuelve [Probabilidad_0, Probabilidad_1].
        # Así que extraemos exclusivamente de la probabilidad 1 (Local gana).
        return probabilidades[:, 1]
