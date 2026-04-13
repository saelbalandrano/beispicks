from data.updater import DailyUpdater

def main():
    print("Iniciando script temporal de Setup para parchar huecos en la temporada 2026...")
    
    try:
        # Inicializamos nuestra bestia ingridora que ya carga automáticamente el .env
        updater = DailyUpdater()
        
        # Mandamos llamar el Backfill con las fechas solicitadas
        updater.auditar_y_rellenar_huecos("03/01/2026", "04/11/2026")
        
        print("Misión cumplida. Bóveda histórica actualizada con éxito.")
    except Exception as e:
        print(f"Ocurrió un dolor de cabeza en el proceso: {str(e)}")

if __name__ == "__main__":
    main()
