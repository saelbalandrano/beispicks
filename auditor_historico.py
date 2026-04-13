from data.updater import DailyUpdater

def main():
    print("--- Lanzando Escudo Anti-Sobrecostos de API ---")
    updater = DailyUpdater()
    # MODO INTELIGENTE DE PARCHEO: Solo busca y rellena juegos huérfanos sin quemar API (force_overwrite=False)
    updater.auditar_y_rellenar_momios_historicos(dry_run=False, force_overwrite=False)

if __name__ == "__main__":
    main()
