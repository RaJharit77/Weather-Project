import pandas as pd
from pathlib import Path

def clean_and_merge():
    """
    Nettoie et fusionne les données historiques et actuelles
    """
    data_dir = Path(__file__).parents[2] / 'data'
    raw_dir = data_dir / 'raw'
    processed_dir = data_dir / 'processed'
    
    # Créer les répertoires si inexistants
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(exist_ok=True)
    
    try:
        # Charger les données avec vérification d'existence
        historical_path = raw_dir / 'historical_weather.csv'
        current_path = raw_dir / 'current_weather.csv'
        
        if not historical_path.exists():
            raise FileNotFoundError(f"Fichier historique introuvable : {historical_path}")
        if not current_path.exists():
            raise FileNotFoundError(f"Fichier courant introuvable : {current_path}")
            
        historical = pd.read_csv(historical_path)
        current = pd.read_csv(current_path)
        
        # Nettoyage des données
        historical['date'] = pd.to_datetime('2023-' + historical['month'].astype(str) + '-01')
        current['date'] = pd.to_datetime(current['date'])
        current['month'] = current['date'].dt.month
        
        # Fusion et sauvegarde
        merged = pd.concat([historical.rename(columns={'avg_temp': 'temp'}), current])
        merged.to_csv(processed_dir / 'merged_weather_data.csv', index=False)
        return merged
        
    except Exception as e:
        print(f"Erreur critique lors du nettoyage : {str(e)}")
        raise