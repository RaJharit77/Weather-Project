import pandas as pd
import requests
import os
from pathlib import Path

def fetch_historical_data(cities):
    """
    Récupère les données historiques depuis une source externe (ex: Kaggle)
    """
    data_dir = Path(__file__).parents[2] / 'data' / 'raw'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Exemple avec un dataset fictif - à remplacer par votre source réelle
    historical_data = []
    
    for city in cities:
        # Simulation de données historiques
        df = pd.DataFrame({
            'city': [city] * 12,
            'month': range(1, 13),
            'avg_temp': [10 + i * 2 for i in range(12)],
            'precipitation': [50 + i * 5 for i in range(12)],
            'wind_speed': [10 + i for i in range(12)]
        })
        historical_data.append(df)
    
    full_df = pd.concat(historical_data)
    full_df.to_csv(data_dir / 'historical_weather.csv', index=False)
    return full_df