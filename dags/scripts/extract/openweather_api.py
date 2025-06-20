import requests
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

API_KEY = "6fb650f9a94642f038de6f1e10379da4"

def fetch_current_weather(cities):
    data_dir = Path(__file__).parents[2] / 'data' / 'raw'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    current_data = []
    for city in cities:
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url)
            data = response.json()
            
            if 'main' not in data:
                print(f"Données incomplètes pour {city}")
                continue
                
            current_data.append({
                'city': city,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'temp': data['main']['temp'],
                'precipitation': data.get('rain', {}).get('1h', 0),
                'wind_speed': data['wind']['speed'],
                'humidity': data['main']['humidity']
            })
        except Exception as e:
            print(f"Erreur pour {city}: {str(e)}")
    
    df = pd.DataFrame(current_data) if current_data else pd.DataFrame()
    df.to_csv(data_dir / 'current_weather.csv', index=False)  # <-- LIGNE CRITIQUE AJOUTÉE
    return df