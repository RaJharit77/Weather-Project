import gspread # type: ignore
import pandas as pd
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials # type: ignore
from pathlib import Path
import logging

# Configuration
SHEET_URLS = {
    'monthly_scores': '1sPXRdUHMasRur_4Ip5Fw4CDsta9e9KStYJSoVt7asgI',
    'merged_data': '1IyPIhvDhH51dDtgNe-CW81wnOhBwOt13_mppf9a_ozk',
    'historical': '18cfFKsLZTla0EkeRdO5TyFfmXv683v1m4G3AJAOV3Y4',
    'current': '1mz0ubMdm056Cj8mTAR2hk-bSMdzA5thvLw4g6thP6Ag'
}

def clean_dataframe(df):
    """Nettoie le dataframe en remplaçant les valeurs problématiques"""
    # Remplacer les NaN et inf par des valeurs nulles
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna('')
    
    # Convertir les colonnes numériques en strings pour éviter les problèmes de sérialisation
    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else '')
    
    return df

def update_all_sheets():
    try:
        # Authentification
        base_dir = Path(__file__).parents[2]
        creds_path = base_dir / 'credentials' / 'weather_tourism_project.json'
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # Mettre à jour chaque feuille
        update_sheet(client, 'current', base_dir / 'data' / 'raw' / 'current_weather.csv')
        update_sheet(client, 'historical', base_dir / 'data' / 'raw' / 'historical_weather.csv')
        update_sheet(client, 'merged_data', base_dir / 'data' / 'processed' / 'merged_weather_data.csv')
        update_sheet(client, 'monthly_scores', base_dir / 'data' / 'outputs' / 'monthly_weather_scores.csv')
        
        logging.info("Tous les Google Sheets ont été mis à jour")
        return True
        
    except Exception as e:
        logging.error(f"Erreur lors de la mise à jour des sheets: {str(e)}")
        raise

def update_sheet(client, sheet_type, file_path):
    try:
        if not file_path.exists():
            raise FileNotFoundError(f"Fichier {file_path} introuvable")
            
        df = pd.read_csv(file_path)
        
        # Nettoyer les données avant envoi
        df = clean_dataframe(df)
        
        sheet = client.open_by_key(SHEET_URLS[sheet_type])
        worksheet = sheet.get_worksheet(0)
        
        # Convertir en liste de listes pour l'API
        data = [df.columns.values.tolist()] + df.values.tolist()
        
        # Mise à jour par lots pour les grands datasets
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            worksheet.update(f'A{i+1}', batch)
            
        logging.info(f"Feuille {sheet_type} mise à jour avec succès")
        
    except Exception as e:
        logging.error(f"Erreur lors de la mise à jour de {sheet_type}: {str(e)}")
        raise