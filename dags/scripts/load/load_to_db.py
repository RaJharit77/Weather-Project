import gspread # type: ignore
import pandas as pd
from airflow.models import Variable
import json
from oauth2client.service_account import ServiceAccountCredentials # type: ignore
from pathlib import Path
import logging

SHEET_URLS = json.loads(Variable.get("GOOGLE_SHEETS_URLS")) 

def load_to_database():
    try:
        # Configuration des chemins
        base_dir = Path(__file__).parents[2]
        creds_path = base_dir / 'credentials' / 'weather_tourism_project.json'
        data_path = base_dir / 'data' / 'outputs' / 'monthly_weather_scores.csv'
        
        logging.info(f"Chemin des credentials: {creds_path}")
        logging.info(f"Chemin des données: {data_path}")

        # Vérification des fichiers
        if not creds_path.exists():
            raise FileNotFoundError(f"Fichier credentials introuvable : {creds_path}")
        if not data_path.exists():
            raise FileNotFoundError(f"Fichier de données introuvable : {data_path}")

        # Authentification
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
            client = gspread.authorize(creds)
            logging.info("Authentification réussie avec Google Sheets")
        except Exception as auth_error:
            logging.error(f"Erreur d'authentification: {str(auth_error)}")
            raise

        # Chargement des données
        try:
            df = pd.read_csv(data_path)
            logging.info(f"Données chargées : {len(df)} lignes")
        except Exception as data_error:
            logging.error(f"Erreur de lecture des données: {str(data_error)}")
            raise

        # Mise à jour du Google Sheet existant
        try:
            # Ouvrir le spreadsheet existant
            sheet = client.open_by_key(SHEET_URLS['monthly_scores'])
            worksheet = sheet.get_worksheet(0)
            
            # Effacer et mettre à jour les données
            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
            logging.info("Google Sheet mis à jour avec succès")
            return True
            
        except Exception as sheet_error:
            logging.error(f"Erreur de mise à jour du Google Sheet: {str(sheet_error)}")
            raise

    except Exception as e:
        logging.error(f"Erreur critique lors du chargement : {str(e)}")
        raise