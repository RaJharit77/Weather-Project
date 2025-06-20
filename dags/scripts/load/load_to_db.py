import gspread # type: ignore
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials # type: ignore
from pathlib import Path
import logging

def load_to_database():
    try:
        # Configuration des chemins
        base_dir = Path(__file__).parents[2]
        creds_path = base_dir / 'credential' / 'weather_tourism_project.json'
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

        # Mise à jour du Google Sheet
        try:
            # Vérification de l'existence du spreadsheet
            spreadsheet_title = "Weather Tourism Data"
            try:
                sheet = client.open(spreadsheet_title)
                logging.info(f"Spreadsheet '{spreadsheet_title}' trouvé")
            except gspread.SpreadsheetNotFound:
                # Création si non trouvé
                logging.info(f"Création du spreadsheet '{spreadsheet_title}'")
                sheet = client.create(spreadsheet_title)
                # Partage avec le compte de service dans google cloud
                sheet.share('airflow-sheets-access@weather-tourism-project.iam.gserviceaccount.com', perm_type='user', role='writer')
            
            worksheet = sheet.get_worksheet(0) if len(sheet.worksheets()) > 0 else sheet.add_worksheet("Données", 100, 20)
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