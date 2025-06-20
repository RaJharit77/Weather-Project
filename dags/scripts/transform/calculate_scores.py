import pandas as pd
from pathlib import Path
import logging

def calculate_weather_scores():
    """Calcule les scores météo avec une meilleure gestion des erreurs"""
    try:
        # Configuration des chemins avec vérification
        data_dir = Path(__file__).parents[2] / 'data'
        processed_dir = data_dir / 'processed'
        output_dir = data_dir / 'outputs'
        
        # Création des répertoires si inexistants
        output_dir.mkdir(parents=True, exist_ok=True)
        input_file = processed_dir / 'merged_weather_data.csv'
        
        # Vérification de l'existence du fichier
        if not input_file.exists():
            raise FileNotFoundError(f"Fichier d'entrée introuvable: {input_file}")

        # Chargement des données
        logging.info(f"Chargement des données depuis {input_file}")
        df = pd.read_csv(input_file)
        
        # Validation des colonnes requises
        required_cols = ['city', 'month', 'temp', 'precipitation', 'wind_speed']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

        # Nettoyage des données
        df.fillna({
            'temp': df['temp'].mean(),
            'precipitation': 0,
            'wind_speed': 0
        }, inplace=True)

        # Fonction de calcul de score avec gestion d'erreur
        def calculate_score(row):
            try:
                temp_score = max(0, 1 - abs(row['temp'] - 25) / 10)
                precip_score = max(0, 1 - row['precipitation'] / 100)
                wind_score = max(0, 1 - row['wind_speed'] / 20)
                return 0.5 * temp_score + 0.3 * precip_score + 0.2 * wind_score
            except Exception as e:
                logging.error(f"Erreur ligne {row.name}: {str(e)}")
                return 0

        # Application du calcul
        logging.info("Calcul des scores météo")
        df['weather_score'] = df.apply(calculate_score, axis=1)

        # Agrégation par ville et mois
        logging.info("Agrégation des données")
        monthly_scores = df.groupby(['city', 'month']).agg({
            'weather_score': 'mean',
            'temp': 'mean',
            'precipitation': 'mean',
            'wind_speed': 'mean'
        }).reset_index()

        # Sauvegarde
        output_file = output_dir / 'monthly_weather_scores.csv'
        monthly_scores.to_csv(output_file, index=False)
        logging.info(f"Données sauvegardées dans {output_file}")
        
        return monthly_scores

    except Exception as e:
        logging.error(f"ERREUR CRITIQUE: {str(e)}")
        raise