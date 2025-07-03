import os
import pandas as pd
from datetime import datetime

def merge_weather_data():
    # Définir les chemins
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    HISTORICAL_DIR = os.path.join(BASE_DIR, "data", "historical_data")
    PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Fichier de sortie combiné
    OUTPUT_PATH = os.path.join(PROCESSED_DIR, "weather_data_combined.csv")

    # Charger les données historiques (si elles existent)
    historical_file = os.path.join(HISTORICAL_DIR, "weather_data_for_the_last_3_years.csv")
    if os.path.exists(historical_file):
        df_historical = pd.read_csv(historical_file)
        df_historical = df_historical.rename(columns={"date_extraction": "date", "extracted_at": "extracted_at"})
        df_historical["date"] = pd.to_datetime(df_historical["date"]).dt.strftime("%Y-%m-%d")
        df_historical["source"] = df_historical.get("source", "Open-Meteo")
        df_historical["is_historical"] = True
        for col in ["pression", "description"]:
            df_historical[col] = pd.NA
    else:
        df_historical = pd.DataFrame(columns=[
            "ville", "pays", "continent", "date", "temperature", "humidite", 
            "pression", "precipitation", "description", "source", "extracted_at", "is_historical"
        ])

    # Charger tous les fichiers CSV dans tous les sous-dossiers de RAW_DIR
    all_daily_data = []
    for root, _, files in os.walk(RAW_DIR):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)
                df["date"] = pd.to_datetime(df["date_extraction"]).dt.strftime("%Y-%m-%d")
                df["precipitation"] = pd.NA
                df["source"] = "OpenWeatherMap"
                df["extracted_at"] = df["date_extraction"]
                df["is_historical"] = False
                all_daily_data.append(df)

    # Fusionner les données quotidiennes
    if all_daily_data:
        df_daily = pd.concat(all_daily_data, ignore_index=True)
    else:
        df_daily = pd.DataFrame(columns=[
            "ville", "pays", "continent", "date", "temperature", "humidite", 
            "pression", "description", "precipitation", "source", "extracted_at", "is_historical"
        ])

    # Fusionner les données historiques et quotidiennes
    df_combined = pd.concat([df_historical, df_daily], ignore_index=True)

    # Supprimer les lignes pour NukuʻAlofa
    df_combined = df_combined[df_combined['ville'] != 'NukuʻAlofa']

    # Supprimer les doublons
    df_combined = df_combined.sort_values(by="extracted_at", ascending=False)
    df_combined = df_combined.drop_duplicates(subset=["ville", "date"], keep="last")

    # Trier les données
    df_combined["date"] = pd.to_datetime(df_combined["date"], errors="coerce")
    df_combined = df_combined.sort_values(by=["ville", "date"])

    # Réorganiser les colonnes
    df_combined = df_combined[[
        "ville", "pays", "continent", "date", "temperature", "humidite", 
        "pression", "precipitation", "description", "source", "extracted_at", "is_historical"
    ]]

    # Sauvegarder
    df_combined.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"✅ Données combinées sauvegardées dans {OUTPUT_PATH}")
    print(f"Nombre total d'enregistrements : {len(df_combined)}")
    print("Résumé des valeurs manquantes :\n", df_combined.isna().sum())

if __name__ == "__main__":
    merge_weather_data()