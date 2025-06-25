import os
import requests
import json
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("La clé API est introuvable. Vérifie ton fichier .env.")

cities = [
    {"ville": "Antananarivo", "pays": "Madagascar", "continent": "Afrique"},
    {"ville": "Nairobi", "pays": "Kenya", "continent": "Afrique"},
    {"ville": "Pretoria", "pays": "Afrique du Sud", "continent": "Afrique"},
    {"ville": "Le Caire", "pays": "Égypte", "continent": "Afrique"},
    {"ville": "Paris", "pays": "France", "continent": "Europe"},
    {"ville": "Berlin", "pays": "Allemagne", "continent": "Europe"},
    {"ville": "Londres", "pays": "Royaume-Uni", "continent": "Europe"},
    {"ville": "Rome", "pays": "Italie", "continent": "Europe"},
    {"ville": "Tokyo", "pays": "Japon", "continent": "Asie"},
    {"ville": "Pékin", "pays": "Chine", "continent": "Asie"},
    {"ville": "New Delhi", "pays": "Inde", "continent": "Asie"},
    {"ville": "Riyad", "pays": "Arabie Saoudite", "continent": "Asie"},
    {"ville": "Washington", "pays": "États-Unis", "continent": "Amérique"},
    {"ville": "Ottawa", "pays": "Canada", "continent": "Amérique"},
    {"ville": "Brasília", "pays": "Brésil", "continent": "Amérique"},
    {"ville": "Buenos Aires", "pays": "Argentine", "continent": "Amérique"},
    {"ville": "Canberra", "pays": "Australie", "continent": "Océanie"},
    {"ville": "Wellington", "pays": "Nouvelle-Zélande", "continent": "Océanie"},
    {"ville": "Ankara", "pays": "Turquie", "continent": "Moyen-Orient"},
    {"ville": "Jérusalem", "pays": "Israël", "continent": "Moyen-Orient"},
    {"ville": "Bamako", "pays": "Mali", "continent": "Afrique"},
    {"ville": "Dakar", "pays": "Sénégal", "continent": "Afrique"},
    {"ville": "Abuja", "pays": "Nigeria", "continent": "Afrique"},
    {"ville": "Accra", "pays": "Ghana", "continent": "Afrique"},
    {"ville": "Alger", "pays": "Algérie", "continent": "Afrique"},
    {"ville": "Addis-Abeba", "pays": "Éthiopie", "continent": "Afrique"},
    {"ville": "Madrid", "pays": "Espagne", "continent": "Europe"},
    {"ville": "Oslo", "pays": "Norvège", "continent": "Europe"},
    {"ville": "Vienne", "pays": "Autriche", "continent": "Europe"},
    {"ville": "Athènes", "pays": "Grèce", "continent": "Europe"},
    {"ville": "Bruxelles", "pays": "Belgique", "continent": "Europe"},
    {"ville": "Amsterdam", "pays": "Pays-Bas", "continent": "Europe"},
    {"ville": "Séoul", "pays": "Corée du Sud", "continent": "Asie"},
    {"ville": "Hanoï", "pays": "Vietnam", "continent": "Asie"},
    {"ville": "Bangkok", "pays": "Thaïlande", "continent": "Asie"},
    {"ville": "Manille", "pays": "Philippines", "continent": "Asie"},
    {"ville": "Kuala Lumpur", "pays": "Malaisie", "continent": "Asie"},
    {"ville": "Kaboul", "pays": "Afghanistan", "continent": "Asie"},
    {"ville": "Mexico", "pays": "Mexique", "continent": "Amérique"},
    {"ville": "Lima", "pays": "Pérou", "continent": "Amérique"},
    {"ville": "Santiago", "pays": "Chili", "continent": "Amérique"},
    {"ville": "La Paz", "pays": "Bolivie", "continent": "Amérique"},
    {"ville": "Port-au-Prince", "pays": "Haïti", "continent": "Amérique"},
    {"ville": "La Havane", "pays": "Cuba", "continent": "Amérique"},
    {"ville": "Suva", "pays": "Fidji", "continent": "Océanie"},
    {"ville": "Port Moresby", "pays": "Papouasie-Nouvelle-Guinée", "continent": "Océanie"},
    {"ville": "Nukuʻalofa", "pays": "Tonga", "continent": "Océanie"},
    {"ville": "Téhéran", "pays": "Iran", "continent": "Moyen-Orient"},
    {"ville": "Bagdad", "pays": "Irak", "continent": "Moyen-Orient"},
    {"ville": "Doha", "pays": "Qatar", "continent": "Moyen-Orient"},
]

# Préparer le dossier de sauvegarde (commune à toutes les villes)
date_folder = datetime.now().strftime("%Y-%m-%d")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw", date_folder)
os.makedirs(DATA_DIR, exist_ok=True)

# Boucle sur chaque ville
for city in cities:
    try:
        nom_ville = city["ville"]
        url = f"http://api.openweathermap.org/data/2.5/weather?q={nom_ville}&appid={API_KEY}&units=metric"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        temperature = data['main']['temp']
        humidite = data['main']['humidity']
        pression = data['main']['pressure']
        description = data['weather'][0]['description']
        date_extraction = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        weather_data = {
            'ville': nom_ville,
            'pays': city['pays'],
            'continent': city['continent'],
            'date_extraction': date_extraction,
            'temperature': temperature,
            'humidite': humidite,
            'pression': pression,
            'description': description
        }

        # Affichage console
        print(f"\n--- Données météo pour {nom_ville} ---")
        print(f"Pays : {city['pays']}")
        print(f"Continent : {city['continent']}")
        print(f"Température : {temperature}°C")
        print(f"Humidité : {humidite}%")
        print(f"Pression : {pression} hPa")
        print(f"Météo : {description}")
        print(f"Date extraction : {date_extraction}")

        # Sauvegarde du fichier CSV pour chaque ville
        safe_filename = nom_ville.replace(" ", "_")  # ex: "New Delhi" → "New_Delhi"
        pd.DataFrame([weather_data]).to_csv(
            os.path.join(DATA_DIR, f"meteo_{safe_filename}.csv"),
            index=False
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {nom_ville}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {nom_ville}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {nom_ville}: {str(e)}")
