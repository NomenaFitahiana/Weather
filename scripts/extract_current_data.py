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

city = "Antananarivo"
url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

try:
    response = requests.get(url)
    response.raise_for_status()  # Lève une exception pour les codes != 200

    data = response.json()
    
    temperature = data['main']['temp']
    humidite = data['main']['humidity']
    pression = data['main']['pressure']
    description = data['weather'][0]['description']
    date_extraction = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    weather_data = {
        'ville': city,
        'date_extraction': date_extraction,
        'temperature': temperature,
        'humidite': humidite,
        'pression': pression,
        'description': description
    }

    print(f"Ville : {city}")
    print(f"Température : {temperature}°C") 
    print(f"Humidité : {humidite}%")
    print(f"Pression : {pression} hPa")
    print(f"Météo : {description}")
    print(f"Date_extraction: {date_extraction}")

    # Dossier de sauvegarde (nommé par date), dans weather/data/raw/YYYY-MM-DD
    date_folder = datetime.now().strftime("%Y-%m-%d")
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # -> weather/
    DATA_DIR = os.path.join(BASE_DIR, "data", "raw", date_folder)

    os.makedirs(DATA_DIR, exist_ok=True)

    pd.DataFrame([weather_data]).to_csv(
        os.path.join(DATA_DIR, f"meteo_{city}.csv"),
        index=False
    )

    success = True

except requests.exceptions.RequestException as e:
    logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    success = False
except KeyError as e:
    logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    success = False
except Exception as e:
    logging.error(f"Erreur inattendue pour {city}: {str(e)}")
    success = False

# Facultatif : retourner une valeur si dans une fonction
# return success
