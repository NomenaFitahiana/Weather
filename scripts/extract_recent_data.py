import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv 

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("La clé API est introuvable. Vérifie ton fichier .env.")

city = "Antananarivo"

url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"


response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    ville = data['name']
    temperature = data['main']['temp']
    humidite = data['main']['humidity']
    pression = data['main']['pressure']
    description = data['weather'][0]['description']
    print(f"Ville : {ville}")
    print(f"Température : {temperature}°C")
    print(f"Humidité : {humidite}%")
    print(f"Pression : {pression} hPa")
    print(f"Météo : {description}")


    filename = f"meteo_{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"✅ Données sauvegardées dans {filename}")
else:
    print("❌ Erreur :", response.status_code)