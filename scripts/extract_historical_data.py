import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import logging
from retrying import retry
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_data_extraction.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Liste des villes (abrégée pour tester)
cities = [
    {"ville": "Antananarivo", "pays": "Madagascar", "continent": "Afrique", "latitude": -18.8792, "longitude": 47.5079},
    {"ville": "Nairobi", "pays": "Kenya", "continent": "Afrique", "latitude": -1.2864, "longitude": 36.8172},
    {"ville": "Pretoria", "pays": "Afrique du Sud", "continent": "Afrique", "latitude": -25.7313, "longitude": 28.2184},
    {"ville": "Le Caire", "pays": "Égypte", "continent": "Afrique", "latitude": 30.0444, "longitude": 31.2357},
    {"ville": "Paris", "pays": "France", "continent": "Europe", "latitude": 48.8566, "longitude": 2.3522},
    {"ville": "Berlin", "pays": "Allemagne", "continent": "Europe", "latitude": 52.5200, "longitude": 13.4050},
    {"ville": "Londres", "pays": "Royaume-Uni", "continent": "Europe", "latitude": 51.5074, "longitude": -0.1278},
    {"ville": "Rome", "pays": "Italie", "continent": "Europe", "latitude": 41.9028, "longitude": 12.4964},
    {"ville": "Tokyo", "pays": "Japon", "continent": "Asie", "latitude": 35.6762, "longitude": 139.6503},
    {"ville": "Pékin", "pays": "Chine", "continent": "Asie", "latitude": 39.9042, "longitude": 116.4074},
    {"ville": "New Delhi", "pays": "Inde", "continent": "Asie", "latitude": 28.6139, "longitude": 77.2090},
    {"ville": "Riyad", "pays": "Arabie Saoudite", "continent": "Asie", "latitude": 24.7136, "longitude": 46.6753},
    {"ville": "Washington", "pays": "États-Unis", "continent": "Amérique", "latitude": 38.9072, "longitude": -77.0369},
    {"ville": "Ottawa", "pays": "Canada", "continent": "Amérique", "latitude": 45.4215, "longitude": -75.6972},
    {"ville": "Brasília", "pays": "Brésil", "continent": "Amérique", "latitude": -15.8267, "longitude": -47.9218},
    {"ville": "Buenos Aires", "pays": "Argentine", "continent": "Amérique", "latitude": -34.6037, "longitude": -58.3816},
    {"ville": "Canberra", "pays": "Australie", "continent": "Océanie", "latitude": -35.2809, "longitude": 149.1300},
    {"ville": "Wellington", "pays": "Nouvelle-Zélande", "continent": "Océanie", "latitude": -41.2865, "longitude": 174.7762},
    {"ville": "Ankara", "pays": "Turquie", "continent": "Moyen-Orient", "latitude": 39.9334, "longitude": 32.8597},
    {"ville": "Jérusalem", "pays": "Israël", "continent": "Moyen-Orient", "latitude": 31.7683, "longitude": 35.2137},
    {"ville": "Bamako", "pays": "Mali", "continent": "Afrique", "latitude": 12.6392, "longitude": -8.0029},
    {"ville": "Dakar", "pays": "Sénégal", "continent": "Afrique", "latitude": 14.7167, "longitude": -17.4677},
    {"ville": "Abuja", "pays": "Nigeria", "continent": "Afrique", "latitude": 9.0579, "longitude": 7.4951},
    {"ville": "Accra", "pays": "Ghana", "continent": "Afrique", "latitude": 5.6037, "longitude": -0.1870},
    {"ville": "Alger", "pays": "Algérie", "continent": "Afrique", "latitude": 36.7538, "longitude": 3.0588},
    {"ville": "Addis-Abeba", "pays": "Éthiopie", "continent": "Afrique", "latitude": 9.0054, "longitude": 38.7636},
    {"ville": "Madrid", "pays": "Espagne", "continent": "Europe", "latitude": 40.4168, "longitude": -3.7038},
    {"ville": "Oslo", "pays": "Norvège", "continent": "Europe", "latitude": 59.9139, "longitude": 10.7522},
    {"ville": "Vienne", "pays": "Autriche", "continent": "Europe", "latitude": 48.2082, "longitude": 16.3738},
    {"ville": "Athènes", "pays": "Grèce", "continent": "Europe", "latitude": 37.9838, "longitude": 23.7275},
    {"ville": "Bruxelles", "pays": "Belgique", "continent": "Europe", "latitude": 50.8503, "longitude": 4.3517},
    {"ville": "Amsterdam", "pays": "Pays-Bas", "continent": "Europe", "latitude": 52.3676, "longitude": 4.9041},
    {"ville": "Séoul", "pays": "Corée du Sud", "continent": "Asie", "latitude": 37.5665, "longitude": 126.9780},
    {"ville": "Hanoï", "pays": "Vietnam", "continent": "Asie", "latitude": 21.0278, "longitude": 105.8342},
    {"ville": "Bangkok", "pays": "Thaïlande", "continent": "Asie", "latitude": 13.7563, "longitude": 100.5018},
    {"ville": "Manille", "pays": "Philippines", "continent": "Asie", "latitude": 14.5995, "longitude": 120.9842},
    {"ville": "Kuala Lumpur", "pays": "Malaisie", "continent": "Asie", "latitude": 3.1390, "longitude": 101.6869},
    {"ville": "Kaboul", "pays": "Afghanistan", "continent": "Asie", "latitude": 34.5553, "longitude": 69.2075},
    {"ville": "Mexico", "pays": "Mexique", "continent": "Amérique", "latitude": 19.4326, "longitude": -99.1332},
    {"ville": "Lima", "pays": "Pérou", "continent": "Amérique", "latitude": -12.0464, "longitude": -77.0428},
    {"ville": "Santiago", "pays": "Chili", "continent": "Amérique", "latitude": -33.4489, "longitude": -70.6693},
    {"ville": "La Paz", "pays": "Bolivie", "continent": "Amérique", "latitude": -16.4897, "longitude": -68.1193},
    {"ville": "Port-au-Prince", "pays": "Haïti", "continent": "Amérique", "latitude": 18.5944, "longitude": -72.3074},
    {"ville": "La Havane", "pays": "Cuba", "continent": "Amérique", "latitude": 23.1136, "longitude": -82.3666},
    {"ville": "Suva", "pays": "Fidji", "continent": "Océanie", "latitude": -18.1416, "longitude": 178.4419},
    {"ville": "Port Moresby", "pays": "Papouasie-Nouvelle-Guinée", "continent": "Océanie", "latitude": -9.4438, "longitude": 147.1803},
    {"ville": "Nukuʻalofa", "pays": "Tonga", "continent": "Océanie", "latitude": -21.1393, "longitude": -175.2049},
    {"ville": "Téhéran", "pays": "Iran", "continent": "Moyen-Orient", "latitude": 35.6892, "longitude": 51.3890},
    {"ville": "Bagdad", "pays": "Irak", "continent": "Moyen-Orient", "latitude": 33.3152, "longitude": 44.3661},
    {"ville": "Doha", "pays": "Qatar", "continent": "Moyen-Orient", "latitude": 25.2769, "longitude": 51.5200},
]

# Calcul des dates
end_date = datetime(2025, 6, 24)
start_date = end_date - timedelta(days=3*365)  # 3 ans

# Configuration des répertoires
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "historical_data")
os.makedirs(DATA_DIR, exist_ok=True)
output_file = os.path.join(DATA_DIR, "weather_data_for_the_last_3_years.csv")

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def get_weather_data(latitude, longitude, start_date, end_date):
    """Récupère les données météo historiques via l'API Open-Meteo avec retry"""
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": "temperature_2m_mean,relative_humidity_2m_mean,precipitation_sum",
        "timezone": "auto"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Réponse API pour lat={latitude}, lon={longitude}: {data}")
        return data
    except requests.exceptions.HTTPError as err:
        logger.error(f"Erreur HTTP pour lat={latitude}, lon={longitude}: {err} - Réponse: {response.text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur de requête pour lat={latitude}, lon={longitude}: {e}")
        raise

def split_date_range(start_date, end_date, chunk_months=1):
    """Découpe la période en segments de 1 mois pour réduire les erreurs"""
    current_date = start_date
    while current_date < end_date:
        chunk_end = current_date + timedelta(days=30*chunk_months)
        chunk_end = min(chunk_end, end_date)
        yield current_date, chunk_end
        current_date = chunk_end + timedelta(days=1)

def validate_data(data):
    """Valide les données récupérées pour détecter les valeurs manquantes"""
    if not data or "daily" not in data:
        logger.warning("Aucune donnée journalière dans la réponse")
        return False
    required_keys = ["time", "temperature_2m_mean", "relative_humidity_2m_mean", "precipitation_sum"]
    return all(key in data["daily"] for key in required_keys)

all_data = []

for city in cities:
    name = city['ville']
    latitude = city['latitude']
    longitude = city['longitude']
    city_data = []
    
    logger.info(f"Extraction des données pour : {name}")
    
    for chunk_start, chunk_end in split_date_range(start_date, end_date, chunk_months=1):
        logger.info(f"Traitement de la période : {chunk_start.date()} à {chunk_end.date()}")
        
        try:
            weather_data = get_weather_data(latitude, longitude, chunk_start, chunk_end)
            
            if validate_data(weather_data):
                daily_data = weather_data["daily"]
                for i in range(len(daily_data.get("time", []))):
                    city_data.append({
                        "ville": name,
                        "pays": city["pays"],
                        "continent": city["continent"],
                        "date_extraction": daily_data["time"][i],
                        "temperature": daily_data.get("temperature_2m_mean", [None])[i],
                        "humidite": daily_data.get("relative_humidity_2m_mean", [None])[i],
                        "precipitation": daily_data.get("precipitation_sum", [None])[i],
                        "source": "Open-Meteo",
                        "extracted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                logger.info(f"Récupération réussie de {len(daily_data['time'])} jours pour {name}")
            else:
                logger.warning(f"Aucune donnée valide pour {name} pour la période {chunk_start.date()} à {chunk_end.date()}")
        
        except Exception as e:
            logger.error(f"Échec de la récupération des données pour {name} : {str(e)}")
        
        time.sleep(1)  # Respect des limites de l'API
    
    if city_data:
        all_data.extend(city_data)
        logger.info(f"Total pour {name} : {len(city_data)} enregistrements")
    else:
        logger.warning(f"Aucune donnée récupérée pour {name}")

# Sauvegarde des données
if all_data:
    df = pd.DataFrame(all_data)
    df = df.dropna(subset=["temperature", "humidite", "precipitation"], how="all")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logger.info(f"Enregistrement réussi de {len(df)} enregistrements dans {output_file}")
else:
    logger.error("Aucune donnée n'a été récupérée")