import os
import pandas as pd
from datetime import datetime

# Définir les chemins
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
date_folder = datetime.now().strftime("%Y-%m-%d")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", date_folder)
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed", date_folder)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Initialiser une liste pour stocker les DataFrames
all_data = []


# Charger chaque fichier CSV dans un DataFrame et les stocker
for file in os.listdir(RAW_DIR):
    if file.endswith(".csv"):
        file_path = os.path.join(RAW_DIR, file)
        df = pd.read_csv(file_path)
        all_data.append(df)
      
# Fusionner tous les DataFrames en un seul
df_combined = pd.concat(all_data, ignore_index=True)
df_combined.set_index("ville", inplace = True)
print(df_combined)

# Sauvegarder le DataFrame transformé
output_path = os.path.join(PROCESSED_DIR, "meteo_combined.csv")
df_combined.to_csv(output_path, index=False)

print(f"✅ Données transformées et sauvegardées dans {output_path}")
