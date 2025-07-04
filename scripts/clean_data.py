import pandas as pd
import os

def clean_weather_data():
    # Charger les données
    df = pd.read_csv("/home/noums/airflow/dags/weather/data/processed/weather_data_combined.csv", encoding='utf-8')

    # Vérification initiale des données
    print("Dimensions initiales du DataFrame :", df.shape)
    print("Valeurs manquantes avant nettoyage :")
    print(df.isnull().sum())

    # Suppression de la colonne 'description'
    df = df.drop(columns=['description'], errors='ignore')  # errors='ignore' au cas où la colonne n'existe pas

    # Standardisation des colonnes textuelles
    df['ville'] = df['ville'].str.strip().str.title()
    df['pays'] = df['pays'].str.strip().str.title()
    df['continent'] = df['continent'].str.strip().str.title()

    # Standardisation des dates
    print("Exemple de valeurs dans 'date' avant conversion :")
    print(df['date'].head(10))
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Vérifier les lignes avec dates invalides avant de les supprimer
    invalid_dates = df[df['date'].isna()]
    if not invalid_dates.empty:
        print("Lignes avec dates invalides :", len(invalid_dates))
        print(invalid_dates.head())
    df = df.dropna(subset=['date'])

    # Imputation des valeurs manquantes
    # 1. Précipitation : moyenne historique par ville
    precip_mean_by_city = df.groupby('ville')['precipitation'].mean()

    # Remplacer les NaN dans 'precipitation' par la moyenne historique de la ville
    df['precipitation'] = df.apply(
        lambda row: precip_mean_by_city[row['ville']] if pd.isna(row['precipitation']) else row['precipitation'],
        axis=1
    )

    # 2. Pression : moyenne des données récentes (à partir du 25 juin 2025)
    recent_date_threshold = '2025-06-25'
    recent_data = df[df['date'] >= recent_date_threshold]
    pression_mean_by_city = recent_data.groupby('ville')['pression'].mean()
    global_pression_mean = recent_data['pression'].mean()  # Moyenne globale des données récentes

    # Remplacer les NaN dans 'pression' par la moyenne des données récentes de la ville, ou la moyenne globale si la ville n'a pas de données récentes
    if 'pression' in df.columns:
        df['pression'] = df.apply(
            lambda row: pression_mean_by_city.get(row['ville'], global_pression_mean) if pd.isna(row['pression']) else row['pression'],
            axis=1
        )

    # Si des valeurs manquantes persistent dans 'pression', imputer par la moyenne historique
    if 'pression' in df.columns:
        historical_pression_mean = df['pression'].mean()  # Moyenne historique comme dernier recours
        df['pression'] = df['pression'].fillna(historical_pression_mean)

    # Imputer les valeurs manquantes restantes dans 'precipitation' par la moyenne globale
    df['precipitation'] = df['precipitation'].fillna(df['precipitation'].mean())

    # Suppression des doublons
    df = df.drop_duplicates()

    # Filtrage des valeurs aberrantes
    df = df[df['temperature'].between(-50, 50)]
    df = df[df['humidite'].between(0, 100)]
    df = df[df['precipitation'] >= 0]
    if 'pression' in df.columns:
        df = df[df['pression'].between(900, 1100)]  # Plage typique pour la pression

    # Vérification finale
    print("Valeurs manquantes après nettoyage :")
    print(df.isnull().sum())
    print("Dimensions du DataFrame après nettoyage :", df.shape)

    # Sauvegarde dans un fichier avec un nom différent pour éviter d'écraser
    output_path = '/home/noums/airflow/dags/weather/data/processed/cleaned_weather_data.csv'
    df.to_csv(output_path, index=False)
    print(f"Fichier sauvegardé à : {os.path.abspath(output_path)}")

if __name__ == "__main__":
    clean_weather_data()