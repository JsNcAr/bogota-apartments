"""
This script connects to a MongoDB database, retrieves data, performs some transformations on it, and saves the transformed data to a CSV file. 

The script first connects to a MongoDB database using the MONGO_URI and MONGO_DATABASE environment variables. It then retrieves data from the 'scrapy_bogota_apartments' collection and stores it in a pandas DataFrame. 

The script then performs two transformations on the data. First, it explodes the 'imagenes' column and saves the resulting DataFrame to a CSV file. Second, it extracts several features from the 'featured_interior', 'featured_zona_comun', 'featured_exterior', and 'featured_sector' columns and saves the resulting DataFrame to a CSV file.

The resulting CSV files are saved in the 'data/processed' and 'data/interim' directories, respectively.

The script requires the following packages to be installed: src, dotenv, logging, pandas, pymongo, os.
"""
from src import extract_features
from dotenv import load_dotenv
import logging
import pandas as pd
import pymongo
import os

load_dotenv()

filename = f'logs/01_initial_transformations.log'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)

# verificar si etoy dentro de la carpeta notebooks o no
if os.getcwd().split('/')[-1] == 'ETL':
    os.chdir('..')

# Connect to MongoDB
logging.info('Connecting to MongoDB')

try:
    client = pymongo.MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DATABASE')]
    collection = db[os.getenv('MONGO_COLLECTION_RAW')]
    logging.info('Connected to MongoDB')

except pymongo.errors.ConnectionFailure as error:
    logging.error(error)
    exit(1)

# Get data from MongoDB
df = pd.DataFrame(list(collection.find()))
df = df.drop(columns=['_id'], axis=1)

# Transformations
logging.info('Transforming data (explode images)')
images_explode = df.explode('imagenes')
images_explode = images_explode.dropna(subset=['imagenes'])

images_df = images_explode[['codigo', 'imagenes']].rename(columns={'imagenes': 'url_imagen'})
images_df.to_csv('data/processed/images.csv', index=False)
logging.info(f'Images saved, shape: {images_df.shape}')

df = df.drop(columns=['imagenes'], axis=1)

logging.info('Transforming data (extract features)')
df['jacuzzi'] = df.caracteristicas.apply(extract_features.check_jacuzzi)
df['piso'] = df.caracteristicas.apply(extract_features.extract_piso)
df['closets'] = df.caracteristicas.apply(extract_features.extract_closets)
df['chimenea'] = df.caracteristicas.apply(extract_features.check_chimeny)
df['permite_mascotas'] = df.caracteristicas.apply(extract_features.check_mascotas)
df['gimnasio'] = df.caracteristicas.apply(extract_features.check_gimnasio)
df['ascensor'] = df.caracteristicas.apply(extract_features.check_ascensor)
df['conjunto_cerrado'] = df.caracteristicas.apply(extract_features.check_conjunto_cerrado)
df['piscina'] = df.caracteristicas.apply(extract_features.check_piscina)
df['salon_comunal'] = df.caracteristicas.apply(extract_features.check_salon_comunal)
df['terraza'] = df.caracteristicas.apply(extract_features.check_terraza)
df['vigilancia'] = df.caracteristicas.apply(extract_features.check_vigilancia)


df = df.drop(columns=['caracteristicas'], axis=1)

df.to_csv('data/interim/apartments.csv', index=False)
logging.info(f'Data saved, shape: {df.shape}')