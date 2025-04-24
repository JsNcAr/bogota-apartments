from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import pymongo
import logging
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# --- NUEVA Configuración del Logger ---

# Nombre del logger (más específico que el root logger)
logger_name = 'data_save_script'
logger = logging.getLogger(logger_name)

# Establecer el nivel MÍNIMO que este logger procesará.
# ¡¡CAMBIO CLAVE!! Cambiamos de DEBUG a INFO. Esto filtrará los logs DEBUG de pymongo.
logger.setLevel(logging.INFO)

# Evitar que los mensajes se propaguen al logger raíz (si existiera y tuviera handlers)
logger.propagate = False

# Crear la carpeta de logs si no existe
log_directory = "logs/04_data_save_logs"
os.makedirs(log_directory, exist_ok=True)

# Nombre del archivo de log basado en el mes actual
current_month = datetime.now().strftime("%Y-%m")
log_filename = os.path.join(log_directory, f"data_save_{current_month}.log")

# Crear el handler para escribir en el archivo mensual
file_handler = logging.FileHandler(log_filename, encoding='utf-8')

# Formato del log (puedes mantener el tuyo o ajustarlo)
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)

# Añadir el handler al logger (SOLO si no tiene ya)
if not logger.handlers:
    logger.addHandler(file_handler)
    # Opcional: Si también quieres ver los logs en consola mientras corre
    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # logger.addHandler(stream_handler)

# --- Fin Nueva Configuración ---

# Cambiar el directorio de trabajo si es necesario
if os.path.basename(os.getcwd()) == 'ETL':
    logging.info('Changing working directory')
    os.chdir('..')

# Iniciar el proceso y registrar el inicio
logging.info(f'Process started at {datetime.now()}')

try:
    # Conectar a MongoDB
    logging.info('Connecting to MongoDB')
    client = pymongo.MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DATABASE')]
    collection = db['scrapy_bogota_apartments_processed']

    # Ruta al archivo de datos procesados
    PROCESSED_DATA = 'data/processed/apartments.csv'

    # Leer los datos procesados desde el archivo CSV
    logging.info('Reading the processed data')
    df = pd.read_csv(PROCESSED_DATA, low_memory=False)
    logging.info('Processed data read successfully')

    # Guardar los datos procesados en MongoDB
    logging.info('Saving the processed data to MongoDB')
    for index, row in df.iterrows():
        apartment = collection.find_one({'codigo': row['codigo']})
        if apartment:
            if apartment != row.to_dict():
                collection.update_one({'codigo': row['codigo']}, {
                                      '$set': row.to_dict()})
        else:
            collection.insert_one(row.to_dict())

    logging.info('Processed data saved successfully')

except FileNotFoundError as e:
    logging.error(f'File not found: {e}')

except pd.errors.EmptyDataError as e:
    logging.error(f'Empty data error: {e}')

except Exception as e:
    logging.error(f'An error occurred: {e}')

finally:
    # Cerrar la conexión a MongoDB
    if 'client' in locals():
        logging.info('Closing the connection to MongoDB')
        client.close()

    logging.info(f'Process finished at {datetime.now()}')
