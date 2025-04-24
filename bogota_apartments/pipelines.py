"""
This module defines the pipelines for the bogota_apartments Scrapy project. It contains a MongoDBPipeline class that
handles the processing of items and their storage in a MongoDB database.

Classes:
    MongoDBPipeline: A class that handles the processing of items and their storage in a MongoDB database.
"""

# useful for handling different item types with a single interface
from bogota_apartments.items import ApartmentsItem
from scrapy.exceptions import DropItem, NotConfigured
from scrapy.utils.project import get_project_settings
from datetime import datetime
import logging
import pymongo

# Logger a nivel de módulo (opcional, pero si lo usas consistentemente está bien)
# O puedes usar el self.logger definido en __init__ en todo el módulo.
# logger = logging.getLogger(__name__) # Ya lo tienes en __init__


class MongoDBPipeline(object):
    """
    A class that handles the processing of items and their storage in a MongoDB database.

    Attributes:
        collection (str): The name of the collection in the MongoDB database.
        mongo_uri (str): The URI of the MongoDB instance.
        mongo_db (str): The name of the MongoDB database.

    Methods:
        from_crawler(cls, crawler): Returns an instance of the class with the specified URI and database name.
        open_spider(self, spider): Initializes the MongoDB client and database.
        close_spider(self, spider): Closes the MongoDB client.
        process_item(self, item, spider): Processes the item and stores it in the MongoDB database.
    """

    collection = get_project_settings().get('MONGO_COLLECTION_RAW')

    def __init__(self, mongo_uri, mongo_db):
        """
        Initializes a new instance of the MongoDBPipeline class.

        Args:
            mongo_uri (str): The URI of the MongoDB instance.
            mongo_db (str): The name of the MongoDB database.

        """
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.logger = logging.getLogger(__name__)
        self.client = None
        self.db = None
        self.enabled = True  # Asume habilitado hasta que open_spider falle o se deshabilite

    @classmethod
    def from_crawler(cls, crawler):
        """
        Returns an instance of the class with the specified URI and database name.

        Args:
            crawler (scrapy.crawler.Crawler): The Scrapy crawler.

        Returns:
            MongoDBPipeline: An instance of the MongoDBPipeline class.
        """
        mongo_uri = crawler.settings.get('MONGO_URI')
        mongo_db = crawler.settings.get('MONGO_DATABASE')

        # 3. Chequeo de configuración esencial
        if not mongo_uri or not mongo_db:
            # Lanza NotConfigured si falta la URI o DB. Scrapy desactivará el pipeline.
            raise NotConfigured(
                "MongoDB settings (MONGO_URI, MONGO_DATABASE) not found.")

        # 4. Verifica si el pipeline está explícitamente deshabilitado en ITEM_PIPELINES
        # (Scrapy maneja esto, pero es bueno saberlo)
        # Si MongoDBPipeline no está en ITEM_PIPELINES, esta clase ni siquiera se instanciará.

        return cls(
            mongo_uri=mongo_uri,
            mongo_db=mongo_db
        )

    def open_spider(self, spider):
        """
        Initializes the MongoDB client and database.

        Args:
            spider (scrapy.Spider): The Scrapy spider.
        """
        # 5. Intenta conectar solo si las settings estaban presentes (pasó from_crawler)
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            # Forzar conexión/autenticación para detectar errores temprano
            self.client.admin.command('ping')
            self.db = self.client[self.mongo_db]
            self.logger.info(
                f"MongoDB connection opened for spider '{spider.name}'. DB: '{self.mongo_db}'.")
            self.enabled = True
        except pymongo.errors.ConnectionFailure as e:
            self.logger.error(
                f"Failed to connect to MongoDB at {self.mongo_uri}. Pipeline DISABLED. Error: {e}")
            # 6. Deshabilita el pipeline si la conexión falla al inicio
            self.enabled = False
            self.client = None  # Asegurarse que no haya cliente
            self.db = None
        except Exception as e:  # Captura otros posibles errores de conexión/autenticación
            self.logger.error(
                f"Unexpected error connecting to MongoDB. Pipeline DISABLED. Error: {e}", exc_info=True)
            self.enabled = False
            self.client = None
            self.db = None

    def close_spider(self, spider):
        """
        Closes the MongoDB client.

        Args:
            spider (scrapy.Spider): The Scrapy spider.
        """
        # 7. Cierra la conexión solo si se estableció
        if self.client:
            self.client.close()
            self.logger.info(
                f"MongoDB connection closed for spider '{spider.name}'.")

    def process_item(self, item, spider):
        """
        Processes the item and stores it in the MongoDB database if enabled.

        Args:
            item (scrapy.Item): The Scrapy item.
            spider (scrapy.Spider): The Scrapy spider.

        Returns:
            scrapy.Item: The processed Scrapy item.
        """
        # Verifica si el pipeline está habilitado antes de procesar
        if not self.enabled or self.client is None or self.db is None:
            self.logger.debug(
                f"MongoDB pipeline disabled or connection failed/None. Skipping DB operation for item {item.get('codigo', 'N/A')}.")
            return item

        # Continúa solo si el pipeline está habilitado y conectado
        data = dict(ApartmentsItem(item))
        codigo = data.get('codigo')

        if not codigo:
            self.logger.warning(
                f"Item sin 'codigo' descartado (antes de DB). URL: {data.get('url', 'N/A')}")
            raise DropItem(f"Missing 'codigo' in item: {item}")

        try:
            # Lógica específica por spider
            if spider.name == 'metrocuadrado':
                existing_item = self.db[self.collection].find_one(
                    {'codigo': data['codigo']})
                data['caracteristicas'] = []
                for key in ['featured_interior', 'featured_exterior', 'featured_zona_comun', 'featured_sector']:
                    if key in data:
                        data['caracteristicas'] += data.get(key, [])
                        del data[key]

                if existing_item:
                    # Preparar la actualización del item existente
                    update_data = existing_item
                    update_data['last_view'] = datetime.now()
                    timeline = update_data.setdefault('timeline', [])

                    fields_to_track = ['precio_venta', 'precio_arriendo']
                    price_changed = False
                    for field in fields_to_track:
                        if field in data and data[field] != update_data.get(field):
                            if not timeline:
                                timeline.append({
                                    'fecha': update_data.get('datetime', datetime.now()),
                                    field: update_data.get(field)
                                })
                            timeline.append({
                                'fecha': datetime.now(),
                                field: data[field],
                            })
                            update_data[field] = data[field]
                            price_changed = True
                        elif field not in update_data and field in data:
                            update_data[field] = data[field]
                            price_changed = True

                    # Actualizar otros campos si es necesario
                    update_data['imagenes'] = data.get('imagenes', [])
                    update_data['descripcion'] = data.get('descripcion', '')
                    # ... otros campos ...

                    result = self.db[self.collection].update_one(
                        {'codigo': data['codigo']}, {'$set': existing_item})

                    # --- LOG INFO DESPUÉS DE UPDATE ---
                    if result.modified_count > 0 or price_changed:
                        precio = update_data.get('precio_arriendo') or update_data.get(
                            'precio_venta', 'N/A')
                        tipo_propiedad = data.get('tipo_propiedad', 'N/A')
                        tipo_operacion = data.get('tipo_operacion', 'N/A')
                        self.logger.info(
                            f"Item ACTUALIZADO [Metrocuadrado]: Codigo={codigo}, Precio={precio}, Modificado={result.modified_count > 0}, CambioPrecio={price_changed}, TipoPropiedad={tipo_propiedad}, TipoOperacion={tipo_operacion}")
                    # else: (Opcional: log debug si no hubo cambios)
                    #    self.logger.debug(f"Item sin cambios detectados [Metrocuadrado]: Codigo={codigo}")

                else:
                    # --- LOG INFO DESPUÉS DE INSERT ---
                    data['datetime'] = data.get('datetime', datetime.now())
                    result = self.db[self.collection].insert_one(data)
                    precio = data.get('precio_arriendo') or data.get(
                        'precio_venta', 'N/A')
                    self.logger.info(
                        f"Item INSERTADO [Metrocuadrado]: Codigo={codigo}, Precio={precio}, ID={result.inserted_id}")

            elif spider.name == 'habi':
                existing_item = self.db[self.collection].find_one(
                    {'codigo': data['codigo']})
                if existing_item:
                    update_data = existing_item
                    update_data['last_view'] = datetime.now()
                    timeline = update_data.setdefault('timeline', [])
                    price_changed = False

                    field = 'precio_venta'
                    if field in data and data[field] != update_data.get(field):
                        if not timeline:
                            timeline.append({
                                'fecha': update_data.get('datetime', datetime.now()),
                                field: update_data.get(field)
                            })
                        timeline.append({
                            'fecha': datetime.now(),
                            field: data[field],
                        })
                        update_data[field] = data[field]
                        price_changed = True
                    elif field not in update_data and field in data:
                        update_data[field] = data[field]
                        price_changed = True

                    # ... otros campos a actualizar para Habi ...

                    result = self.db[self.collection].update_one(
                        {'codigo': data['codigo']}, {'$set': update_data})

                    # --- LOG INFO DESPUÉS DE UPDATE ---
                    if result.modified_count > 0 or price_changed:
                        precio = update_data.get('precio_venta', 'N/A')
                        tipo_propiedad = data.get('tipo_propiedad', 'N/A')
                        tipo_operacion = data.get('tipo_operacion', 'N/A')
                        self.logger.info(
                            f"Item ACTUALIZADO [Habi]: Codigo={codigo}, Precio={precio}, Modificado={result.modified_count > 0}, CambioPrecio={price_changed}, TipoPropiedad={tipo_propiedad}, TipoOperacion={tipo_operacion}")
                    # else: (Opcional: log debug si no hubo cambios)
                    #    self.logger.debug(f"Item sin cambios detectados [Habi]: Codigo={codigo}")

                else:
                    # --- LOG INFO DESPUÉS DE INSERT ---
                    data['datetime'] = data.get('datetime', datetime.now())
                    result = self.db[self.collection].insert_one(data)
                    precio = data.get('precio_venta', 'N/A')
                    self.logger.info(
                        f"Item INSERTADO [Habi]: Codigo={codigo}, Precio={precio}, ID={result.inserted_id}")

            elif spider.name == 'metrocuadrado_search':
                # No guarda, solo retorna
                self.logger.debug(
                    f"Item pasado sin guardar [metrocuadrado_search]: Codigo={codigo}")
                # No hay operación DB, retorna directamente el item

            else:  # Caso por defecto o para otros spiders no especificados
                # --- LOG INFO DESPUÉS DE INSERT (Fallback) ---
                data['datetime'] = data.get('datetime', datetime.now())
                result = self.db[self.collection].insert_one(data)
                precio = data.get('precio_arriendo') or data.get(
                    'precio_venta', 'N/A')
                self.logger.info(
                    f"Item INSERTADO [{spider.name}]: Codigo={codigo}, Precio={precio}, ID={result.inserted_id}")

            # Si todo va bien dentro del try, devuelve el item
            return item

        # Manejo de errores específicos y generales
        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Error de MongoDB procesando item {codigo} para spider {spider.name}: {e}", exc_info=True)
            raise DropItem(f"Database error processing item {codigo}: {e}")
        except KeyError as e:
            self.logger.error(
                f"KeyError procesando item {codigo} para spider {spider.name}: Falta la clave {e}", exc_info=True)
            raise DropItem(f"Missing key {e} in item {codigo}")
        except Exception as e:
            self.logger.error(
                f"Error inesperado procesando item {codigo} para spider {spider.name}: {e}", exc_info=True)
            raise DropItem(f"Unexpected error processing item {codigo}: {e}")
