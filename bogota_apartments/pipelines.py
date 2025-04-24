"""
This module defines the pipelines for the bogota_apartments Scrapy project. It contains a MongoDBPipeline class that
handles the processing of items and their storage in a MongoDB database.

Classes:
    MongoDBPipeline: A class that handles the processing of items and their storage in a MongoDB database.
"""

# useful for handling different item types with a single interface
from bogota_apartments.items import ApartmentsItem
from scrapy.exceptions import DropItem
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
        # Ya tienes el logger inicializado aquí, ¡perfecto!
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Returns an instance of the class with the specified URI and database name.

        Args:
            crawler (scrapy.crawler.Crawler): The Scrapy crawler.

        Returns:
            MongoDBPipeline: An instance of the MongoDBPipeline class.
        """
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        """
        Initializes the MongoDB client and database.

        Args:
            spider (scrapy.Spider): The Scrapy spider.
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        # Log un poco más informativo
        self.logger.info(
            f"MongoDB connection opened for spider '{spider.name}'. Collection: '{self.collection}'.")

    def close_spider(self, spider):
        """
        Closes the MongoDB client.

        Args:
            spider (scrapy.Spider): The Scrapy spider.
        """
        self.client.close()
        self.logger.info(
            f"MongoDB connection closed for spider '{spider.name}'.")

    def process_item(self, item, spider):
        """
        Processes the item and stores it in the MongoDB database.

        Args:
            item (scrapy.Item): The Scrapy item.
            spider (scrapy.Spider): The Scrapy spider.

        Returns:
            scrapy.Item: The processed Scrapy item.
        """
        # Usar dict(item) o ItemAdapter(item).asdict()
        data = dict(ApartmentsItem(item))
        codigo = data.get('codigo')  # Obtener código una vez

        if not codigo:
            self.logger.warning(
                f"Item sin 'codigo' descartado. URL: {data.get('url', 'N/A')}")
            # Opcional: Descartar item
            raise DropItem(f"Missing 'codigo' in item: {item}")

        try:
            if spider.name == 'metrocuadrado':
                # --- Lógica Metrocuadrado ---
                existing_item = self.db[self.collection].find_one(
                    {'codigo': codigo})
                data['caracteristicas'] = []
                for key in ['featured_interior', 'featured_exterior', 'featured_zona_comun', 'featured_sector']:
                    if key in data:
                        # Usar .get con default
                        data['caracteristicas'] += data.get(key, [])
                        del data[key]

                if existing_item:
                    # Preparar la actualización del item existente
                    update_data = existing_item  # Empezar con los datos existentes
                    update_data['last_view'] = datetime.now()
                    timeline = update_data.setdefault(
                        'timeline', [])  # Mejor forma de inicializar

                    # Lógica de Timeline (simplificada para claridad, mantén tu lógica exacta)
                    fields_to_track = ['precio_venta', 'precio_arriendo']
                    price_changed = False
                    for field in fields_to_track:
                        # Comparar con .get
                        if field in data and data[field] != update_data.get(field):
                            if not timeline:  # Si timeline está vacío, añadir estado inicial
                                timeline.append({
                                    # Fecha original o actual
                                    'fecha': update_data.get('datetime', datetime.now()),
                                    field: update_data.get(field)
                                })
                            timeline.append({
                                'fecha': datetime.now(),
                                field: data[field],
                            })
                            # Actualizar el campo principal
                            update_data[field] = data[field]
                            price_changed = True
                        elif field not in update_data and field in data:
                            # Si el campo no existía y ahora sí, añadirlo
                            update_data[field] = data[field]
                            price_changed = True  # Considerar esto un cambio si es relevante

                    # Otros campos a actualizar siempre (ejemplo)
                    update_data['imagenes'] = data.get('imagenes', [])
                    update_data['descripcion'] = data.get('descripcion', '')
                    # ... otros campos que quieras refrescar ...

                    # Actualiza el item en la base de datos
                    result = self.db[self.collection].update_one(
                        {'codigo': codigo}, {'$set': update_data})

                    # --- AÑADIR LOG INFO DESPUÉS DE UPDATE ---
                    # Log si se modificó o si cambió el precio (timeline)
                    if result.modified_count > 0 or price_changed:
                        precio = update_data.get('precio_arriendo') or update_data.get(
                            'precio_venta', 'N/A')
                        self.logger.info(
                            f"Item ACTUALIZADO [Metrocuadrado]: Codigo={codigo}, Precio={precio}, Modificado={result.modified_count > 0}, CambioPrecio={price_changed}")
                    else:
                        # Log opcional si no hubo cambios detectados
                        # self.logger.debug(f"Item sin cambios detectados [Metrocuadrado]: Codigo={codigo}")
                        pass  # O simplemente no loguear si no hay cambios

                else:
                    # --- AÑADIR LOG INFO DESPUÉS DE INSERT ---
                    # Asegurar fecha de creación
                    data['datetime'] = data.get('datetime', datetime.now())
                    result = self.db[self.collection].insert_one(data)
                    precio = data.get('precio_arriendo') or data.get(
                        'precio_venta', 'N/A')
                    self.logger.info(
                        f"Item INSERTADO [Metrocuadrado]: Codigo={codigo}, Precio={precio}, ID={result.inserted_id}")

                return item  # Devuelve el item original

            elif spider.name == 'habi':
                # --- Lógica Habi ---
                existing_item = self.db[self.collection].find_one(
                    {'codigo': codigo})
                if existing_item:
                    # Preparar la actualización del item existente
                    update_data = existing_item
                    update_data['last_view'] = datetime.now()
                    timeline = update_data.setdefault('timeline', [])
                    price_changed = False

                    # Lógica de Timeline para precio_venta
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

                     # ... otros campos a actualizar siempre para Habi si aplica ...

                    result = self.db[self.collection].update_one(
                        {'codigo': codigo}, {'$set': update_data})

                    # --- AÑADIR LOG INFO DESPUÉS DE UPDATE ---
                    if result.modified_count > 0 or price_changed:
                        precio = update_data.get('precio_venta', 'N/A')
                        self.logger.info(
                            f"Item ACTUALIZADO [Habi]: Codigo={codigo}, Precio={precio}, Modificado={result.modified_count > 0}, CambioPrecio={price_changed}")
                    else:
                        pass  # No loguear si no hay cambios

                else:
                    # --- AÑADIR LOG INFO DESPUÉS DE INSERT ---
                    data['datetime'] = data.get('datetime', datetime.now())
                    result = self.db[self.collection].insert_one(data)
                    precio = data.get('precio_venta', 'N/A')
                    self.logger.info(
                        f"Item INSERTADO [Habi]: Codigo={codigo}, Precio={precio}, ID={result.inserted_id}")

                return item  # Devuelve el item original

            elif spider.name == 'metrocuadrado_search':
                # No guarda, solo retorna, no necesita log de guardado
                # DEBUG por si quieres verlo
                self.logger.debug(
                    f"Item pasado sin guardar [metrocuadrado_search]: Codigo={codigo}")
                return item

            else:  # Caso por defecto o para otros spiders no especificados
                # --- AÑADIR LOG INFO DESPUÉS DE INSERT (Fallback) ---
                # Asumiendo inserción simple para cualquier otro spider
                data['datetime'] = data.get('datetime', datetime.now())
                result = self.db[self.collection].insert_one(data)
                precio = data.get('precio_arriendo') or data.get(
                    'precio_venta', 'N/A')
                self.logger.info(
                    f"Item INSERTADO [{spider.name}]: Codigo={codigo}, Precio={precio}, ID={result.inserted_id}")
                return item

        except pymongo.errors.PyMongoError as e:
            self.logger.error(
                f"Error de MongoDB procesando item {codigo} para spider {spider.name}: {e}", exc_info=True)
            # Decide si descartar el item o relanzar el error
            raise DropItem(f"Database error processing item {codigo}: {e}")
        except KeyError as e:
            self.logger.error(
                f"KeyError procesando item {codigo} para spider {spider.name}: Falta la clave {e}", exc_info=True)
            raise DropItem(f"Missing key {e} in item {codigo}")
        except Exception as e:
            self.logger.error(
                f"Error inesperado procesando item {codigo} para spider {spider.name}: {e}", exc_info=True)
            raise DropItem(f"Unexpected error processing item {codigo}: {e}")
