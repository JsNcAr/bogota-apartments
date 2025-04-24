from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from selenium import webdriver
from datetime import datetime
import re
import json

# Scrapy
from bogota_apartments.items import ApartmentsItem
from scrapy.selector import Selector
from scrapy.loader import ItemLoader
import scrapy
import logging


class MetrocuadradoSpider(scrapy.Spider):
    """
    Spider to scrape apartment data from metrocuadrado.com
    """
    name = 'metrocuadrado'
    allowed_domains = ['metrocuadrado.com']
    base_url = 'https://www.metrocuadrado.com/rest-search/search'
    logger = logging.getLogger(__name__)

    def __init__(self):
        """
        Initializes the spider with a headless Chrome browser instance
        """
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--window-size=1920x1080')
        chrome_options.add_argument(f'user-agent={UserAgent().random}')
        chrome_options.add_argument('--disk-cache=true')

        self.driver = webdriver.Chrome(options=chrome_options,)

    def start_requests(self):
        """
        Generates the initial requests to scrape apartment data
        """
        headers = {
            'X-Api-Key': 'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl',
            'User-Agent': UserAgent().random
        }

        for type in ['venta', 'arriendo']:
            for offset in range(0, 9950, 50):
                self.logger.info(
                    f'Getting {type} apartments from offset {offset}')
                url = f'{self.base_url}?realEstateTypeList=apartamento&realEstateBusinessList={type}&city=bogot%C3%A1&from={offset}&size=50'

                yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response,):
        """
        Parses the response from the initial requests and generates requests to scrape apartment details
        """
        logging.info('Parsing response')
        result = json.loads(response.body)['results']
        self.logger.info(f'Found {len(result)} apartments')

        for item in result:
            yield scrapy.Request(
                url=f'https://metrocuadrado.com{item["link"]}',
                callback=self.details_parse
            )

    def details_parse(self, response):
        # api_item = response.meta.get('api_item', {}) # Descomenta si pasas datos desde parse
        self.driver.get(response.url)
        # <-- Log útil: inicio
        self.logger.info(f'Getting details from {response.url}')

        page_source = self.driver.page_source
        all_script_texts = Selector(text=page_source).xpath(
            '//script/text()').getall()

        if not all_script_texts:
            self.logger.error('No script data found')
            self.driver.get(response.url)
            self.driver.implicitly_wait(5)
            all_script_texts = Selector(text=page_source).xpath(
                '//script/text()').getall()

        item_data = None
        script_analyzed_count = 0
        found_relevant_script = False

        # Quitado: Log verboso de cuántos scripts analizar
        # self.logger.info(f"Analyzing {len(all_script_texts)} script tags...")

        for i, script_content in enumerate(all_script_texts):
            # Filtro previo (sin log verboso)
            if 'self.__next_f.push' in script_content and '\\"data\\"' in script_content:
                found_relevant_script = True
                script_analyzed_count += 1
                # Quitado: Log de que se encontró script relevante y se aplica lógica

                try:
                    # --- LÓGICA DE EXTRACCIÓN con delimitador ']]}],' ---
                    start_key = '\\"data\\":'
                    start_key_pos = script_content.find(start_key)
                    if start_key_pos == -1:
                        self.logger.warning(
                            f"Script {i}: Start key '{start_key}' not found.")
                        continue

                    start_brace_pos = script_content.find(
                        '{', start_key_pos + len(start_key))
                    if start_brace_pos == -1:
                        self.logger.warning(
                            f"Script {i}: Opening brace '{{' not found after key.")
                        continue

                    end_pattern = '}]]}],'
                    end_pattern_pos = script_content.find(
                        end_pattern, start_brace_pos)
                    if end_pattern_pos == -1:
                        continue

                    target_json_str = script_content[start_brace_pos: end_pattern_pos]

                    target_json_str = target_json_str.strip()

                    if not target_json_str.endswith('}'):
                        self.logger.warning(
                            f"Extracted substring for {response.url} doesn't end with '}}'. Might be incomplete.")
                        continue  # Saltar si no parece terminar bien

                    # Intentar desescapar y parsear
                    unescaped_str = target_json_str.replace('\\"', '"')
                    # Quitado: Log de intento de parseo con repr()

                    item_data = json.loads(unescaped_str)  # Parsear

                    # Solo necesitamos verificar si el parseo funcionó y contiene el ID
                    if 'propertyId' in item_data:
                        # <-- Log útil: éxito
                        self.logger.info(
                            f"Successfully parsed JSON for propertyId: {item_data.get('propertyId')}")
                        break  # Éxito, salir del bucle
                    else:
                        # Parseó pero falta ID, es un problema raro
                        item_data = None
                        self.logger.warning(
                            f"Script {i}: Parsed JSON for {response.url}, but 'propertyId' key is missing.")

                except json.JSONDecodeError as e:
                    # Mantenemos el error si falla el parseo, es importante
                    self.logger.error(
                        f"Script {i}: Failed to decode extracted JSON for {response.url}: {e}.")
                    # Quitado: Logs detallados con repr() de los strings fallidos
                    item_data = None
                except Exception as e:
                    # Mantenemos errores inesperados
                    self.logger.error(
                        f"Unexpected error during substring extraction/parsing for script {i} in {response.url}: {e}", exc_info=True)
                    item_data = None

            if item_data:  # Salir del bucle for si ya encontramos los datos
                break

        # Log final si no se encontró en ningún script relevante
        if not item_data and script_analyzed_count > 0:
            self.logger.error(
                f"Analyzed {script_analyzed_count} relevant scripts but failed to extract valid 'data' JSON for {response.url}.")
        elif not item_data and script_analyzed_count == 0:
            self.logger.info(
                f"Analyzed {script_analyzed_count} scripts but none were relevant for {response.url}.")
            self.logger.error(
                f"No relevant scripts (containing 'self.__next_f.push' and 'propertyId') found for {response.url}.")

        # Procesar si encontramos el diccionario item_data
        if item_data:
            try:
                loader = ItemLoader(item=ApartmentsItem(),
                                    selector=Selector(text=''))
                # codigo
                loader.add_value('codigo', item_data.get('propertyId'))
                # tipo_propiedad
                loader.add_value('tipo_propiedad',
                                 item_data.get('propertyType',
                                               {}).get('nombre'))
                # tipo_operacion
                loader.add_value('tipo_operacion',
                                 item_data.get('businessType'))
                # precio_venta
                loader.add_value('precio_venta',
                                 item_data.get('salePrice'))
                # precio_arriendo
                loader.add_value('precio_arriendo',
                                 item_data.get('rentPrice'))
                # area
                loader.add_value('area', item_data.get('area'))
                # habitaciones
                loader.add_value('habitaciones',
                                 item_data.get('rooms'))
                # banos
                loader.add_value('banos', item_data.get('bathrooms'))
                # administracion
                loader.add_value('administracion',
                                 item_data.get('detail', {}).get('adminPrice'))
                # parqueaderos
                loader.add_value('parqueaderos',
                                 item_data.get('garages'))
                # sector
                loader.add_value('sector', self.try_get(
                    item_data, ['sector', 'nombre']))
                # estrato
                loader.add_value('estrato',
                                 item_data.get('stratum'))
                # antiguedad
                loader.add_value('antiguedad',
                                 item_data.get('builtTime'))
                # estado
                loader.add_value('estado',
                                 item_data.get('propertyState'))
                # longitud
                loader.add_value('longitud',
                                 item_data.get('coordinates', {}).get('lon'))
                # latitud
                loader.add_value('latitud',
                                 item_data.get('coordinates', {}).get('lat'))
                # featured_interior
                loader.add_value('featured_interior', self.try_get(
                    item_data, ['featured', 0, 'items']))
                # featured_exterior
                loader.add_value('featured_exterior', self.try_get(
                    item_data, ['featured', 1, 'items']))
                # featured_zona_comun
                loader.add_value('featured_zona_comun', self.try_get(
                    item_data, ['featured', 2, 'items']))
                # featured_sector
                loader.add_value('featured_sector', self.try_get(
                    item_data, ['featured', 3, 'items']))
                # Imagenes
                # Obtener lista o lista vacía
                imagenes_list = item_data.get('images', [])
                imagenes_urls = [img.get('image') for img in imagenes_list if isinstance(
                    img, dict) and img.get('image')]
                if imagenes_urls:  # Añadir solo si se encontraron URLs
                    loader.add_value('imagenes', imagenes_urls)
                # compania
                loader.add_value(
                    'compañia', item_data.get('companyName'))
                # descripcion
                loader.add_value('descripcion', item_data.get('comment'))
                # website
                loader.add_value('website', 'metrocuadrado.com')
                # last_view
                loader.add_value('last_view', datetime.now())
                # datetime
                loader.add_value('datetime', datetime.now())

                yield loader.load_item()
            except KeyError as e:
                self.logger.error(
                    f"KeyError processing ItemLoader for {item_data.get('propertyId', 'N/A')}. Missing key: {e}", exc_info=True)
            except Exception as e:
                self.logger.error(
                    f"Unexpected error processing ItemLoader for {item_data.get('propertyId', 'N/A')}: {e}", exc_info=True)
        else:
            # El log de error ya se generó antes
            self.logger.error(
                f"Final check: Could not extract data for {response.url}. Skipping item.")

    def try_get(self, dictionary, keys: list):
        """
        Tries to get a value from a nested data structure and returns None if the key is not found or if an index is out of range.
        """
        try:
            value = dictionary
            for key in keys:
                if isinstance(value, list) and isinstance(key, int) and 0 <= key < len(value):
                    value = value[key]
                elif isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None  # Key or index is not valid
            return value
        except (KeyError, TypeError, IndexError):
            return None  # Key or index is not valid
