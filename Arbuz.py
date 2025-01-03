import base64
import logging
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service

# Подключение к MongoDB
client = MongoClient('localhost', 27017)
db = client['ARBKLE']
collection = db['ARBUZ']

# Настройка опций Chrome для работы в headless режиме
#chrome_options = Options()
#chrome_options.add_argument("--headless")  # Добавление опции для запуска в фоновом режиме
#chrome_options.add_argument('--no-sandbox')  # Опция для запуска в контейнере или некоторых серверах
# chrome_options.add_argument('--disable-dev-shm-usage')  # Обход ограничений на использование памяти
#chrome_options.add_argument("--remote-debugging-port=9224")
# Инициализация драйвера WebDriver с добавленными опциями
#chrome_options.add_argument("--disable-gpu")
#driver = webdriver.Chrome(options=chrome_options)

options = Options()
options.add_argument("--headless")
options.headless = True  # Добавление опции для запуска в фоновом режиме
geckodriver_path = '/usr/local/bin/geckodriver'
service = Service(executable_path=geckodriver_path)
# Создание экземпляра драйвера Firefox
driver = webdriver.Firefox(service=service, options=options)
logging.basicConfig(level=logging.INFO)
def download_image(image_url):
    if not image_url or image_url == 'null':
        return None
    try:
        response = requests.get(image_url)
        if response.status_code == 200:
            return base64.b64encode(response.content).decode('utf-8')
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке изображения: {e}")
    return None


def parse_category(driver, category_url, category_name):
    # Переход по URL, предоставленному в списке categories
    driver.get(category_url)

    # Явное ожидание, чтобы убедиться, что элементы страницы загрузились
    try:
        # Ожидание появления элемента с классом product-card на странице
        WebDriverWait(driver, 300).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'article.product-item.product-card'))
        )
    except TimeoutException:
        print("Timed out waiting for page to load")
        return  # Прекращение выполнения функции, если страница не загрузилась вовремя

    # Получение исходного кода страницы после загрузки
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # Парсинг элементов страницы
    product_cards = soup.find_all('article', class_='product-item product-card')

    for card in product_cards:
        title_element = card.find('a', class_='product-card__title')
        title = title_element.text.strip() if title_element else "Title not found"
        image_element = card.find('img', class_='product-card__img')
        image_url = image_element.get('data-src') if image_element and image_element.has_attr('data-src') else None
        product_element = card.find('a', class_='product-card__link')
        link = product_element.get('href') if product_element else "Link not found"
        price_element = card.find('span', class_='price--wrapper')
        price = price_element.text.strip() if price_element else "Price not found"
        current_time = datetime.now().strftime("%d.%m.%Y %H.%M")
        print({
                'name': title,
                'price': price,
                'image_url': image_url,
                'category': category_name,
                'link': link,
                'parsed_time': current_time
            })
        # Вставка или обновление данных в MongoDB
        collection.update_one(
            {'link': link},
            {'$set': {
                'name': title,
                'price': price,
                'image_url': image_url,
                'category': category_name,
                'link': link,
                'parsed_time': current_time
            }},
            upsert=True
        )


# Список категорий с соответствующим количеством страниц
categories = [
    ('https://arbuz.kz/ru/almaty/catalog/cat/225164-svezhie_ovoshi_i_frukty', 10, 'Свежие овощи и фрукты'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225161-moloko_syr_maslo_yaica', 17, 'Молочные продукты'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225268-fermerskaya_lavka', 5, 'Фермерская лавка'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225253-kulinariya', 5, 'Кулинария'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225162-myaso_ptica', 4, 'Мясо и птица'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/14-napitki', 36, 'Напитки'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225163-ryba_i_moreprodukty', 5, 'Рыба и морепродукты'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225165-hleb_vypechka', 2, 'Хлеб и выпечка'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225167-kolbasy', 6, 'Колбасы'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225183-zamorozhennye_produkty', 14, 'Замороженные продукты'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225244-rastitelnye_produkty', 1, 'Растительные продукты'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225168-dlya_gotovki_i_vypechki', 18, 'Для готовки и выпечки'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225166-sladosti', 34, 'Сладости'),
    ('https://arbuz.kz/ru/almaty/catalog/cat/225169-krupy_konservy_sneki', 31, 'Крупы, консервы, снеки')
]

# Бесконечный цикл для перезапуска парсинга
while True:
    try:
        logging.info("Старт Парс")
        for base_url, max_page, category_name in categories:
            base_url_template = base_url + '#/?%5B%7B%22slug%22%3A%22page%22,%22value%22%3A{}%2C%22component%22%3A%22pagination%22%7D%5D'
            for page_number in range(1, max_page + 1):
                logging.info("Старт юрл")
                page_url = base_url_template.format(page_number)

                # Переход на страницу.
                driver.get(page_url)
                time.sleep(20)  # Кратковременная пауза.

                # Принудительное выполнение перезагрузки страницы с помощью JavaScript.
                driver.execute_script("window.location.reload();")
                try:
                    logging.info("Получение страницы")
                    main_page = driver.window_handles[0]
                    driver.switch_to.window(main_page)
                    WebDriverWait(driver, 300).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'article.product-item.product-card'))
                    )
                    logging.info("Парс категории в странице")
                    # Парсинг страницы после полной загрузки.
                    parse_category(driver, page_url, category_name)
                    logging.info("Конец парса")
                except TimeoutException:
                    continue
        logging.info("Сон 300сек")
        time.sleep(300)  # Задержка перед началом нового цикла парсинга.
    except Exception:
        logging.info("ОШИБКА")
