import datetime
import hashlib
import random
import time

import prox  # Импорт списка прокси из prox.py
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# Настройка подключения к MongoDB
client = MongoClient('localhost', 27017)
db = client['ARBKLE']
collection = db['KASPI']


def get_html_hash(driver):
    """Вычисляет хэш HTML содержимого страницы."""
    return hashlib.sha256(driver.page_source.encode('utf-8')).hexdigest()


# Функция для получения настроек прокси
def get_chrome_options(proxy_address):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Запуск Chrome в фоновом режиме
    chrome_options.add_argument(f'--proxy-server={proxy_address}')
    return chrome_options


# Выбор случайного прокси из списка
proxy_ip = random.choice(prox.PROXIES)

# Получение опций Chrome с настройками прокси
chrome_options = get_chrome_options(proxy_ip)

# Использование ChromeDriverManager для автоматического управления драйвером
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

wait = WebDriverWait(driver, 10)

# Чтение данных из файла 'cat'
categories_data = []
with open('cat', 'r', encoding='utf-8') as file:
    for line in file:
        link, title = line.strip().split(': ')
        categories_data.append({"link": link, "title": title})

# Обработка каждой категории из прочитанных данных
for category in categories_data:
    category_title = category['title']
    category_link = category['link']

    try:
        driver.get(category_link)
        last_successful_parse_time = datetime.datetime.now()
        last_page_html_hash = get_html_hash(driver)

        while True:
            try:
                wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'item-card')))
                html = driver.page_source
                soup = bs(html, 'html.parser')
                cards = soup.find_all('div', class_='item-card')

                if not cards and (datetime.datetime.now() - last_successful_parse_time).seconds > 20:
                    break

                for card in cards:
                    name_link = card.find('a', class_='item-card__name-link')
                    name = name_link.text.strip() if name_link else 'Нет названия'
                    product_url = name_link['href'] if name_link and name_link.has_attr('href') else 'Нет ссылки'

                    if collection.count_documents({'link': product_url}) == 0:
                        price = card.find('span', class_='item-card__prices-price').text.strip() if card.find('span',
                                                                                                              class_='item-card__prices-price') else 'Нет цены'
                        image_tag = card.find('img', class_='item-card__image')
                        image_url = image_tag['src'] if image_tag and image_tag.has_attr('src') else 'Нет изображения'

                        now = datetime.datetime.now()
                        formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")

                        collection.insert_one({
                            'name': name,
                            'price': price,
                            'image_url': image_url,
                            'product_url': product_url,
                            'category': category_title,
                            'parsed_time': formatted_date
                        })

                # Случайная задержка перед переходом на следующую страницу
                time.sleep(random.uniform(2, 7))

                next_button = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//li[contains(@class, 'pagination__el') and contains(., 'Следующая')]")))
                next_button.click()

                # Даем странице время для обновления
                time.sleep(random.uniform(2, 7))

                new_page_html_hash = get_html_hash(driver)
                if new_page_html_hash == last_page_html_hash:
                    break
                else:
                    last_page_html_hash = new_page_html_hash

            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                break
            except Exception as e:
                break

    except Exception as e:
        pass

time.sleep(300)  # Задержка перед началом нового цикла парсинга.
