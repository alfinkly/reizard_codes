import datetime
import hashlib
import random
import time
import psycopg2
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# Подключение к PostgreSQL
conn = psycopg2.connect(
    dbname="umkabots",
    user="postgres",
    password="W4p_Aspect",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Функция для получения хэша HTML содержимого страницы
def get_html_hash(driver):
    return hashlib.sha256(driver.page_source.encode('utf-8')).hexdigest()

# Функция для получения настроек прокси
def get_chrome_options(proxy_address):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Запуск Chrome в фоновом режиме
    chrome_options.add_argument(f'--proxy-server={proxy_address}')
    return chrome_options

# Выбор случайного прокси из списка
# proxy_ip = random.choice(prox.PROXIES)
proxy_ip = "31.43.179.161"

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

                    cur.execute("SELECT COUNT(*) FROM kaspi WHERE link = %s", (product_url,))
                    if cur.fetchone()[0] == 0:
                        price = card.find('span', class_='item-card__prices-price').text.strip() if card.find('span',
                                                                                                              class_='item-card__prices-price') else 'Нет цены'
                        image_tag = card.find('img', class_='item-card__image')
                        image_url = image_tag['src'] if image_tag and image_tag.has_attr('src') else 'Нет изображения'

                        now = datetime.datetime.now()
                        formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")

                        cur.execute("INSERT INTO kaspi (name, price, image_url, product_url, category, parsed_time) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (name, price, image_url, product_url, category_title, formatted_date))
                        conn.commit()

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
