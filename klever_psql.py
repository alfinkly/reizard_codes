import time
from datetime import datetime
import psycopg2
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Настройка драйвера Selenium для работы в фоновом режиме
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")  # Добавление опции для запуска в фоновом режиме
chrome_options.add_argument("--no-sandbox")  # Запуск Chrome без использования песочницы
chrome_options.add_argument("--disable-dev-shm-usage")  # Отключение использования разделяемой памяти
chrome_options.add_argument("--window-size=1920,1080")  # Установка размера окна

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Подключение к PostgreSQL
conn = psycopg2.connect(
    dbname="umkabots",
    user="postgres",
    password="W4p_Aspect",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

category_urls = [
    'https://clevermarket.kz/supermarket/catalog/Ovoshchi-zelen-gribi-solenya/1089',
    # Ваш список URL-адресов категорий продуктов продолжается здесь...
]

def insert_or_update_product(name, price, image_url, link, category_name):
    current_time = datetime.now().strftime("%d-%m-%Y %H:%M")  # Текущая дата и время
    cur.execute("SELECT COUNT(*) FROM klever WHERE link = %s", (link,))
    existing_product_count = cur.fetchone()[0]

    if existing_product_count == 0:
        # Если товара нет, добавляем его с текущим временем парсинга
        cur.execute("INSERT INTO klever (name, price, image_url, link, category_name, parsed_time) VALUES (%s, %s, %s, %s, %s, %s)",
                    (name, price, image_url, link, category_name, current_time))
    else:
        # Если товар уже существует, обновляем информацию о времени парсинга, независимо от изменения цены
        cur.execute("UPDATE klever SET price = %s, name = %s, image_url = %s, parsed_time = %s WHERE link = %s",
                    (price, name, image_url, current_time, link))

    conn.commit()


while True:
    for category_url in category_urls:
        driver.get(category_url)
        time.sleep(5)  # Дайте странице некоторое время, чтобы загрузиться

        try:
            category_name_element = driver.find_element(By.CLASS_NAME, 'description-sm')
            category_name = category_name_element.text.strip()
        except NoSuchElementException:
            category_name = "Категория не найдена"

        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        product_cards = driver.find_elements(By.CLASS_NAME, 'product-card')
        for card in product_cards:
            name = card.find_element(By.CLASS_NAME, "product-card-title").text.strip()
            price = card.find_element(By.CLASS_NAME, "text-sm").text.strip()
            image_url = card.find_element(By.TAG_NAME, "img").get_attribute("src")
            link = card.find_element(By.TAG_NAME, "a").get_attribute("href")

            insert_or_update_product(name, price, image_url, link, category_name)

    time.sleep(300)  # Пауза перед следующим циклом парсинга, здесь 5 минут
