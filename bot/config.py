import psycopg2
from motor.motor_asyncio import AsyncIOMotorClient

category_mapping = {
    'Свежие овощи и фрукты': ['Овощи, зелень, грибы, соленья', 'Фрукты, ягоды'],
    'Молочные продукты': ['Молочные продукты, яйцо', 'Сыры'],
    'Фермерская лавка': ['Мясо птица'],
    'Кулинария': ['Майонез, соусы', 'Полуфабрикаты', ],
    'Мясо и птица': ['Мясо птица'],
    'Напитки': ['Вода', 'Напитки', 'Соки, нектары, компоты', 'Чай', 'Кофе, какао, сухое молоко'],
    'Рыба и морепродукты': ['Рыба, морепродукты, икра'],
    'Хлеб и выпечка': ['Хлеб', 'Выпечка'],
    'Колбасы': ['Колбасы, деликатесы', 'Сосиски, сардельки'],
    'Замороженные продукты': ['Овощи фрукты замороженные', 'Мороженое'],
    'Растительные продукты': ['Растительные масла', 'Орехи, сухофрукты, семечки'],
    'Для готовки и выпечки': ['Мука и всё для выпечки', 'Специи, приправы', 'Кетчуп, томатная паста, соусы'],
    'Сладости': ['Конфеты, зефир, мармелад', 'Печенье, вафли, торты', 'Шоколад, батончики, паста'],
    'Крупы, консервы, снеки': ['Крупы', 'Консервы', 'Чипсы, сухарики, снеки'],
}

TOKEN = '6752451387:AAFabdJ8glHI6iK46hPd9-CZV4PHdKS2RBY'
POSTGRES_URI = "postgresql://postgres:W4p_Aspect@localhost:5432/umkabots"

# Подключение к базе данных
connection = psycopg2.connect(POSTGRES_URI)
cursor = connection.cursor()