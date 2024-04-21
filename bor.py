import asyncio
import hashlib
import logging
from datetime import datetime, timedelta

import pymongo
from aiogram import Bot, executor
from aiogram import Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from fuzzywuzzy import process
from motor.motor_asyncio import AsyncIOMotorClient

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = '6752451387:AAFabdJ8glHI6iK46hPd9-CZV4PHdKS2RBY'
MONGO_URI = 'localhost'
DATABASE_NAME = 'ARBKLE'
ADMIN_CHAT_ID = '5149601388'  # ID вашего чата с ботом для уведомлений
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
arbuz_collection = db['ARBUZ']
klever_collection = db['KLEVER']
kaspi_collection = db['KASPI']
print(arbuz_collection)
print("-----" * 7)
print(klever_collection)
print("-----" * 7)
# print(kaspi_collection)
print("-----" * 7)

product_clicks_collection = db['product_clicks']


class ProductView(StatesGroup):
    viewing = State()


class ProductSearch(StatesGroup):
    choosing_category = State()  # Уже существующее состояние
    waiting_for_search_query = State()  # Новое состояние для обработки ввода поиска
    viewing = State()  # Новое состояние для просмотра продуктов


category_mapping = {
    'Свежие овощи и фрукты': [
        'Овощи, зелень, грибы, соленья', 'Фрукты, ягоды',
        # Kaspi categories
        'Овощи', 'Фрукты', 'Зелень, салаты', 'Ягоды', 'Овощи, фрукты, ягоды, грибы'
    ],
    'Молочные продукты': [
        'Молочные продукты, яйцо', 'Сыры',
        # Kaspi categories
        'Молоко, сухое молоко, сливки', 'Кефир, Тан, Айран', 'Молочные продукты, яйца', 'Йогурт', 'Сметана',
        'Творог и творожная масса'
    ],
    'Фермерская лавка': [
        'Мясо птица',
        # Kaspi categories
        'Мясо и птица', 'Мясо', 'Птица', 'Мясная консервация', 'Мясные полуфабрикаты', 'Фарш'
    ],
    'Кулинария': [
        'Майонез, соусы', 'Полуфабрикаты',
        # Kaspi categories
        'Готовая еда', 'Вторые блюда', 'Вторые блюда сублимированные', 'Заготовки для приготовления блюд',
        'Замороженная готовая еда', 'Замороженные полуфабрикаты', 'Полуфабрикаты из рыбы и морепродуктов',
        'Сублимированная туристическая еда'
    ],
    'Мясо и птица': [
        'Мясо птица',
        # Kaspi categories
        'Мясо', 'Птица'
    ],
    'Напитки': [
        'Вода', 'Напитки', 'Соки, нектары, компоты', 'Чай', 'Кофе, какао, сухое молоко',
        # Kaspi categories (включая алкогольные напитки)
        'Квас, комбуча', 'Соки, вода, напитки', 'Соки, нектары, морсы', 'Чай, кофе, какао', 'Холодный кофе',
        'Холодный чай', 'Энергетические напитки', 'Абсент, Самбука', 'Алкоголь', 'Бренди', 'Вермут', 'Вино', 'Виски',
        'Водка', 'Игристые вина, шампанское', 'Джин', 'Кальвадос', 'Коньяк', 'Ликер', 'Портвейн', 'Ром', 'Текила',
        'Чача'
    ],
    'Рыба и морепродукты': [
        'Рыба, морепродукты, икра',
        # Kaspi categories
        'Морепродукты', 'Рыба', 'Рыба, морепродукты', 'Полуфабрикаты из рыбы и морепродуктов'
    ],
    'Хлеб и выпечка': [
        'Хлеб', 'Выпечка',
        # Kaspi categories
        'Выпечка и сдоба', 'Замороженная выпечка и десерты', 'Кексы, рулеты, бисквиты'
    ],
    'Колбасы': [
        'Колбасы, деликатесы', 'Сосиски, сардельки',
        # Kaspi categories
        'Колбасы и копчености', 'Колбасы, сосиски, деликатесы', 'Сосиски, сардельки, колбаски'
    ],
    'Замороженные продукты': [
        'Овощи фрукты замороженные', 'Мороженое',
        # Kaspi categories
        'Замороженные овощи, смеси, грибы', 'Замороженные фрукты и ягоды', 'Замороженные продукты, мороженое'
    ],
    'Растительные продукты': [
        'Растительные масла', 'Орехи, сухофрукты, семечки',
        # Kaspi categories
        'Орехи'
    ],
    'Для готовки и выпечки': [
        'Мука и всё для выпечки', 'Специи, приправы', 'Кетчуп, томатная паста, соусы',
        # Kaspi categories
        'Все для выпечки', 'Ингредиенты для выпечки'
    ],
    'Сладости': [
        'Конфеты, зефир, мармелад', 'Печенье, вафли, торты', 'Шоколад, батончики, паста',
        # Kaspi categories
        'Батончики и печенье протеиновое, злаковое', 'Варенье, повидло, протертые ягоды', 'Зефир, пастила, безе'
    ],
    'Крупы, консервы, снеки': [
        'Крупы', 'Консервы', 'Чипсы, сухарики, снеки',
        # Kaspi categories
        'Бульоны и заправки для супа', 'Грибы консервированные', 'Овощная консервация',
        'Кукуруза и бобы консервированные', 'Мясная консервация', 'Рыбная консервация', 'Фруктово-ягодная консервация',
        'Консервация'
    ]
}


async def cache_category_data(category):
    # Соответствие категорий в "арбуз" и "клевер"
    klever_categories = category_mapping[category]

    # Загрузка и кэширование данных для "арбуз"
    arbuz_products = await arbuz_collection.find({'category': category}).to_list(None)
    arbuz_cache[category] = arbuz_products

    # Загрузка и кэширование данных для "клевер" по соответствующим категориям
    klever_products = []
    for klever_category in klever_categories:
        klever_products += await klever_collection.find({'category': klever_category}).to_list(None)
    klever_cache[category] = klever_products

    # Убедитесь, что кэш для категории инициализирован
    matched_products_cache.setdefault(category, [])

    # Сопоставление товаров и добавление несоответствий
    matched_products = await find_matching_products(arbuz_products, klever_products)
    matched_products_cache[category].extend(matched_products)

    # Добавление несопоставленных товаров из обоих магазинов
    for product in klever_products:
        if all(product['name'].lower() != arbuz_product['name'].lower() for arbuz_product in arbuz_products):
            matched_products_cache[category].append((None, product))

    for product in arbuz_products:
        if all(product['name'].lower() != klever_product['name'].lower() for klever_product in klever_products):
            matched_products_cache[category].append((product, None))

    print(f"Cache for category '{category}' refreshed with {len(matched_products_cache[category])} items.")


def get_category_mapping(category, category_mapping):
    for key, values in category_mapping.items():
        if category in values:
            return key
    return category  # Возвращаем исходную категорию, если сопоставление не найдено


def find_matching_products(arbuz_products, klever_products, kaspi_products, category_mapping):
    matches = []

    # Объединяем все товары в один список с указанием источника
    all_products = [(product, 'arbuz') for product in arbuz_products] + \
                   [(product, 'klever') for product in klever_products] + \
                   [(product, 'kaspi') for product in kaspi_products]

    for product, source in all_products:
        # Определяем категорию для текущего продукта
        base_category = get_category_mapping(product['category'], category_mapping)

        # Фильтруем товары из других магазинов по совпадению категории
        other_products = [p for p, s in all_products if
                          get_category_mapping(p['category'], category_mapping) == base_category and s != source]

        # Ищем сходные товары по названию в рамках категории
        for other_product in other_products:
            similarity = process.extractOne(product['name'], [other_product['name']])

            # Добавляем в список совпадений, если сходство выше порога
            if similarity[1] >= 80:  # Порог сходства 80
                matches.append((product, other_product, similarity[1]))

    # Убираем возможные дубликаты совпадений
    matches = list(set(matches))

    return matches


arbuz_cache = {}
klever_cache = {}
kaspi_cache = {}
matched_products_cache = {}


async def periodic_cache_update(interval_seconds=1800):  # 1800 секунд = 30 минут
    while True:
        await asyncio.sleep(interval_seconds)  # Ожидание заданного интервала
        await cache_arbuz_klever_data()  # Ваша функция для обновления кэша
        logger.info("cashe refesh.")


async def on_startup(dp):
    await cache_arbuz_klever_data()


# Команда для администратора, чтобы обновить кеш вручную
@dp.message_handler(commands=['refresh_cache'], user_id=ADMIN_CHAT_ID)
async def refresh_cache_command(message: types.Message):
    await cache_arbuz_klever_data()
    await message.answer("cashe refesh.")


async def cache_collection_data(collection_name, cache):
    # logger.info(f"Начало кеширования данных из коллекции {collection_name}...")
    collection = db[collection_name]
    documents = await collection.find().to_list(None)
    cache[collection_name] = documents
    # logger.info(f"Кешировано {len(documents)} документов из коллекции '{collection_name}'.")
    # Выводим количество закешированных документов для подтверждения
    # print(f"Кешировано {len(documents)} документов из коллекции '{collection_name}'.")


async def cache_arbuz_klever_data():
    # Кеширование данных из коллекции 'ARBUZ'
    await cache_collection_data('ARBUZ', arbuz_cache)
    # Кеширование данных из коллекции 'KLEVER'
    await cache_collection_data('KLEVER', klever_cache)
    # Кеширование данных из коллекции 'KASPI'
    await cache_collection_data('KASPI', kaspi_cache)

    # Проверяем и подтверждаем кеширование
    if 'ARBUZ' in arbuz_cache and 'KLEVER' in klever_cache:
        logger.info("vse danne uspeshno save.")
        # print("Все данные были успешно закешированы.")  # Подтверждение в консоли
    else:
        logger.error("information no cashe.")
        # print("Данные не были закешированы.")  # Ошибка в консоли
        # После кеширования проверяем размер кеша
        # print(f"Размер кеша 'ARBUZ': {len(arbuz_cache['ARBUZ'])}")
        # print(f"Размер кеша 'KLEVER': {len(klever_cache['KLEVER'])}")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('category:'))
async def process_category_selection(callback_query: types.CallbackQuery):
    logger.info(f"Вызов функции process_category_selection с data: {callback_query.data}")  # Логирование вызова функции
    category_name = callback_query.data.split('category:')[1]

    # Пытаемся получить данные из кэша для всех трех магазинов
    arbuz_products = arbuz_cache.get(category_name)
    klever_products = klever_cache.get(category_name)
    kaspi_products = kaspi_cache.get(category_name)  # Добавляем "Каспий"

    # Проверяем, были ли данные взяты из кэша, и если нет, делаем запросы в базу данных
    if arbuz_products is None or klever_products is None or kaspi_products is None:
        logger.info(f"Делаем запрос в базу данных по категории '{category_name}'.")
        if arbuz_products is None:
            arbuz_products = await db['arbuz'].find({'category': category_name}).to_list(None)
            arbuz_cache[category_name] = arbuz_products  # Обновляем кэш данными из базы данных
        if klever_products is None:
            klever_products = await db['klever'].find({'category': category_name}).to_list(None)
            klever_cache[category_name] = klever_products
        if kaspi_products is None:  # Делаем запрос для "Каспий"
            kaspi_products = await db['kaspi'].find({'category': category_name}).to_list(None)
            kaspi_cache[category_name] = kaspi_products  # Обновляем кэш данными из базы данных

    # Формирование ответа с учетом всех трех магазинов
    message_text = f"Продукты в категории '{category_name}':\n"
    message_text += "\nArbuz:\n" + "\n".join([prod['name'] for prod in arbuz_products])
    message_text += "\n\nKlever:\n" + "\n".join([prod['name'] for prod in klever_products])
    message_text += "\n\nKaspi:\n" + "\n".join([prod['name'] for prod in kaspi_products])  # Добавляем "Каспий"

    await bot.send_message(callback_query.from_user.id, message_text)
    await callback_query.answer()


category_cache = {}
last_cache_update = datetime.now() - timedelta(days=1)  # Инициализируем в прошлом для обновления при старте
cache_ttl = timedelta(minutes=100)  # Время жизни кэша, например, 10 минут


async def update_category_cache():
    global category_cache, last_cache_update
    now = datetime.now()
    try:
        if now - last_cache_update > cache_ttl:
            new_cache = {}
            for arbuz_category, klever_categories in category_mapping.items():
                arbuz_count = await arbuz_collection.count_documents({'category': arbuz_category})
                klever_count = await klever_collection.count_documents({'category': {'$in': klever_categories}})
                kaspi_count = await kaspi_collection.count_documents({'category': {'$in': klever_categories}})

                total_count = arbuz_count + klever_count + kaspi_count
                new_cache[arbuz_category] = total_count

            category_cache = new_cache
            last_cache_update = now
            logger.info("Кэш категорий успешно обновлен.")
    except Exception as e:
        logger.error(f"Ошибка при обновлении кэша категорий: {e}")
        logger.error("Скрипт работает неправильно.")


@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    existing_user = await db['user_contacts'].find_one({'user_id': user_id})

    if existing_user:
        # Обновление кэша перед использованием
        await update_category_cache()
        markup = InlineKeyboardMarkup()
        for category, total_count in category_cache.items():
            button_text = f"{category} ({total_count} Продуктов)"
            markup.add(InlineKeyboardButton(button_text, callback_data='category:' + category))
        await message.answer("Вы уже поделились своим номером телефона. Выберите категорию:", reply_markup=markup)
        await ProductSearch.choosing_category.set()
    else:
        contact_request_button = KeyboardButton('Поделиться номером телефона', request_contact=True)
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True).add(contact_request_button)
        await message.answer("Для продолжения работы с ботом, поделитесь вашим номером телефона.",
                             reply_markup=keyboard)


@dp.callback_query_handler(text='share_contact')
async def prompt_for_contact(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    # Отправляем пользователю инструкцию, как поделиться контактом
    await bot.send_message(callback_query.from_user.id,
                           "Пожалуйста, отправьте ваш контакт через прикрепление -> Контакт.")
    # Удаляем исходное сообщение с кнопкой
    await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)


@dp.message_handler(content_types=['contact'], state='*')
async def contact_received(message: types.Message, state: FSMContext):
    contact = message.contact
    await db['user_contacts'].update_one(
        {'user_id': message.from_user.id},
        {'$set': {'phone_number': contact.phone_number, 'first_name': contact.first_name,
                  'last_name': contact.last_name}},
        upsert=True
    )

    await message.answer("Спасибо за предоставленную информацию!", reply_markup=types.ReplyKeyboardRemove())

    # После получения контакта, предлагаем выбрать категорию
    markup = await category_keyboard()
    await message.answer("Выберите категорию:", reply_markup=markup)
    await ProductSearch.choosing_category.set()


async def show_products(message: types.Message, state: FSMContext, page: int = 0):
    data = await state.get_data()
    matched_products = data.get('matched_products', [])
    # sent_products теперь используется для кэширования товаров, отправленных на каждой странице
    page_cache = data.get('page_cache', {})
    sent_products = data.get('sent_products', set())  # Общий кэш отправленных товаров
    last_message_ids = data.get('last_message_ids', [])

    items_per_page = 5
    total_pages = (len(matched_products) + items_per_page - 1) // items_per_page

    if page in page_cache:
        # Извлекаем товары из кэша страницы, если уже просматривали эту страницу
        page_products = page_cache[page]
    else:
        # Выбираем товары для текущей страницы, если впервые на ней
        start_index = page * items_per_page
        end_index = start_index + items_per_page
        page_products = []
        for prod in matched_products[start_index:end_index]:
            arbuz_id = prod[0]['_id'] if prod[0] else None
            klever_id = prod[1]['_id'] if prod[1] else None
            kaspi_id = prod[2]['_id'] if len(prod) > 2 and prod[2] else None

            # Проверяем, не был ли товар уже отправлен
            if not any(pid in sent_products for pid in [arbuz_id, klever_id, kaspi_id]):
                page_products.append(prod)
                # Добавляем ID отправленных товаров в общий кэш
                if arbuz_id: sent_products.add(arbuz_id)
                if klever_id: sent_products.add(klever_id)
                if kaspi_id: sent_products.add(kaspi_id)

        # Кэшируем товары текущей страницы
        page_cache[page] = page_products

    if not page_products:
        await message.answer("Товары не найдены или вы достигли конца списка.")
        return

    for product_pair in page_products:
        arbuz_product, klever_product, kaspi_product = product_pair if len(product_pair) > 2 else (
        product_pair[0], product_pair[1], None)
        arbuz_text, klever_text, kaspi_text, image_url = format_message(arbuz_product, klever_product,
                                                                        kaspi_product)  # Обновите функцию format_message
        text = arbuz_text + "\n" + klever_text + "\n" + kaspi_text  # Добавление текста для "Каспий"

        markup = InlineKeyboardMarkup(row_width=2)
        if arbuz_product and klever_product:
            product_id = arbuz_product.get('_id', klever_product.get('_id', 'unknown'))
            markup.add(
                InlineKeyboardButton("Соответствует", callback_data=f"match:{product_id}"),
                InlineKeyboardButton("Не соответствует", callback_data=f"nomatch:{product_id}")
            )

        try:
            if image_url and image_url.startswith('http'):
                sent_message = await message.bot.send_photo(chat_id=message.chat.id, photo=image_url, caption=text,
                                                            reply_markup=markup)
            else:
                raise ValueError("Invalid image URL")
        except Exception as e:
            print(f"Ошибка при отправке изображения: {e}. Отправляю сообщение без изображения.")
            sent_message = await message.answer(text, reply_markup=markup)

        # Добавляем ID отправленного товара в список отправленных
        if arbuz_product:
            sent_products.add(arbuz_product['_id'])
        if klever_product:
            sent_products.add(klever_product['_id'])

        last_message_ids.append(sent_message.message_id)

    await state.update_data(page_cache=page_cache, sent_products=sent_products, last_message_ids=last_message_ids)

    navigation_markup = InlineKeyboardMarkup(row_width=2)
    if page > 0:
        navigation_markup.insert(InlineKeyboardButton("⬅ Назад", callback_data=f"page:{page - 1}"))
    if page + 1 < total_pages:
        navigation_markup.insert(InlineKeyboardButton("Вперед ➡", callback_data=f"page:{page + 1}"))

    navigation_markup.insert(InlineKeyboardButton("🔍 Инлайн-поиск", switch_inline_query_current_chat=""))

    navigation_message = await message.answer("Перейдите на следующую страницу или используйте инлайн-поиск:",
                                              reply_markup=navigation_markup)
    last_message_ids.append(navigation_message.message_id)

    # Обновляем состояние с новым списком ID отправленных сообщений и товаров
    await state.update_data(page_cache=page_cache, sent_products=sent_products, last_message_ids=last_message_ids)


@dp.inline_handler(state='*')
async def inline_query_handler(inline_query: types.InlineQuery, state: FSMContext):
    # Сохраняем текущее состояние пользователя и его данные
    current_state = await state.get_state()
    user_data = await state.get_data()

    # Сбрасываем состояние пользователя для обработки инлайн-запроса
    await state.finish()

    query = inline_query.query.strip()
    results = []

    if query:
        try:
            # Использование индекса `name_text` для ускорения поиска
            projection = {'_id': True, 'name': True, 'price': True, 'image_url': True, 'link': True}
            search_results_arbuz = await arbuz_collection.find(
                {'$text': {'$search': query}},
                projection
            ).sort('name', pymongo.ASCENDING).limit(10).to_list(None)
            search_results_klever = await klever_collection.find(
                {'$text': {'$search': query}},
                projection
            ).sort('name', pymongo.ASCENDING).limit(10).to_list(None)

            # Объединение результатов из обеих коллекций
            combined_results = search_results_arbuz + search_results_klever
            unique_results = {result['name']: result for result in combined_results}.values()

            for result in unique_results:
                full_url = result["link"]
                if not full_url.startswith('http'):
                    full_url = 'https://arbuz.kz' + full_url
                photo_url = result["image_url"]
                title = result['name']
                price = result['price']

                keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton(text="Подробнее", url=full_url))

                results.append(
                    InlineQueryResultArticle(
                        id=str(result['_id']),
                        title=title,
                        input_message_content=InputTextMessageContent(
                            message_text=f"<b>{title}</b>\nЦена: {price}\n<a href='{full_url}'>Ссылка на товар</a>",
                            parse_mode=types.ParseMode.HTML
                        ),
                        reply_markup=keyboard,
                        thumb_url=photo_url,
                        description=f"Цена: {price}"
                    )
                )
        except Exception as e:
            # Отправляем сообщение в случае ошибки
            await inline_query.answer(
                results=[],
                cache_time=1,
                switch_pm_text="Произошла ошибка при поиске, попробуйте позже.",
                switch_pm_parameter="start"
            )
            return

    # Обрезаем до 20 результатов для ответа
    results = results[:20]
    if current_state is not None:
        await state.set_state(current_state)
        await state.set_data(user_data)

    # Отправляем результаты пользователю
    await bot.answer_inline_query(inline_query.id, results=results, cache_time=1)


button_states = {}  # Ключ: (user_id, product_id), Значение: 'match' или 'nomatch'


async def update_clicks(user_id, product_id, product_name, product_url, click_type, callback_query):
    # Обновляем состояние кнопок в хранилище
    button_states[(user_id, product_id)] = click_type

    collection = db['product_clicks']
    user_field = f"{click_type}_users"
    await collection.update_one(
        {"product_id": product_id},
        {"$set": {"product_name": product_name, "product_url": product_url},
         "$inc": {f"{click_type}_clicks": 1},
         "$addToSet": {user_field: user_id}},
        upsert=True
    )
    doc = await collection.find_one({"product_id": product_id})
    if doc:
        message = f"Товар '{product_name}' ({product_id}) был отмечен как {'соответствует' if click_type == 'match' else 'не соответствует'}. Текущее количество кликов: {doc[click_type + '_clicks']}."
        await bot.send_message(ADMIN_CHAT_ID, message)
        await callback_query.message.copy_to(ADMIN_CHAT_ID)

    # Обновляем сообщение с кнопками
    await refresh_message_buttons(callback_query, product_id)


async def refresh_message_buttons(callback_query: types.CallbackQuery, product_id: str):
    user_id = callback_query.from_user.id
    state = button_states.get((user_id, product_id), None)

    # Настраиваем текст кнопок в зависимости от состояния
    match_button_text = "✅ Соответствует" if state == "match" else "Соответствует"
    nomatch_button_text = "✅ Не соответствует" if state == "nomatch" else "Не соответствует"

    markup = InlineKeyboardMarkup(row_width=2)
    match_button = InlineKeyboardButton(match_button_text, callback_data=f"match:{product_id}")
    nomatch_button = InlineKeyboardButton(nomatch_button_text, callback_data=f"nomatch:{product_id}")
    markup.add(match_button, nomatch_button)

    # Обновляем сообщение с новой разметкой кнопок
    await callback_query.message.edit_reply_markup(reply_markup=markup)


@dp.callback_query_handler(lambda c: c.data.startswith("match:"), state=ProductSearch.viewing)
async def handle_match(callback_query: types.CallbackQuery, state: FSMContext):
    product_id = callback_query.data.split(':')[1]  # Извлекаем ID продукта
    # Здесь должен быть код для извлечения названия и ссылки продукта, пока используем заглушки
    product_name = "Product Name Placeholder"
    product_url = "http://example.com/placeholder"
    await update_clicks(callback_query.from_user.id, product_id, product_name, product_url, "match", callback_query)
    await callback_query.answer("Вы отметили товар как соответствующий.")


@dp.callback_query_handler(lambda c: c.data.startswith('category:'), state='*')
async def process_category_selection(callback_query: types.CallbackQuery, state: FSMContext):
    # Парсим имя категории из callback_data
    category = callback_query.data.split(':')[1]
    await callback_query.answer()

    # Ищем товары в Арбузе
    arbuz_products = await arbuz_collection.find({'category': category}).to_list(None)

    # Используем category_mapping для поиска соответствующих категорий в Клевере и Каспий
    klever_categories = category_mapping.get(category, [])
    klever_products = []
    kaspi_products = []  # Инициализация списка товаров из Каспий
    for klever_category in klever_categories:
        klever_products.extend(await klever_collection.find({'category': klever_category}).to_list(None))
        # Допустим, что категории для Каспий те же, что и для Клевер. Если это не так, необходимо настроить соответствующее отображение
        kaspi_products.extend(await kaspi_collection.find({'category': klever_category}).to_list(None))

    # Проверяем, есть ли уже сравнения для данной категории в кэше
    if category not in matched_products_cache:
        # Теперь передаем все три списка товаров в функцию
        matched_products = await find_matching_products(arbuz_products, klever_products, kaspi_products,
                                                        category_mapping)
        matched_products_cache[category] = matched_products

    # Сохраняем сравненные товары в состояние
    await state.update_data(matched_products=matched_products_cache[category])

    if matched_products_cache[category]:
        await show_products(callback_query.message, state)
    else:
        await callback_query.message.edit_text("Соответствующие товары не найдены.")


@dp.callback_query_handler(lambda c: c.data.startswith("nomatch:"), state=ProductSearch.viewing)
async def handle_nomatch(callback_query: types.CallbackQuery, state: FSMContext):
    product_id = callback_query.data.split(':')[1]  # Извлекаем ID продукта
    user_id = callback_query.from_user.id  # ID пользователя, который нажал кнопку

    # Извлечение информации о продукте (здесь используется заглушка)
    product_name = "Product Name Placeholder"
    product_url = "http://example.com/placeholder"

    # Обновляем информацию о кликах
    await update_clicks(callback_query.from_user.id, product_id, product_name, product_url, "nomatch", callback_query)

    # Извлекаем номер телефона и имя пользователя из базы данных
    user_contact = await db['user_contacts'].find_one({'user_id': user_id})
    if user_contact:
        phone_number = user_contact.get('phone_number', 'Номер не предоставлен')
        first_name = user_contact.get('first_name', 'Имя не предоставлено')
        last_name = user_contact.get('last_name', '')

        # Составляем сообщение администратору
        admin_message = (
            f"Пользователь: {first_name} {last_name}\n"
            f"ID: {user_id}\n"
            f"Телефон: {phone_number}\n"
            f"Отметил товар как 'Не соответствует':\n"
            f"{product_name}\n"
            f"{product_url}"
        )
        await bot.send_message(ADMIN_CHAT_ID, admin_message)
    else:
        await bot.send_message(ADMIN_CHAT_ID, f"Пользователь с ID {user_id} не найден в базе данных.")

    await callback_query.answer("Вы отметили товар как не соответствующий.")


page_storage = {}


# Обработчик пагинации
@dp.callback_query_handler(lambda c: c.data.startswith("page:"), state='*')
async def navigate_page(callback_query: types.CallbackQuery, state: FSMContext):
    page = int(callback_query.data.split(':')[1])

    # Получаем и обновляем данные пользователя
    data = await state.get_data()
    last_message_ids = data.get('last_message_ids', [])
    # Здесь добавьте логику для обновления данных пользователя, если необходимо

    # Удаляем предыдущие сообщения
    for message_id in last_message_ids:
        try:
            await bot.delete_message(chat_id=callback_query.message.chat.id, message_id=message_id)
        except Exception as e:
            logger.error(f"Не удалось удалить сообщение с ID {message_id}: {e}")

    # Очищаем список ID в состоянии
    await state.update_data(last_message_ids=[])

    # Переходим к отображению продуктов на новой странице
    await show_products(callback_query.message, state, page)
    await callback_query.answer()


def generate_hash(link):
    hash_object = hashlib.md5(link.encode())
    return hash_object.hexdigest()[:10]  # Берем первые 10 символов для уменьшения размера


def format_message(arbuz_product=None, klever_product=None, kaspi_product=None, base_url_arbuz="https://arbuz.kz",
                   base_url_klever="https://klever.kz", base_url_kaspi="https://kaspi.kz"):
    arbuz_text = ""
    klever_text = ""
    kaspi_text = ""  # Текст для "Каспий"
    image_url = None

    if arbuz_product:
        # Формируем информацию о продукте из Арбуза
        arbuz_text = (
            f"Арбуз:\n"
            f"Название: {arbuz_product.get('name', 'Название отсутствует')}\n"
            f"Цена: {arbuz_product.get('price', 'Цена отсутствует')}\n"
            f"Категория: {arbuz_product.get('category', 'Категория отсутствует')}\n"
            f"Актуально на: {arbuz_product.get('parsed_time', 'Время не указано')}\n"
            f"Ссылка: {base_url_arbuz + arbuz_product.get('link', '')}\n"
        )
        image_url = arbuz_product.get('image_url', None)
    else:
        arbuz_text = "Соответствий в Арбузе не найдено.\n"

    if klever_product:
        # Формируем информацию о продукте из Клевера
        klever_text = (
            f"Клевер:\n"
            f"Название: {klever_product.get('name', 'Название отсутствует')}\n"
            f"Цена: {klever_product.get('price', 'Цена отсутствует')}\n"
            f"Категория: {klever_product.get('category', 'Категория отсутствует')}\n"
            f"Актуально на: {klever_product.get('parsed_time', 'Время не указано')}\n"
            f"Ссылка: {klever_product.get('link', '')}\n"
        )
        # Изображение товара из Клевера используется, если нет изображения из Арбуза
        image_url = image_url or klever_product.get('image_url', None)
    else:
        klever_text = "Соответствий в Клевере не найдено.\n"

    if kaspi_product:
        # Формируем информацию о продукте из Каспий
        kaspi_text = (
            f"Каспий:\n"
            f"Название: {kaspi_product.get('name', 'Название отсутствует')}\n"
            f"Цена: {kaspi_product.get('price', 'Цена отсутствует')}\n"
            f"Категория: {kaspi_product.get('category', 'Категория отсутствует')}\n"
            f"Актуально на: {kaspi_product.get('parsed_time', 'Время не указано')}\n"
            f"Ссылка: {kaspi_product.get('product_url', '')}\n"
        )
        # Изображение товара из Каспий используется, если нет изображения из других магазинов
        image_url = image_url or kaspi_product.get('image_url', None)
    else:
        kaspi_text = "Соответствий в Каспии не найдено.\n"

    return arbuz_text, klever_text, kaspi_text, image_url


@dp.callback_query_handler(lambda c: c.data.startswith('page:'), state='*')
async def handle_page_change(callback_query: types.CallbackQuery, state: FSMContext):
    # Получаем номер страницы из callback данных
    page_number = int(callback_query.data.split(':')[1])

    # Попытка удалить предыдущее сообщение бота
    try:
        await bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    except Exception as e:
        logger.error(f"Не удалось удалить сообщение: {e}")

    # Показываем продукты на новой странице
    await show_products(callback_query.message, state, page=page_number)


@dp.message_handler(state=ProductSearch.waiting_for_search_query)
async def process_search_query(message: types.Message, state: FSMContext):
    search_query = message.text.strip()
    if not search_query:
        await message.answer("Поисковый запрос не может быть пустым. Пожалуйста, введите название товара.")
        return

    # Выполнение поиска в коллекциях Арбуз и Клевер
    arbuz_products = await arbuz_collection.find({'$text': {'$search': search_query}}).to_list(length=100)
    klever_products = await klever_collection.find({'$text': {'$search': search_query}}).to_list(length=100)
    kaspi_products = await kaspi_collection.find({'$text': {'$search': search_query}}).to_list(length=100)

    # Поиск совпадений между двумя коллекциями
    matched_products = await find_matching_products(arbuz_products, klever_products, kaspi_products)

    # Проверяем, есть ли совпадающие товары
    if not matched_products:
        await message.answer("Товары по запросу не найдены.")
        await state.finish()
        return

    # Выводим информацию о первом совпадении
    # (Для упрощения предполагаем, что совпадения уже отсортированы по релевантности)
    arbuz_product, klever_product, kaspi_product = matched_products[0]

    # Обновляем вызов format_message, чтобы он теперь принимал три продукта
    arbuz_text, klever_text, kaspi_text, image_url = format_message(arbuz_product, klever_product, kaspi_product)

    # Формируем текст сообщения, включающий информацию из всех трех источников
    text = f"{arbuz_text}\n{klever_text}\n{kaspi_text}"

    # Отправляем результаты пользователю
    if image_url:
        await message.answer_photo(photo=image_url, caption=text)
    else:
        await message.answer(text)
    # Завершаем сессию состояния
    await state.finish()

    # Показать пользователю кнопки для перехода по страницам, если есть более одного совпадения
    if len(matched_products) > 1:
        # Создаем клавиатуру для перехода по страницам
        pagination_markup = InlineKeyboardMarkup()
        pagination_markup.add(InlineKeyboardButton("➡️ Следующий товар", callback_data='next_product:1'))
        await message.answer("Перейти к следующему товару:", reply_markup=pagination_markup)

        # Сохраняем все совпадения и текущую страницу в состояние
        await state.update_data(matched_products=matched_products, current_page=0)


@dp.callback_query_handler(lambda c: c.data == 'back_to_categories', state='*')
async def handle_back_to_categories(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await cmd_start(callback_query.message, state)


@dp.callback_query_handler(lambda c: c.data == 'search_by_name', state='*')
async def prompt_search_query(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.answer("Введите название товара для поиска:")
    await ProductSearch.waiting_for_search_query.set()
    await callback_query.answer()


async def create_indexes():
    await arbuz_collection.create_index([("name", "text")])
    await klever_collection.create_index([("name", "text")])


if __name__ == '__main__':
    logger.info("start bot")
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(create_indexes())
        # Запуск задачи периодического обновления кэша
        loop.create_task(periodic_cache_update())
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    except Exception as e:
        logger.exception(f"Произошло исключение при запуске бота: {e}")
