import asyncio
import hashlib
import logging
from datetime import timedelta, datetime

from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from fuzzywuzzy import fuzz

from bot.config import category_mapping, ADMIN_CHAT_ID, arbuz_collection, klever_collection, db
from bot.models import ProductSearch

arbuz_cache = {}
klever_cache = {}
matched_products_cache = {}
category_cache = {}
last_cache_update = datetime.now() - timedelta(days=1)  # Инициализируем в прошлом для обновления при старте
cache_ttl = timedelta(minutes=100)  # Время жизни кэша, например, 10 минут

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
button_states = {}  # Ключ: (user_id, product_id), Значение: 'match' или 'nomatch'
page_storage = {}


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


async def find_matching_products(source_products, target_products):
    matched_products = []
    target_products_lower = {product['name'].lower(): product for product in target_products}

    # Сравнение источника с целью
    for source_product in source_products:
        source_name = source_product['name'].lower()
        best_match = None
        best_score = 0
        for target_name, target_product in target_products_lower.items():
            score = fuzz.token_sort_ratio(source_name, target_name)
            if score > best_score and score > 75:
                best_match = target_product
                best_score = score

        matched_products.append((source_product, best_match))

    # Поиск товаров в цели, которые не найдены в источнике
    source_names_lower = {product['name'].lower() for product in source_products}
    for target_product in target_products:
        target_name = target_product['name'].lower()
        if target_name not in source_names_lower:
            matched_products.append((None, target_product))

    return matched_products


async def periodic_cache_update(interval_seconds=1800):  # 1800 секунд = 30 минут
    while True:
        await asyncio.sleep(interval_seconds)  # Ожидание заданного интервала
        await cache_arbuz_klever_data()  # Ваша функция для обновления кэша
        logger.info("cashe refesh.")


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


async def update_category_cache():
    global category_cache, last_cache_update
    now = datetime.now()
    if now - last_cache_update > cache_ttl:
        new_cache = {}
        for arbuz_category, klever_categories in category_mapping.items():
            arbuz_count = await arbuz_collection.count_documents({'category': arbuz_category})
            klever_count = await klever_collection.count_documents({'category': {'$in': klever_categories}})
            total_count = arbuz_count + klever_count
            new_cache[arbuz_category] = total_count
        category_cache = new_cache
        last_cache_update = now


async def show_products(message: types.Message, state: FSMContext, page: int = 0):
    data = await state.get_data()
    matched_products = data.get('matched_products', [])
    sent_products = data.get('sent_products', set())  # Получаем уже отправленные товары
    last_message_ids = data.get('last_message_ids', [])

    await ProductSearch.viewing.set()

    items_per_page = 5
    total_pages = (len(matched_products) + items_per_page - 1) // items_per_page
    start_index = page * items_per_page
    end_index = start_index + items_per_page

    # Фильтруем товары, чтобы не отправлять повторно
    page_products = []
    for prod in matched_products[start_index:end_index]:
        arbuz_id = prod[0]['_id'] if prod[0] else None
        klever_id = prod[1]['_id'] if prod[1] else None
        # Добавляем товар в список, если он еще не был отправлен (проверяем по ID из обоих источников)
        if (arbuz_id and arbuz_id not in sent_products) or (klever_id and klever_id not in sent_products):
            page_products.append(prod)
            if arbuz_id:
                sent_products.add(arbuz_id)
            if klever_id:
                sent_products.add(klever_id)

    if not page_products:
        await message.answer("Товары не найдены или вы достигли конца списка.")
        return

    for product_pair in page_products:
        arbuz_product, klever_product = product_pair
        arbuz_text, klever_text, image_url = format_message(arbuz_product, klever_product)
        text = arbuz_text + "\n" + klever_text

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

    await state.update_data(last_message_ids=last_message_ids, sent_products=sent_products)

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
    await state.update_data(last_message_ids=last_message_ids, sent_products=sent_products)


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
        await callback_query.bot.send_message(ADMIN_CHAT_ID, message)
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


def generate_hash(link):
    hash_object = hashlib.md5(link.encode())
    return hash_object.hexdigest()[:10]  # Берем первые 10 символов для уменьшения размера


def format_message(arbuz_product=None, klever_product=None, base_url_arbuz="https://arbuz.kz", ):
    arbuz_text = ""
    klever_text = ""
    image_url = None

    if arbuz_product:
        # Формируем информацию о продукте из Арбуза
        arbuz_parsed_time_str = arbuz_product.get('parsed_time', "Время не указано")
        arbuz_text = (
            f"Арбуз:\n"
            f"Название: {arbuz_product.get('name', 'Название отсутствует')}\n"
            f"Цена: {arbuz_product.get('price', 'Цена отсутствует')}\n"
            f"Категория: {arbuz_product.get('category', 'Категория отсутствует')}\n"
            f"Актуально на: {arbuz_parsed_time_str}\n"
            f"Ссылка: {base_url_arbuz + arbuz_product.get('link', '')}\n"
        )
        image_url = arbuz_product.get('image_url', None)
    else:
        arbuz_text = "Соответствий в Арбузе не найдено.\n"

    if klever_product:
        # Формируем информацию о продукте из Клевера
        klever_parsed_time_str = klever_product.get('parsed_time', "Время не указано")
        klever_text = (
            f"Клевер:\n"
            f"Название: {klever_product.get('name', 'Название отсутствует')}\n"
            f"Цена: {klever_product.get('price', 'Цена отсутствует')}\n"
            f"Категория: {klever_product.get('category', 'Категория отсутствует')}\n"
            f"Актуально на: {klever_parsed_time_str}\n"
            f"Ссылка: {klever_product.get('link', '')}\n"
        )
        # Изображение товара из Клевера используется, если нет изображения из Арбуза
        image_url = image_url or klever_product.get('image_url', None)
    else:
        klever_text = "Соответствий в Клевере не найдено.\n"

    return arbuz_text, klever_text, image_url


async def create_indexes():
    await arbuz_collection.create_index([("name", "text")])
    await klever_collection.create_index([("name", "text")])
