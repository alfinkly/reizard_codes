import pymongo
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineQueryResultArticle, InputTextMessageContent

from bot.functions import *
from bot.main import dp
from bot.models import *
from bot.config import *


@dp.message_handler(commands=['refresh_cache'], user_id=ADMIN_CHAT_ID)
async def refresh_cache_command(message: types.Message):
    await cache_arbuz_klever_data()
    await message.answer("cashe refesh.")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('category:'))
async def process_category_selection(callback_query: types.CallbackQuery):
    logger.info(f"Вызов функции process_category_selection с data: {callback_query.data}")  # Логирование вызова функции
    category_name = callback_query.data.split('category:')[1]

    # Пытаемся получить данные из кэша
    arbuz_products = arbuz_cache.get(category_name)
    klever_products = klever_cache.get(category_name)

    # Проверяем, были ли данные взяты из кэша
    if arbuz_products is not None and klever_products is not None:
        logger.info(f"Данные по категории '{category_name}' выбраны из кэша.")  # Логирование выбора данных из кэша
    else:
        logger.info(f"Делаем запрос в базу данных по категории '{category_name}'.")  # Логирование запроса в базу данных
        arbuz_products = await db['arbuz'].find({'category': category_name}).to_list(None)
        klever_products = await db['klever'].find({'category': category_name}).to_list(None)
        # Обновляем кэш данными из базы данных
        arbuz_cache[category_name] = arbuz_products
        klever_cache[category_name] = klever_products

    # Теперь у нас есть все необходимые данные для формирования ответа
    message_text = f"Продукты в категории '{category_name}':\n"
    message_text += "\nArbuz:\n" + "\n".join([prod['name'] for prod in arbuz_products])
    message_text += "\n\nKlever:\n" + "\n".join([prod['name'] for prod in klever_products])

    await callback_query.bot.send_message(callback_query.from_user.id, message_text)
    await callback_query.answer()


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
    await callback_query.bot.answer_callback_query(callback_query.id)
    # Отправляем пользователю инструкцию, как поделиться контактом
    await callback_query.bot.send_message(callback_query.from_user.id,
                           "Пожалуйста, отправьте ваш контакт через прикрепление -> Контакт.")
    # Удаляем исходное сообщение с кнопкой
    await callback_query.bot.delete_message(chat_id=callback_query.message.chat.id, message_id=callback_query.message.message_id)


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

    await message.answer("Выберите категорию:")
    await ProductSearch.choosing_category.set()


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
    await inline_query.bot.answer_inline_query(inline_query.id, results=results, cache_time=1)


@dp.callback_query_handler(lambda c: c.data.startswith("match:"), state=ProductSearch.viewing)
async def handle_match(callback_query: types.CallbackQuery, state: FSMContext):
    product_id = callback_query.data.split(':')[1]  # Извлекаем ID продукта
    # Здесь должен быть код для извлечения названия и ссылки продукта, пока используем заглушки
    product_name = "Product Name Placeholder"
    product_url = "http://example.com/placeholder"
    await update_clicks(callback_query.from_user.id, product_id, product_name, product_url, "match", callback_query)
    await callback_query.answer("Вы отметили товар как соответствующий.")


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
        await callback_query.bot.send_message(ADMIN_CHAT_ID, admin_message)
    else:
        await callback_query.bot.send_message(ADMIN_CHAT_ID, f"Пользователь с ID {user_id} не найден в базе данных.")

    await callback_query.answer("Вы отметили товар как не соответствующий.")


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
            await callback_query.bot.delete_message(chat_id=callback_query.message.chat.id, message_id=message_id)
        except Exception as e:
            logger.error(f"Не удалось удалить сообщение с ID {message_id}: {e}")

    # Очищаем список ID в состоянии
    await state.update_data(last_message_ids=[])

    # Переходим к отображению продуктов на новой странице
    await show_products(callback_query.message, state, page)
    await callback_query.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('category:'), state='*')
async def process_category_selection(callback_query: types.CallbackQuery, state: FSMContext):
    # Парсим имя категории из callback_data
    category = callback_query.data.split(':')[1]
    await callback_query.answer()

    # Ищем товары в Арбузе
    arbuz_products = await arbuz_collection.find({'category': category}).to_list(None)

    # Используем category_mapping для поиска соответствующих категорий в Клевере
    klever_categories = category_mapping.get(category, [])
    klever_products = []
    for klever_category in klever_categories:
        klever_products.extend(await klever_collection.find({'category': klever_category}).to_list(None))

    if category not in matched_products_cache:
        matched_products = await find_matching_products(arbuz_products, klever_products)
        matched_products_cache[category] = matched_products

    # Сохраняем сравненные товары в состояние
    await state.update_data(matched_products=matched_products_cache[category])

    if matched_products_cache[category]:
        await show_products(callback_query.message, state)
    else:
        await callback_query.message.edit_text("Соответствующие товары не найдены.")


@dp.callback_query_handler(lambda c: c.data.startswith('page:'), state='*')
async def handle_page_change(callback_query: types.CallbackQuery, state: FSMContext):
    # Получаем номер страницы из callback данных
    page_number = int(callback_query.data.split(':')[1])

    # Попытка удалить предыдущее сообщение бота
    try:
        await callback_query.bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
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

    # Поиск совпадений между двумя коллекциями
    matched_products = await find_matching_products(arbuz_products, klever_products)

    # Проверяем, есть ли совпадающие товары
    if not matched_products:
        await message.answer("Товары по запросу не найдены.")
        await state.finish()
        return

    # Выводим информацию о первом совпадении
    # (Для упрощения предполагаем, что совпадения уже отсортированы по релевантности)
    arbuz_product, klever_product = matched_products[0]
    arbuz_text, klever_text, arbuz_image_url = format_message(arbuz_product, klever_product)
    text = arbuz_text + "\n" + klever_text

    # Отправляем результаты пользователю
    if arbuz_image_url:
        await message.answer_photo(photo=arbuz_image_url, caption=text)
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
