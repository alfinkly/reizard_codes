from aiogram.dispatcher.filters.state import StatesGroup, State


class ProductView(StatesGroup):
    viewing = State()


class ProductSearch(StatesGroup):
    choosing_category = State()  # Уже существующее состояние
    waiting_for_search_query = State()  # Новое состояние для обработки ввода поиска
    viewing = State()  # Новое состояние для просмотра продуктов