import asyncio
import logging
import pymongo
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters.state import State, StatesGroup
from motor.motor_asyncio import AsyncIOMotorClient
from fuzzywuzzy import process, fuzz
import hashlib
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram import Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from datetime import datetime, timedelta
from aiohttp import ClientSession

from bot.config import TOKEN, MONGO_URI, DATABASE_NAME
from bot.functions import cache_arbuz_klever_data, logger, create_indexes, periodic_cache_update

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


async def on_startup(dp):
    await cache_arbuz_klever_data()


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
