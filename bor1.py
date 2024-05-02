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

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = '6752451387:AAFabdJ8glHI6iK46hPd9-CZV4PHdKS2RBY'
MONGO_URI = 'mongodb://localhost:27017'
DATABASE_NAME = 'ARBKLE'
ADMIN_CHAT_ID = '5149601388'  # ID вашего чата с ботом для уведомлений
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
arbuz_collection = db['ARBUZ']
klever_collection = db['KLEVER']

product_clicks_collection = db['product_clicks']




































