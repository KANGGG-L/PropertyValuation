from scraper.scraper import Scraper
from database.database import DatabaseManager
from config.config import DB_CONFIG
from utils.constant import *


db_manager = DatabaseManager(DB_CONFIG)

scraper = Scraper(headless=False, teardown=True)

rental_data = scraper.fetch_rentals("3000")

db_manager.store_data_in_postgis(rental_data, RENTAL_MODE)





