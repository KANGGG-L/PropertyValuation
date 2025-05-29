from scraper.scraper import Scraper
from database.database import DatabaseManager
from config.config import DB_CONFIG
from utils.constant import *
from utils.postcode import *
from valuation.valuation import *


db_manager = DatabaseManager(DB_CONFIG)

group_postcodes_by_state("data/australian_postcodes.csv")
postcodes = get_postcodes_by_state("data/postcode/grouped_postcodes_by_state.xlsx", "VIC")

scraper = Scraper(headless=False, teardown=True)

rental_data = scraper.scrape("VIC", postcodes, True)
db_manager.store_data_in_postgis(rental_data, RENTAL_MODE)

sold_data = scraper.scrape("VIC", postcodes, False)
db_manager.store_data_in_postgis(sold_data, SOLD_MODE)


valuator = PropertyValuator(db_manager)

# Income approach
income_value = valuator.income_based_valuation(
    state="VIC",
    postcode=3000,
    address="43 Spencer St",
    area_name="Melbourne",
    property_type=1,  # apartment
    bedroom_num=2,
    bathroom_num=1,
    parking_num=1
)

# Sales comparison approach
comparison_value = valuator.comparison_based_valuation(
    state="VIC",
    postcode=3000,
    address="43 Spencer St",
    area_name="Melbourne",
    property_type=1,  # apartment
    bedroom_num=2,
    bathroom_num=1,
    parking_num=1
)

# Train and use regression model
train_mae, test_mae = valuator.train_regression_model()
regression_value = valuator.predict_with_regression_model(
    postcode=3000,
    bedrooms=2,
    bathrooms=1,
    parking=1,
    property_type=1,
    latitude=-33.8688,
    longitude=151.2093
)

# Train and use classification model
train_acc, test_acc = valuator.train_classification_model()
price_category = valuator.predict_with_classification_model(
    postcode=2000,
    bedrooms=2,
    bathrooms=1,
    parking=1,
    property_type=1,
    latitude=-33.8688,
    longitude=151.2093
)
