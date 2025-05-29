from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException, TimeoutException, NoSuchElementException
)

import re

from utils.geolocation import get_coordinates
from utils.constant import *

def filter_digit(price_string):
    """
    Extracts the first integer number (removing commas) found in a price string.

    :param price_string: The string potentially containing a price (e.g., "$2,500 per week").
    :return: The extracted integer price, or None if not found.
    """
    match = re.search(r"\d+(?:,\d+)*", price_string)
    if match:
        return int(match.group().replace(",", ""))  # Convert to integer
    return None

def extract_price(listing, mode):
    """
    Extracts the price from a property listing WebElement.

    :param listing: Selenium WebElement of the property listing.
    :param mode: Not used (reserved for extension).
    :return: The extracted integer price, or None if not found.
    """
    full_price_string = listing.find_element(By.CSS_SELECTOR, 'p[data-testid="listing-card-price"]').text.strip()
    return filter_digit(full_price_string.split(' ', 1)[0])

def extract_property_type(listing):
    """
    Extracts the property type code from a property listing WebElement.

    :param listing: Selenium WebElement of the property listing.
    :return: The property type code as a single character (e.g., 'A' for Apartment).
    """
    full_property_type = listing.find_element(By.CLASS_NAME, 'css-11n8uyu').text
    property_type = full_property_type.split("/", 1)[0].strip()[0]
    return property_type

def extract_address(listing, property_type):
    """
    Extracts the unit (if available) and street address from a property listing.

    :param listing: Selenium WebElement of the property listing.
    :param property_type: Integer property type code.
    :return: Tuple of (unit, street_address) or (None, None) if extraction fails.
    """
    try:
        unprocessed_address = listing.find_element(By.CSS_SELECTOR, 'span[data-testid="address-line1"]').text
        processed_address = unprocessed_address.strip().replace(",", "")

        address_list = processed_address.split('/', 1)

        unit = 'N/A'  # Unit only available for apartment OR townhouse
        street_address = processed_address

        if len(address_list) > 1:
            if (property_type == 0 or property_type == 2):
                unit = address_list[0]
            street_address = address_list[1]

        return unit, street_address
    except Exception:
        return None, None

def extract_features(listing):
    """
    Extracts the number of bedrooms, bathrooms, and parking spaces from a property listing.

    :param listing: Selenium WebElement of the property listing.
    :return: List [bedroom_num, bathroom_num, parking_num], defaults to [1, 1, 0] if not found.
    """
    property_feature = [1, 1, 0]
    features = listing.find_elements(By.CLASS_NAME, 'css-lvv8is')

    for i in range(3):
        feature_text = features[i].text.strip()[0]
        if feature_text.isdigit():
            property_feature[i] = int(feature_text)

    return property_feature

def extract_area_info(listing):
    """
    Extracts the area name and state from a property listing.

    :param listing: Selenium WebElement of the property listing.
    :return: The area name as a string, the state name as a string.
    """
    area_info = listing.find_element(By.CSS_SELECTOR, 'span[data-testid="address-line2"]').text.strip()

    area_name, state, _ = area_info.split(" ", 2)
    return area_name, state

def click_next_button(driver):
    """
    Clicks the "Next" button on a paginated listing page, if available.

    :param driver: Selenium WebDriver object.
    :return: True if next button clicked and page loaded, False otherwise.
    """
    try:
        # Wait for paginator navigation buttons
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="paginator-navigation-button"]'))
        )
        buttons = driver.find_elements(By.CSS_SELECTOR, 'a[data-testid="paginator-navigation-button"]')

        if not buttons:
            print("No pagination buttons found. Possibly the last page or incorrect postcode.")
            return False

        next_button = None
        for button in buttons:
            try:
                button_info = button.find_element(By.CLASS_NAME, 'css-16q9xmc').text.lower()
                if 'next' in button_info:
                    next_button = button
                    break
            except Exception:
                continue  # If the text element is missing, skip to the next button

        # Ensure the button is visible and clickable before clicking
        if next_button and next_button.is_displayed():
            # Scroll to next page button
            driver.execute_script("arguments[0].scrollIntoView({block: 'end'});", next_button)
            next_button.click()
            # Wait for new page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'css-1pmltjx'))
            )
            return True

        print("Next button is not visible or clickable.")
        return False

    except Exception as e:
        print("Have reached the last page or incorrect postcode was given.")
        return False

class Scraper:
    """
    Scraper class to automate extraction of property rental data from domain.com.au using Selenium.

    Attributes:
        PROPERTY_TYPE_MAPPING (dict): Maps property type code to integer.
        driver (webdriver.Chrome): Selenium WebDriver instance.
        teardown (bool): Whether to automatically close browser.
    """

    PROPERTY_TYPE_MAPPING = {
        'A': 0,  # Apartment
        'S': 1,  # Studio
        'T': 2,  # Townhouse
        'H': 3,  # House
        'C': 4,  # Car space
    }

    def __init__(self, headless=True, teardown=True):
        """
        Initializes the Selenium WebDriver.

        :param headless: Run Chrome in headless mode (default: True).
        :param teardown: Close browser automatically when Scraper object is destroyed (default: True).
        """
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
        self.teardown = teardown
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures the browser is closed when the object is deleted.
        """
        if self.teardown:
            self.driver.quit()

    @classmethod
    def map_property_type(cls, property_type):
        """
        Maps scraped property type code (single character) to an integer value.

        :param property_type: Property type code (e.g., 'A', 'H').
        :return: Integer property type, or -1 if not found.
        """
        return cls.PROPERTY_TYPE_MAPPING.get(property_type, -1)


    def scrape(self, state, postcodes, is_rental, is_exclude_taken=1, page_limit=1):
        """
        Scrapes rental or sold property listings from Domain.com.au for a list of postcodes.

        :param state: The state abbreviation for the expected property (e.g., "ACT").
        :param postcodes: The list of postcodes to search (e.g., [3000, 3001]).
        :param is_rental: Bool to indicate if scrape rental properties.
        :param is_exclude_taken: Exclude already taken listings (default: 1).
        :param page_limit: Maximum number of pages to search per postcode (default: 10).

        :return: List of dictionaries, each containing property data fields matching the DB schema.
        """
        res = []
        for postcode in postcodes:
            if is_rental:
                base_url = f"https://www.domain.com.au/rent/?postcode={postcode}&excludedeposittaken={is_exclude_taken}"
            else:
                base_url = f"https://www.domain.com.au/sold-listings/?excludepricewithheld=1&postcode={postcode}"

            print(f"Fetching properties for postcode {postcode} from {base_url}")
            self.driver.get(base_url)
            rental_data = []
            page_count = 0
            while page_count < page_limit:
                try:
                    listings = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, 'css-1pmltjx'))
                    )

                    for i in range(len(listings)):
                        retry = 0
                        while retry < 3:
                            try:
                                listing = self.driver.find_elements(By.CLASS_NAME, 'css-1pmltjx')[i]
                                area_name, extracted_state = extract_area_info(listing)

                                if extracted_state != state:
                                    break  # Skip this listing

                                property_type = self.map_property_type(extract_property_type(listing))
                                unit, street_address = extract_address(listing, property_type)
                                if not street_address:
                                    break
                                bedroom_num, bathroom_num, parking_num = extract_features(listing)
                                if property_type == 4:
                                    bedroom_num = bathroom_num = 0
                                price = extract_price(listing, RENTAL_MODE)
                                if not price:
                                    break
                                latitude, longitude = get_coordinates(street_address, area_name, state, postcode)
                                if not latitude or not longitude:
                                    break
                                rental_data.append({
                                    "postcode": postcode,
                                    "unit": unit,
                                    "street_address": street_address,
                                    "bedroom_num": bedroom_num,
                                    "bathroom_num": bathroom_num,
                                    "parking_num": parking_num,
                                    "price": price,
                                    "property_type": property_type,
                                    "latitude": latitude,
                                    "longitude": longitude,
                                    "description": ""
                                })
                                break  # Listing processed successfully, exit retry loop
                            except StaleElementReferenceException:
                                retry += 1
                                print(f"Stale element at listing {i} for postcode {postcode}, retry {retry}/3.")
                            except Exception as e:
                                print(f"Error processing listing {i} in postcode {postcode}: {e}")
                                break  # Give up on this listing after logging error

                    page_count += 1
                    if not click_next_button(self.driver):
                        break  # No more pages for this postcode

                except TimeoutException:
                    print(f"No listings found or page took too long to load for postcode {postcode}.")
                    break
                except Exception as e:
                    print(f"Error fetching listings for postcode {postcode}: {e}")
                    break

            res.extend(rental_data)

        return res
