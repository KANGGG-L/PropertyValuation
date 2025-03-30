from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

import re

from utils.geolocation import get_coordinates

from utils.constant import *

def filter_digit(price_string):
    match = re.search(r"\d+(?:,\d+)*", price_string)
    if match:
        return int(match.group().replace(",", ""))  # Convert to integer
    return None

def extract_price(listing, mode):

    full_price_string = listing.find_element(By.CSS_SELECTOR, 'p[data-testid="listing-card-price"]').text.strip()
    
    return filter_digit(full_price_string.split(' ', 1)[0])
        
    


def extract_property_type(listing):
    """
    Extract the property type of
    :param postcode: Listing Property Element.
    :return: The type of property
    """

    full_property_type = listing.find_element(By.CLASS_NAME, 'css-11n8uyu').text
    property_type = full_property_type.split("/", 1)[0].strip()[0]

    return property_type

def extract_address(listing, property_type):

    try:
        unprocessed_address = listing.find_element(By.CSS_SELECTOR, 'span[data-testid="address-line1"]').text
        processed_address = unprocessed_address.strip().replace(",", "")

        address_list = processed_address.split('/', 1)

        unit = 'N/A' # Unit only available for apartment OR townhouse
        street_address = processed_address

        if len(address_list) > 1:
            if (property_type == 0 or property_type == 2):
                unit = address_list[0]
            street_address = address_list[1]

        return unit, street_address
    except Exception:
        return None, None

def extract_features(listing):
    # bedroom_num, bathroom_num, parking_num = 1, 1, 0
    property_feature = [1, 1, 0]
    features = listing.find_elements(By.CLASS_NAME, 'css-lvv8is')

    for i in range(3):
        feature_text = features[i].text.strip()[0]
        if feature_text.isdigit():
            property_feature[i] = int(feature_text)

    return property_feature


def extract_area_name(listing):

    area_name = listing.find_element(By.CSS_SELECTOR, 'span[data-testid="address-line2"]').text.strip()

    return area_name.split(" ", 1)[0]



def click_next_button(driver):
    try:
        # Find all paginator navigation buttons (both Prev & Next)
        
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

            # Enforce to scroll to next page button
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
        print(f"Error: {e}")
        return False
    

class Scraper:
    PROPERTY_TYPE_MAPPING = {
        'A': 0,  # Apartment
        'S': 1,  # Studio
        'T': 2,  # Townhouse
        'H': 3,  # House
        'C': 4, # Car space
    }

    def __init__(self, headless=True, teardown=True):
        """
        Initializes the Selenium WebDriver.
        :param headless: Run Chrome in headless mode (default: True).
        :param teardown: Close browser automatically (default: True).
        """

        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")

        self.teardown = teardown

        # Initialize WebDriver
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Ensures the browser is closed when the object is deleted. """
        if self.teardown:
            self.driver.quit()
    
    @classmethod
    def map_property_type(cls, property_type):
        """ Maps scraped property type to an integer value. Defaults to -1 if not found. """
        return cls.PROPERTY_TYPE_MAPPING.get(property_type, -1)

    
    def fetch_rentals(self, postcode, is_exclude_taken=1):
        """
        Scrapes rental listings from Domain.com.au for a given postcode.
        :param postcode: The postcode to search (e.g., "3000").
        :param max_results: The max number of results to fetch (default: 10).
        :return: List of rental properties.
        """

        base_url = f"https://www.domain.com.au/rent/?postcode={postcode}&excludedeposittaken={is_exclude_taken}/"
      
        self.driver.get(base_url)

        rental_data = []

        while True:

            try:
                listings = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'css-1pmltjx'))
                )

                try:
                    # Atomic Element Handling
                    for i in range(len(listings)):
                        listing = self.driver.find_elements(By.CLASS_NAME, 'css-1pmltjx')[i]
                        property_type = self.map_property_type(extract_property_type(listing))
                        unit, street_address = extract_address(listing, property_type)
                        if not street_address:
                            continue

                        bedroom_num, bathroom_num, parking_num = extract_features(listing)
                        if property_type == 4:
                            bedroom_num = bathroom_num = 0

                        price = extract_price(listing, RENTAL_MODE)
                        if not price:
                            continue
                        
                        area_name = extract_area_name(listing)
                        latitude, longitude = get_coordinates(street_address, area_name)
                        if not latitude or not longitude:
                            continue
                        rental_data.append({
                            "unit": unit,
                            "street_address": street_address,
                            "bedroom_num": bedroom_num,
                            "bathroom_num": bathroom_num,
                            "parking_num": parking_num,
                            "price": price,
                            "property_type": property_type,
                            "latitude": latitude,
                            "longitude": longitude
                        })
                
                except StaleElementReferenceException:
                    print("Skipping stale listing element")
                    continue
                except Exception as e:
                    print(f"Skipping listing due to error: {e}")

                if not click_next_button(self.driver):
                    break
                

            except Exception:
                # Also applicable for the case that postcode doesn't exit
                print(f"No valid data be given by domain")
                break

        return rental_data