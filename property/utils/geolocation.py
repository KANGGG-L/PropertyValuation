from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="property_locator", timeout=10)

def get_coordinates(address, area_name):
    """
    Fetch latitude and longitude from an address using geopy.
    """
    try:
        location = geolocator.geocode(f'{address}, {area_name}, Victoria, Australia')
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Geolocation error for {address}: {e}")
    return None, None
