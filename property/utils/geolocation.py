from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="property_locator", timeout=10)

def get_coordinates(address, area_name, state, postcode):
    """
    Fetch latitude and longitude from an address using geopy. 
    Tries to find the most relevant result for the given suburb/area and postcode.

    :param address: Street address (e.g., "79 Franklin St")
    :param area_name: Suburb or area (e.g., "Melbourne")
    :param state: Australian state abbreviation (e.g., "VIC")
    :param postcode: postcode to help disambiguate
    :return: (latitude, longitude) as floats, or (None, None) if not found
    """
    try:
        search_str = f"{address}, {area_name}, {state}, Australia"
        if postcode:
            search_str = f"{address}, {area_name}, {state}, {postcode}, Australia"
        # Get all matches
        locations = geolocator.geocode(search_str, exactly_one=False)
        if not locations:
            return None, None
        # Prioritize by postcode (if provided) and area_name/state
        for location in locations:
            address_str = location.address.lower()
            if postcode and str(postcode) in address_str:
                return location.latitude, location.longitude
            # Fallback: match area_name and state
            if area_name.lower() in address_str and state.lower() in address_str:
                return location.latitude, location.longitude
        # If none match specifically, return the first
        return locations[0].latitude, locations[0].longitude
    except Exception as e:
        print(f"Geolocation error for {address}: {e}")
    return None, None
