
CREATE OR REPLACE FUNCTION get_k_nearest_properties(lon FLOAT, lat FLOAT, k INT, mode INT, property_type INT, bedroom_num INT, bathroom_num INT, parking_num INT, range_percentage INT)
RETURNS TABLE (
    id INT,
    unit TEXT,
    street_address TEXT,
    bedroom_num INT,
    bathroom_num INT,
    parking_num INT,
    price INT,
    property_type INT,
    record_date DATE,
    latitude FLOAT,
    longitude FLOAT,
    description TEXT
) AS $$
DECLARE
    median_price FLOAT;
    lower_limit_price FLOAT;
    upper_limit_price FLOAT;
BEGIN
    -- Calculate the median price if range_percentage is not -1
    IF range_percentage != -1 THEN
        -- Get the median price for rental or sold properties based on mode
        IF mode = 0 THEN
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) 
            INTO median_price 
            FROM rental_properties
            WHERE property_type = property_type 
                AND bedroom_num = bedroom_num 
                AND bathroom_num = bathroom_num 
                AND parking_num = parking_num;
        ELSIF mode = 1 THEN
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) 
            INTO median_price 
            FROM sold_properties
            WHERE property_type = property_type 
                AND bedroom_num = bedroom_num 
                AND bathroom_num = bathroom_num 
                AND parking_num = parking_num;
        END IF;
        
        -- Calculate the price limits based on the percentage
        lower_limit_price := median_price * (1 - range_percentage / 100.0);
        upper_limit_price := median_price * (1 + range_percentage / 100.0);
    END IF;

    -- Conditional query based on the mode parameter
    IF mode = 0 THEN
        RETURN QUERY
        SELECT rental_properties.id, rental_properties.unit, rental_properties.street_address, rental_properties.bedroom_num, 
               rental_properties.bathroom_num, rental_properties.parking_num, rental_properties.price, rental_properties.property_type, 
               rental_properties.record_date, rental_properties.latitude, rental_properties.longitude, rental_properties.description
        FROM rental_properties
        WHERE property_type = property_type
            AND bedroom_num = bedroom_num
            AND bathroom_num = bathroom_num
            AND parking_num = parking_num
            AND (range_percentage = -1 OR rental_properties.price BETWEEN lower_limit_price AND upper_limit_price)
        ORDER BY ST_Distance(rental_properties.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326))
        LIMIT k;
    ELSIF mode = 1 THEN
        RETURN QUERY
        SELECT sold_properties.id, sold_properties.unit, sold_properties.street_address, sold_properties.bedroom_num, 
               sold_properties.bathroom_num, sold_properties.parking_num, sold_properties.price, sold_properties.property_type, 
               sold_properties.record_date, sold_properties.latitude, sold_properties.longitude, sold_properties.description
        FROM sold_properties
        WHERE property_type = property_type
            AND bedroom_num = bedroom_num
            AND bathroom_num = bathroom_num
            AND parking_num = parking_num
            AND (range_percentage = -1 OR sold_properties.price BETWEEN lower_limit_price AND upper_limit_price)
        ORDER BY ST_Distance(sold_properties.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326))
        LIMIT k;
    ELSE
        RAISE EXCEPTION 'Invalid mode. Mode must be 0 (rental_properties) or 1 (sold_properties).';
    END IF;
END;
$$ LANGUAGE plpgsql;
