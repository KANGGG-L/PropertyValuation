CREATE OR REPLACE FUNCTION get_k_nearest_properties(
    lon DOUBLE PRECISION, 
    lat DOUBLE PRECISION, 
    k INT, 
    mode INT, 
    property_type INT, 
    bedroom_num INT, 
    bathroom_num INT, 
    parking_num INT, 
    range_percentage INT
)
RETURNS TABLE (
    property_id INT,
    unit TEXT,
    street_address TEXT,
    bedrooms INT,
    bathrooms INT,
    parking_spaces INT,
    price INT,
    property_type_id INT,
    record_date DATE,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    description TEXT
)
AS $$
DECLARE
    median_price DOUBLE PRECISION;
    lower_limit_price DOUBLE PRECISION;
    upper_limit_price DOUBLE PRECISION;
BEGIN
    -- Input validation for k
    IF k IS NULL OR k <= 0 THEN
        RAISE NOTICE 'Parameter k must be positive.';
        RETURN;
    END IF;

    -- Calculate the median price if range_percentage is not -1
    IF range_percentage != -1 THEN
        IF mode = 0 THEN
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) 
            INTO median_price 
            FROM rental_properties
            WHERE property_type = get_k_nearest_properties.property_type
                AND bedroom_num = get_k_nearest_properties.bedroom_num
                AND bathroom_num = get_k_nearest_properties.bathroom_num
                AND parking_num = get_k_nearest_properties.parking_num;
        ELSIF mode = 1 THEN
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) 
            INTO median_price 
            FROM sold_properties
            WHERE property_type = get_k_nearest_properties.property_type
                AND bedroom_num = get_k_nearest_properties.bedroom_num
                AND bathroom_num = get_k_nearest_properties.bathroom_num
                AND parking_num = get_k_nearest_properties.parking_num;
        END IF;

        -- Check if median_price is NULL (no matches)
        IF median_price IS NULL THEN
            RAISE NOTICE 'No properties found for given filters. Cannot compute median price.';
            RETURN;
        END IF;

        lower_limit_price := median_price * (1 - range_percentage / 100.0);
        upper_limit_price := median_price * (1 + range_percentage / 100.0);
    END IF;

    IF mode = 0 THEN
        RETURN QUERY
        SELECT 
            rp.id, rp.unit, rp.street_address, rp.bedroom_num, rp.bathroom_num, rp.parking_num,
            rp.price, rp.property_type, rp.record_date, rp.latitude, rp.longitude, rp.description
        FROM rental_properties rp
        WHERE rp.property_type = get_k_nearest_properties.property_type
            AND rp.bedroom_num = get_k_nearest_properties.bedroom_num
            AND rp.bathroom_num = get_k_nearest_properties.bathroom_num
            AND rp.parking_num = get_k_nearest_properties.parking_num
            AND (range_percentage = -1 OR rp.price BETWEEN lower_limit_price AND upper_limit_price)
        ORDER BY rp.geom <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)
        LIMIT k;
    ELSIF mode = 1 THEN
        RETURN QUERY
        SELECT 
            sp.id, sp.unit, sp.street_address, sp.bedroom_num, sp.bathroom_num, sp.parking_num,
            sp.price, sp.property_type, sp.record_date, sp.latitude, sp.longitude, sp.description
        FROM sold_properties sp
        WHERE sp.property_type = get_k_nearest_properties.property_type
            AND sp.bedroom_num = get_k_nearest_properties.bedroom_num
            AND sp.bathroom_num = get_k_nearest_properties.bathroom_num
            AND sp.parking_num = get_k_nearest_properties.parking_num
            AND (range_percentage = -1 OR sp.price BETWEEN lower_limit_price AND upper_limit_price)
        ORDER BY sp.geom <-> ST_SetSRID(ST_MakePoint(lon, lat), 4326)
        LIMIT k;
    ELSE
        RAISE EXCEPTION 'Invalid mode. Mode must be 0 (rental_properties) or 1 (sold_properties).';
    END IF;
END;
$$ LANGUAGE plpgsql;