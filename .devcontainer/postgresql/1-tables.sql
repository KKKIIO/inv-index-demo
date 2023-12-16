CREATE TABLE
    orders (
        id serial PRIMARY KEY,
        order_status SMALLINT NOT NULL,
        product_id INTEGER NOT NULL,
        provider_id INTEGER DEFAULT NULL,
        create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
ALTER Table orders REPLICA IDENTITY FULL;