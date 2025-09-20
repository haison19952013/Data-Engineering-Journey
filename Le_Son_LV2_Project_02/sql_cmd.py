def get_create_table_sql():
    """
    Returns a dictionary of SQL commands to create fact and dimension tables.
    
    Returns:
        dict: Dictionary with table names as keys and CREATE TABLE SQL as values.
    """
    sql_commands = {}
    
    # Fact table: fact_sales
    sql_commands['fact_sales'] = """
    CREATE TABLE IF NOT EXISTS fact_sales (
        sales_key VARCHAR PRIMARY KEY,
        full_date VARCHAR NOT NULL,
        full_time VARCHAR NOT NULL,
        ip_key VARCHAR NOT NULL,
        user_agent_key VARCHAR NOT NULL,
        product_key VARCHAR NOT NULL,
        referrer_url VARCHAR,
        collection VARCHAR,
        option TEXT,
        email_address VARCHAR,
        resolution VARCHAR,
        user_id_db VARCHAR,
        device_id VARCHAR,
        api_version VARCHAR,
        store_id VARCHAR,
        local_time VARCHAR,
        show_recommendation VARCHAR,
        recommendation TEXT,
        utm_source VARCHAR,
        utm_medium VARCHAR
    );
    """
    
    # Dimension table: dim_date
    sql_commands['dim_date'] = """
    CREATE TABLE IF NOT EXISTS dim_date (
        full_date VARCHAR PRIMARY KEY,
        day_of_week INTEGER NOT NULL,
        day_of_week_short VARCHAR NOT NULL,
        is_weekday_or_weekend VARCHAR NOT NULL,
        day_of_month INTEGER NOT NULL,
        year_month VARCHAR NOT NULL,
        month INTEGER NOT NULL,
        day_of_year INTEGER NOT NULL,
        week_of_year INTEGER NOT NULL,
        quarter_number INTEGER NOT NULL,
        year INTEGER NOT NULL
    );
    """
    
    # Dimension table: dim_product
    sql_commands['dim_product'] = """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key VARCHAR PRIMARY KEY,
        name VARCHAR,
        current_url VARCHAR,
        language_code VARCHAR
    );
    """
    
    # Dimension table: dim_location
    sql_commands['dim_location'] = """
    CREATE TABLE IF NOT EXISTS dim_location (
        ip_key VARCHAR PRIMARY KEY,
        ip_address VARCHAR NOT NULL,
        country_code VARCHAR,
        country_name VARCHAR,
        region_name VARCHAR,
        city_name VARCHAR
    );
    """
    
    # Dimension table: dim_user_agent
    sql_commands['dim_user_agent'] = """
    CREATE TABLE IF NOT EXISTS dim_user_agent (
        user_agent_key VARCHAR PRIMARY KEY,
        user_agent TEXT NOT NULL,
        browser VARCHAR,
        os VARCHAR
    );
    """
    
    return sql_commands