import os
from sqlalchemy import create_engine, text

# Define connection parameters
db_server = os.getenv("DB_SERVER", "")
db_name = os.getenv("DB_NAME", "")
db_user = os.getenv("DB_USER", "")
db_pass = os.getenv("DB_PASS", "")
db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

# Create connection string
connection_string = (
    f"mssql+pyodbc://{db_user}:{db_pass}@{db_server}/{db_name}"
    f"?driver={db_driver.replace(' ', '+')}"
)

# Create an engine
engine = create_engine(connection_string)

# Parts tables
create_dim_eyes_parts_table = text("""
IF OBJECT_ID('dbo.staging_eyes', 'U') IS NOT NULL
    DROP TABLE dbo.staging_eyes;
                                   
CREATE TABLE staging_eyes (
    eyes_id NVARCHAR(50),
    eyes_name NVARCHAR(30),
    eyes_class NVARCHAR(30),
    eyes_type NVARCHAR(30))
""")

create_dim_mouth_parts_table = text("""
IF OBJECT_ID('dbo.staging_mouth', 'U') IS NOT NULL
    DROP TABLE dbo.staging_mouth;
                                    
CREATE TABLE staging_mouth (
    mouth_id NVARCHAR(50),
    mouth_name NVARCHAR(30),
    mouth_class NVARCHAR(30),
    mouth_type NVARCHAR(30))
""")

create_dim_ears_parts_table = text("""
IF OBJECT_ID('dbo.staging_ears', 'U') IS NOT NULL
    DROP TABLE dbo.staging_ears;
                                    
CREATE TABLE staging_ears (
    ears_id NVARCHAR(50),
    ears_name NVARCHAR(30),
    ears_class NVARCHAR(30),
    ears_type NVARCHAR(30))
""")

create_dim_horn_parts_table = text("""
IF OBJECT_ID('dbo.staging_horn', 'U') IS NOT NULL
    DROP TABLE dbo.staging_horn;
                                    
CREATE TABLE staging_horn (
    horn_id NVARCHAR(50),
    horn_name NVARCHAR(30),
    horn_class NVARCHAR(30),
    horn_type NVARCHAR(30))
""")

create_dim_back_parts_table = text("""
IF OBJECT_ID('dbo.staging_back', 'U') IS NOT NULL
    DROP TABLE dbo.staging_back;
                                    
CREATE TABLE staging_back (
    back_id NVARCHAR(50),
    back_name NVARCHAR(30),
    back_class NVARCHAR(30),
    back_type NVARCHAR(30))
""")

create_dim_tail_parts_table = text("""
IF OBJECT_ID('dbo.staging_tail', 'U') IS NOT NULL
    DROP TABLE dbo.staging_tail;
                                    
CREATE TABLE staging_tail (
    tail_id NVARCHAR(50),
    tail_name NVARCHAR(30),
    tail_class NVARCHAR(30),
    tail_type NVARCHAR(30))
""")

# Abilities tables
create_dim_mouth_abilities_table = text("""
IF OBJECT_ID('dbo.staging_mouth_abilities', 'U') IS NOT NULL
    DROP TABLE dbo.staging_mouth_abilities;
                                    
CREATE TABLE staging_mouth_abilities (
    mouth_ability_id NVARCHAR(50),
    mouth_ability_name NVARCHAR(30),
    mouth_ability_attack INT,
    mouth_ability_defense INT,
    mouth_ability_energy INT,
    mouth_ability_description NVARCHAR(200))
""")

create_dim_horn_abilities_table = text("""
IF OBJECT_ID('dbo.staging_horn_abilities', 'U') IS NOT NULL
    DROP TABLE dbo.staging_horn_abilities;
                                    
CREATE TABLE staging_horn_abilities (
    horn_ability_id NVARCHAR(50),
    horn_ability_name NVARCHAR(30),
    horn_ability_attack INT,
    horn_ability_defense INT,
    horn_ability_energy INT,
    horn_ability_description NVARCHAR(200))
""")

create_dim_back_abilities_table = text("""
IF OBJECT_ID('dbo.staging_back_abilities', 'U') IS NOT NULL
    DROP TABLE dbo.staging_back_abilities;
                                    
CREATE TABLE staging_back_abilities (
    back_ability_id NVARCHAR(50),
    back_ability_name NVARCHAR(30),
    back_ability_attack INT,
    back_ability_defense INT,
    back_ability_energy INT,
    back_ability_description NVARCHAR(200))
""")

create_dim_tail_abilities_table = text("""
IF OBJECT_ID('dbo.staging_tail_abilities', 'U') IS NOT NULL
    DROP TABLE dbo.staging_tail_abilities;
                                    
CREATE TABLE staging_tail_abilities (
    tail_ability_id NVARCHAR(50),
    tail_ability_name NVARCHAR(30),
    tail_ability_attack INT,
    tail_ability_defense INT,
    tail_ability_energy INT,
    tail_ability_description NVARCHAR(200))
""")

# Axies table
create_dim_axies_table = text("""
IF OBJECT_ID('dbo.staging_axies', 'U') IS NOT NULL
    DROP TABLE dbo.staging_axies;
                                    
CREATE TABLE staging_axies (
    id NVARCHAR(8),
    birth_date INT,
    body_shape NVARCHAR(50),
    class NVARCHAR(30),
    color NVARCHAR(50),
    pureness INT,
    purity INT,
    stage INT,
    hp INT,
    morale INT,
    skill INT,
    speed INT,
    eyes_id NVARCHAR(50),
    mouth_id NVARCHAR(50),
    ears_id NVARCHAR(50),
    horn_id NVARCHAR(50),
    back_id NVARCHAR(50),
    tail_id NVARCHAR(50),
    mouth_ability_id NVARCHAR(50),
    horn_ability_id NVARCHAR(50),
    back_ability_id NVARCHAR(50),
    tail_ability_id NVARCHAR(50)
);
""")

# Orders & Offers table
create_fact_orders_offers_table = text("""
IF OBJECT_ID('dbo.staging_order_offers', 'U') IS NOT NULL
    DROP TABLE dbo.staging_order_offers;
                                    
CREATE TABLE staging_order_offers (
    order_offer_id INT,
    axie_id NVARCHAR(8),
    added_at INT,
    ended_at INT,
    status NVARCHAR(50),
    payment_token NVARCHAR(100),
    base_price DECIMAL(38, 10),
    current_price_usd DECIMAL(38, 10),
    current_price DECIMAL(38, 10),
    ended_price DECIMAL(38, 10),
    bid_or_ask NVARCHAR(10)
);
""")

# Transfers table
create_fact_transfers_table = text("""
IF OBJECT_ID('dbo.staging_transfers', 'U') IS NOT NULL
    DROP TABLE dbo.staging_transfers;
                                    
CREATE TABLE staging_transfers (
    transfer_id NVARCHAR(100),
    axie_id NVARCHAR(8),
    timestamp INT,
    with_price_usd DECIMAL(38, 10),
    with_price DECIMAL(38, 10)
);
""")

# Execute table creation
with engine.connect() as conn:
    # Begin a transaction
    trans = conn.begin()
    try:
        conn.execute(create_dim_eyes_parts_table)
        conn.execute(create_dim_mouth_parts_table)
        conn.execute(create_dim_ears_parts_table)
        conn.execute(create_dim_horn_parts_table)
        conn.execute(create_dim_back_parts_table)
        conn.execute(create_dim_tail_parts_table)
        conn.execute(create_dim_mouth_abilities_table)
        conn.execute(create_dim_horn_abilities_table)
        conn.execute(create_dim_back_abilities_table)
        conn.execute(create_dim_tail_abilities_table)
        conn.execute(create_dim_axies_table)
        conn.execute(create_fact_orders_offers_table)
        conn.execute(create_fact_transfers_table)
        
        # Commit the transaction
        trans.commit()
    except Exception as e:
        # Rollback in case of error
        trans.rollback()
        print(f"Error: {e}")