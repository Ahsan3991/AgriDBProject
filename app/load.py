import psycopg2 as pg2
import os
from dotenv import load_dotenv
from psycopg2 import OperationalError
from logs import log_operation
from io import StringIO #to create an in memory buffer file

def load_data(log_path, env_path, nodes, sensors, crops):

    load_dotenv(dotenv_path=env_path)
    env_url = os.getenv("db_url")
    log_operation(f"getting environment url: {env_url} ", log_path)

    cursor = None
    conn = None
    try:
        conn = pg2.connect(env_url, connect_timeout=60)
        cursor = conn.cursor()
    except OperationalError as e:
        print(e)

    log_operation("database connection established", log_path)

    log_operation("creating tables if not exist already", log_path)

    #create tables
    #same location can have multiple nodes but with different ids
    cursor.execute('''CREATE TABLE IF NOT EXISTS esp_nodes (
    pseudo_id SERIAL PRIMARY KEY,
    node_id INT NOT NULL UNIQUE,
    location VARCHAR(50),
    rpi_id INT NOT NULL);''')

    #same node can have multiple data rows on different timestamps
    cursor.execute('''CREATE TABLE IF NOT EXISTS sensors (
    read_id SERIAL PRIMARY KEY, 
    pseudo_id INT NOT NULL,
    moisture_data INT NOT NULL,
    temperature_data INT NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    CONSTRAINT fk_sensors FOREIGN KEY (pseudo_id) REFERENCES esp_nodes (pseudo_id) ON DELETE CASCADE ON UPDATE CASCADE);''')

    #creating index for faster time range base retrieval
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensors_node_time ON sensors(pseudo_id, last_updated);')

    #multiple crops can be connected to same node but with different crop ids. Unique ensure that for a given node each crop id is unique
    cursor.execute('''CREATE TABLE IF NOT EXISTS crops(
    crop_id VARCHAR(20) PRIMARY KEY, 
    pseudo_id INT NOT NULL, 
    crop_name VARCHAR(50),
    crop_features VARCHAR(500),
    UNIQUE(pseudo_id, crop_id),
    CONSTRAINT fk_crops FOREIGN KEY (pseudo_id) REFERENCES esp_nodes (pseudo_id) ON DELETE CASCADE ON UPDATE CASCADE)''')

    #One customer can have multiple crops and multiple nodes
    cursor.execute('''CREATE TABLE IF NOT EXISTS customer(
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_email VARCHAR(50),
    customer_location VARCHAR(50) NOT NULL, 
    pseudo_id INT NOT NULL,
    crop_id VARCHAR(20) NOT NULL,
    CONSTRAINT fk_customers1 FOREIGN KEY (pseudo_id) REFERENCES esp_nodes (pseudo_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_customers2 FOREIGN KEY (crop_id) REFERENCES crops (crop_id) ON DELETE CASCADE ON UPDATE CASCADE);''')

    conn.commit()

    log_operation("table creation done, Start loading data", log_path)


    log_operation("inserting data into tables", log_path)

    try:
        #on conflict do nothing ensures that duplicate entries are avoided
        nodes_insert_query = """
        INSERT INTO esp_nodes (pseudo_id, node_id, location, rpi_id) VALUES (%s, %s, %s, %s)
        ON CONFLICT (pseudo_id) DO NOTHING;"""
        sensors_insert_query = """
        INSERT INTO sensors (read_id, pseudo_id, moisture_data, temperature_data, last_updated) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (read_id) DO NOTHING;"""
        crops_insert_query = """
        INSERT INTO crops (crop_id, pseudo_id, crop_name, crop_features) VALUES (%s, %s, %s, %s)
        ON CONFLICT (crop_id) DO NOTHING;"""
        #converting dataframes to lists for insert
        cursor.executemany(nodes_insert_query, list(nodes.to_records(index=False)))
        conn.commit()
        cursor.executemany(sensors_insert_query, list(sensors.to_records(index=False)))
        conn.commit()
        cursor.executemany(crops_insert_query, list(crops.to_records(index=False)))
        conn.commit()
    except Exception as e:
        print(f"error while inserting: {e}")
    finally:
        cursor.close()
        conn.close()


