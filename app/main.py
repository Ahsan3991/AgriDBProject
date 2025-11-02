from load import load_data
from logs import log_operation
from extract import extract
from transform import transform
import os

#path definitions
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '../data')
ENV_PATH = os.path.join(DATA_DIR, '.env')
LOGS_PATH = os.path.join(DATA_DIR, 'logs.txt')
SAMPLE_NODESCSV_PATH = os.path.join(DATA_DIR, 'sample_nodes.csv')
SAMPLE_CROPSCSV_PATH = os.path.join(DATA_DIR, 'sample_crops.csv')
SAMPLE_SENSORSCSV_PATH = os.path.join(DATA_DIR, 'sample_sensors.csv')

def main():

    log_operation('start data extraction', LOGS_PATH)
    nodes_df, crops_df, sensors_df = extract(sample_nodescsv_path=SAMPLE_NODESCSV_PATH, sample_cropscsv_path=SAMPLE_CROPSCSV_PATH, sample_sensorscsv_path=SAMPLE_SENSORSCSV_PATH, log_path= LOGS_PATH)
    log_operation('data is extracted from CSVs', LOGS_PATH)

    log_operation('Start data transformation', LOGS_PATH)
    transformed_nodes_df, transformed_crops_df, transformed_sensors_df = transform(nodes_df, crops_df, sensors_df, log_path=LOGS_PATH, sample_crops_csv_path=SAMPLE_CROPSCSV_PATH)
    log_operation('data transformation is done', LOGS_PATH)

    log_operation('start loading data', LOGS_PATH)
    load_data(log_path=LOGS_PATH, env_path=ENV_PATH, nodes=transformed_nodes_df, sensors=transformed_sensors_df, crops=transformed_crops_df)
    log_operation('Data loaded successfully', LOGS_PATH)




if __name__ == '__main__':
    main()