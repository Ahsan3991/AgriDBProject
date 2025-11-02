import pandas as pd

from app.logs import log_operation


def extract(sample_nodescsv_path = None, sample_cropscsv_path = None, sample_sensorscsv_path = None, log_path = None):
    nodes_columns = ['pseudo_id', 'node_id', 'location', 'rpi_id']
    crops_columns = ['crop_id', 'pseudo_id', 'crop_name', 'crop_features']
    sensors_columns = ['read_id', 'pseudo_id', 'moisture_data', 'temperature_data', 'last_updated']
    nodes_df = pd.read_csv(sample_nodescsv_path, names=nodes_columns, header=None)
    crops_df = pd.read_csv(sample_cropscsv_path, names=crops_columns, header=None)

    sensors_df = pd.read_csv(sample_sensorscsv_path, names=sensors_columns, header=None)
    if nodes_df is not None and crops_df is not None and sensors_df is not None:
        return nodes_df, crops_df, sensors_df
    elif nodes_df is None:
        log_operation("No nodes data to extract. Enter Nodes to continue", log_path)
        return None
    elif crops_df is None:
        log_operation("No crops data to extract. Crops table might be empty", log_path)
        return nodes_df, sensors_df
    elif sensors_df is None:
        log_operation("No sensor data to extract. Sensor table might be empty", log_path)
        return nodes_df, crops_df
    else:
        log_operation("No data to extract.", log_path)
        return None

