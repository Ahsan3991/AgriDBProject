import pandas as pd

from logs import log_operation

def transform(nodes_df, crops_df, sensors_df, log_path, sample_crops_csv_path):

    #check for duplicates in nodes dataframe:
    duplicate_pseudo_id = nodes_df[nodes_df.duplicated(subset=['pseudo_id'], keep=False)]
    duplicate_node_id = nodes_df[nodes_df.duplicated(subset=['node_id'], keep=False)]

    if duplicate_pseudo_id is not None and duplicate_node_id is not None:
        log_operation("Duplicate pseudo_id or duplicate_node_id", log_path)
        if duplicate_pseudo_id is not None:
            log_operation("Duplicate pseudo_id/(s) found", log_path)
        if duplicate_node_id is not None:
            log_operation("Duplicate node_id/(s) found",log_path)
        log_operation('Dropping duplicates', log_path)
        nodes_df.drop_duplicates(subset=["pseudo_id", "node_id"], inplace=True, keep="first")
    else:
        log_operation("No duplicate pseudo_id or duplicate_node_id", log_path)

    #replacing "," if exists in crop description with "." for better reading of csv
    crops_df["crop_features"] = crops_df["crop_features"].str.replace(",", ".", regex=False)
    crops_df.to_csv(sample_crops_csv_path, index=False, header=False)

    #converting dataframe to python objects from pandas objects so psycopg2 can understand dtype and properly execute insert statement and replace null with python's None which is SQL's NULL
    nodes_df = nodes_df.astype(object)
    sensors_df = sensors_df.astype(object)
    crops_df = crops_df.astype(object)

    nodes_df = nodes_df.where(pd.notnull(nodes_df), None)
    sensors_df = sensors_df.where(pd.notnull(sensors_df), None)
    crops_df = crops_df.where(pd.notnull(crops_df), None)

    return nodes_df, crops_df, sensors_df
