import pandas as pd
from mwfunctions.image.conversion import bytes2b64_str
from mwfunctions.cloud.storage import read_file_as_bytes

def get_df_chunks(df, chunk_size):
    return [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

def create_column_b64_str(df, image_gs_url_col="image_gs_url", image_b64_str_col_col="b64_str"):
    df[image_b64_str_col_col] = df.apply(lambda df_row: bytes2b64_str(read_file_as_bytes(df_row[image_gs_url_col])), axis=1)
    # for i, df_row in df.iterrows():
    #     img_bytes = read_file_as_bytes(df_row[image_gs_url_col])
    #     df.loc[i, image_b64_str_col_col] = bytes2b64_str(img_bytes)
