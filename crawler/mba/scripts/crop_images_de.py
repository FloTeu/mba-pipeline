import pandas as pd
import os
import subprocess

SQL_STATMENT = """SELECT * FROM `mba-pipeline.mba_de.products_images` t0
LEFT JOIN `mba-pipeline.mba_de.products_images_cropped` t1 on t1.file_id = t0.asin
where t0.timestamp < '2021-06-10' and t1.url_gs IS NULL
"""

df = pd.read_gbq(SQL_STATMENT, project_id="mba-pipeline")
total_count = len(df)

for i, df_row in df.iterrows():
    gs_url = df_row["url_gs"]
    #os.spawnl(os.P_NOWAIT, 'gsutil setmeta -h "Content-Type:image/jpeg" {}'.format(gs_url))
    command = 'gsutil setmeta -h "Content-Type:image/jpeg" {}'.format(gs_url)
    #command_list = command.split(" ")
    if i % 2 == 0:
        print(f"{i} of {total_count}")
        ls_output=subprocess.Popen([command], shell=True) 
        ls_output.communicate()  # Will block for 30 seconds
    else:
        subprocess.Popen([command], shell=True) 
