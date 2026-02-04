from db import Database
import pandas as pd
import os


db = Database(db_name="postgres", user="postgres", password="postgres")

for file in os.listdir("silver"):
    df = pd.read_parquet(f"silver/{file}")
    db.create_table(file.replace(".parquet", ""), df)
    db.insert_data(file.replace(".parquet", ""), df)
    print(f"Data from {file} inserted into database.")

    db.close()