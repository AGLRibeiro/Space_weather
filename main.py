import os
from datetime import date
from dotenv import load_dotenv
from src.data_extractor import NASADONKIExtractor_pipeline
from src.transform_data import Transform_data_pipeline
from config.settings import API_KEY, BRONZE_DATA_DIR, SILVER_DATA_DIR

start_date = date(2020, 1, 1)
end_date = date(2025, 12, 31)

#Para testes
start_date = date(2024, 5, 10)
end_date = date(2024, 5, 10)

Donki = NASADONKIExtractor_pipeline(API_KEY)
Transform = Transform_data_pipeline(
    transform_data=["CME"],
    input_dir=BRONZE_DATA_DIR,
    outout_dir=SILVER_DATA_DIR
)

if __name__ == "__main__":

    Donki.collect_data(
        start_date, end_date
    )
    Donki.save_bronze_data(
        output_dir=BRONZE_DATA_DIR
    )

    Transform.run()

#CME, CMEAnalysis, FLR e GST