import os
from datetime import date
from dotenv import load_dotenv
from src.data_extractor import NASADONKIExtractor
from config.settings import API_KEY, BRONZE_DATA_DIR

start_date = date(2020, 1, 1)
end_date = date(2025, 12, 31)

#Para testes
start_date = date(2024, 5, 10)
end_date = date(2024, 5, 10)

Donki = NASADONKIExtractor(API_KEY)

if __name__ == "__main__":

    Donki.collect_data(
        start_date, end_date
    )
    Donki.save_data(
        output_dir=BRONZE_DATA_DIR
    )

#CME, CMEAnalysis, FLR e GST