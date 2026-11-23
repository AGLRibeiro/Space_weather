import os
from datetime import date
from dotenv import load_dotenv
from src.extract.data_extractor import NASADONKIExtractor

start_date = date(2020, 1, 1)
end_date = date(2025, 12, 31)

#Para testes
start_date = date(2024, 5, 10)
end_date = date(2024, 5, 10)

load_dotenv()

api_key = os.getenv("API_KEY")

Donki = NASADONKIExtractor(api_key)

if __name__ == "__main__":

    Donki.collect_data(
        start_date, end_date
    )
    Donki.save_data(
        output_path="data/bronze"
    )
