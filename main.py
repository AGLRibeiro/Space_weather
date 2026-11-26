import os
from datetime import date
from dotenv import load_dotenv
from src.data_extractor import NASA_DONKI_Extractor_Pipeline
from src.transform_data import TransformDataPipeline
from config.settings import API_KEY, BRONZE_DATA_DIR, SILVER_DATA_DIR

start_date = date(2020, 1, 1)
end_date = date(2026, 11, 26)

DataExtractor = NASA_DONKI_Extractor_Pipeline(API_KEY)

TransformData = TransformDataPipeline(
    transform_data=["CMEAnalysis"], 
    input_dir=BRONZE_DATA_DIR,
    outout_dir=SILVER_DATA_DIR
    )

if __name__ == "__main__":

    DataExtractor.run(
        base_url="https://api.nasa.gov/DONKI",
        endpoints=["CMEAnalysis", "HSS"],
        start_date=start_date,
        end_date=end_date,
        output_dir=BRONZE_DATA_DIR
    )

    DataExtractor.run(
            base_url="https://ccmc.gsfc.nasa.gov/DONKI-API/get",
            endpoints=["FLR"],
            start_date=start_date,
            end_date=end_date,
            output_dir=BRONZE_DATA_DIR
        )

    TransformData.run_pipeline_CMEAnalysis()