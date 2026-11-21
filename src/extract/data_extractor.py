import time
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import requests

class NASA_DONKI_data_extractor:

    def __init__(
            self,
            api_key: str,
            base_url: str = "https://api.nasa.gov/DONKI",
            endpoints: list[str] = ["CME", "CMEAnalysis", "GST", "IPS", "flr", "SEP", "MPC", "RBE", "HSS",]
    ) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.endpoints = endpoints

    def _fetch_donki_range(
            self, 
            endpoint: str, 
            start_date: date, 
            end_date: date
            ) -> list[dict]:
        url = f"{self.base_url}/{endpoint}"
        params = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "api_key": api_key
        }
        resp = requests.get(url, params=params)

        if resp.status_code == 404:
            print(f"Aviso: endpoint '{endpoint}' retornou 404 para {start_date}..{end_date}.")
            return []

        resp.raise_for_status()
        return resp.json()

    def _collect_history(
            self, 
            endpoint: str, 
            start_date: date, 
            end_date: date
            ) -> pd.DataFrame:
        records = []
        cur = start_date
        while cur <= end_date:
            chunk_end = min(cur + timedelta(days=120), end_date)
            data = self._fetch_donki_range(endpoint, cur, chunk_end)
            records.extend(data)
            cur = chunk_end + timedelta(days=1)
            time.sleep(1)  # Para evitar sobrecarga na API, introduzi um pequeno atraso entre as requisições
        return pd.json_normalize(records)

    def collect_data(
            self,
            start_date: date,
            end_date: date,
    ) -> dict[str, pd.DataFrame]:
        dataframes = {}
        for endpoint in self.endpoints:
            df = self._collect_history(endpoint, start_date, end_date)
            dataframes[endpoint] = df
            self.data = dataframes
        return self.data
    
    def save_data_to_csv(self) -> None:
        for endpoint in self.endpoints:
            df = self.data[endpoint]
            df.to_csv(f"{endpoint}_data.csv", index=False)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # pasta base do projeto)

api_key = (PROJECT_ROOT / "api_key.txt").read_text(encoding="utf-8").strip()

Donki = NASA_DONKI_data_extractor(api_key = api_key)

start_date = date(2020, 1, 1)
end_date = date(2025, 12, 31)

start_date = date(2024, 5, 10)
end_date = date(2024, 5, 10)

Donki.collect_data(start_date, end_date)
Donki.save_data_to_csv()

