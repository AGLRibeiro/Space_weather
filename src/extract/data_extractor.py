import time
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import requests

class NASADONKIExtractor:

    DEFAULT_ENDPOINTS = [
        "CME", "CMEAnalysis", "GST", "IPS",
        "FLR", "SEP", "MPC", "RBE", "HSS"
    ]

    def __init__(
            self,
            api_key: str,
            base_url: str = "https://api.nasa.gov/DONKI",
            endpoints: list[str] | None = None,
            chunk_size: int = 120,
            request_delay: float = 1.0
        ) -> None:

        self.base_url = base_url
        self.api_key = api_key

        self.endpoints = (
            endpoints if endpoints is not None
            else self.DEFAULT_ENDPOINTS.copy()
        )

        self.chunk_size = chunk_size
        self.request_delay = request_delay

        self.data = {}
        self.session = requests.Session()

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
            "api_key": self.api_key
        }
        
        response = self.session.get(
                            url, 
                            params=params,
                            timeout=30
                            )

        if response.status_code == 404:
            print(f"Aviso: endpoint '{endpoint}' retornou 404 para {start_date}..{end_date}.")
            return []

        response.raise_for_status()
        return response.json()

    def _collect_history(
            self, 
            endpoint: str, 
            start_date: date, 
            end_date: date
        ) -> pd.DataFrame:
        
        records = []
        current_date = start_date
        
        while current_date <= end_date:
            
            chunk_end = min(
                current_date + timedelta(days=self.chunk_size - 1), 
                end_date
                )

            data = self._fetch_donki_range(
                endpoint, current_date, chunk_end
                )
            
            records.extend(data)
            current_date = chunk_end + timedelta(days=1)
            time.sleep(self.request_delay)  # Para evitar sobrecarga na API, introduzi um pequeno atraso entre as requisições
        
        return pd.json_normalize(records)

    def collect_data(
            self,
            start_date: date,
            end_date: date,
        ) -> dict[str, pd.DataFrame]:

        if start_date > end_date:
            raise ValueError(
                "start_date deve ser anterior ou igual a end_date."
                )

        self.data = {}

        for endpoint in self.endpoints:

            self.data[endpoint] = self._collect_history(
                endpoint, start_date, end_date
                )

        return self.data
    
    def save_data(
            self,
            output_path = Path
            ) -> None:
        
        for endpoint, df in self.data.items():

            path = f"{output_path}/{endpoint}_data.parquet"

            df.to_parquet(
                path, 
                index=False
            )