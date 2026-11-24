import time
import pandas as pd
import requests
import logging
import json
from pathlib import Path
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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
            max_attempt: int = 5,
            retry_delay: float = 1.0
        ) -> None:

        logging.info("Criando instância do extrator de dados.")

        try:
            self.base_url = base_url

            if not api_key or not api_key.strip():
                raise ValueError("api_key inválida ou vazia.")
                                 
            self.api_key = api_key.strip()

            self.endpoints = (
                endpoints if endpoints is not None
                else self.DEFAULT_ENDPOINTS.copy()
            )

            self.chunk_size = chunk_size
            self.retry_delay = retry_delay
            self.max_attempt = max_attempt

            self.data = {}

            self.session = requests.Session()

        except Exception as e:
            logging.exception(f"Erro: {e}")

    def _fetch_donki_range(
            self, 
            endpoint: str, 
            start_date: date, 
            end_date: date
        ) -> list[dict] | None:

        logging.info(f"Iniciando consulta dos dados de {endpoint} para {start_date}..{end_date}.")

        url = f"{self.base_url}/{endpoint}"
        
        params = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "api_key": self.api_key
        }

        try:
            response = self.session.get(
                url, 
                params=params,
                timeout=30
                )
            
            if response.status_code == 200:
                logging.info("Sucesso!")
                return response.json()

            if response.status_code == 404:
                logging.warning(f"Aviso: endpoint '{endpoint}' não encontrado para {start_date}..{end_date}.")
                return None
            
            logging.error(
            f"Status inesperado {response.status_code} em {endpoint} "
            f"para {start_date}..{end_date}: {response.text[:300]}"
            )

            response.raise_for_status()

        except requests.exceptions.Timeout:
            logging.exception(f"Timeout na requisição.")
            raise

        except requests.exceptions.RequestException as e:
            logging.exception(f"Erro: {e}")
            raise

    def _collect_history(
            self, 
            endpoint: str, 
            start_date: date, 
            end_date: date
        ) -> pd.DataFrame:

        logging.info(f"Iniciando consulta dos dados do {endpoint}.")

        records = []

        current_date = start_date
        attempt = 1
        wait_time = self.retry_delay

        while (current_date <= end_date) and (attempt < self.max_attempt):
                
            chunk_end = min(
                current_date + timedelta(days=self.chunk_size - 1), 
                end_date
                )

            try:
                data = self._fetch_donki_range(
                    endpoint, current_date, chunk_end
                    )

                if data is not None:
                    records.extend(data)
                    
                current_date = chunk_end + timedelta(days=1)
                attempt = 1

            except requests.exceptions.Timeout:
                logging.warning(f"Timeout! Aguardando {wait_time}")
                time.sleep(wait_time)
                wait_time = wait_time * (2 ** (attempt - 1))
                attempt += 1

        if records:
            logging.info("Sucesso!")
            return records
        else:
            logging.info("Sem dados no intervalo.")
            return None

    def collect_data(
            self,
            start_date: date,
            end_date: date,
        ) -> dict[str, pd.DataFrame]:

        logging.info(f"Iniciando coleta de dados.")

        if start_date > end_date:
            logging.error(f"Erro: Data inicial ({start_date}) é posterior à data final ({end_date}).")
            raise ValueError(
                "start_date deve ser anterior ou igual a end_date."
                )
        else:
            self.data = {}
            empty_data = []

            for endpoint in self.endpoints:

                dados = self._collect_history(
                    endpoint, start_date, end_date
                    )

                if dados is not None:
                    self.data[endpoint] = dados
                else:
                    empty_data.append(endpoint)

            if not empty_data:
                logging.info("Todos os dados coletados")
            else:
                logging.warning("%s vazio", empty_data)

            return self.data
    
    def save_data(
            self,
            output_dir: Path | str
            ) -> None:

        logging.info("Salvando dados")

        try:
            target_dir = Path(output_dir)
            target_dir.mkdir(parents = True, exist_ok=True)

            for endpoint, data in self.data.items():

                output_path = target_dir/f"{endpoint}_data.json"

                if data:
                    with output_path.open("w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)

                    logging.info("Arquivo salvo: %s", output_path)

                else: 
                    logging.warning("%s vazio.", endpoint)

        except Exception as e:
            logging.exception("Falha ao salvar os dados em: '%s': %s", output_path, e)
            raise