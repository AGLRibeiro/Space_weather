import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(acstime)s - %(levelname)s - %(message)s"
)

class TransformDataPipeline:

    def __init__(
            self,
            transform_data: list[str],
            input_dir: Path | str,
            outout_dir: Path | str
            ) -> None:

        self.transform_data = transform_data
        self.data = pd.DataFrame()
        self.input_dir = Path(input_dir)
        self.output_dir = Path(outout_dir)

    def _read_data(
            self,
            file_name: str
            ) -> None:

        logging.info("Lendo dados.")

        data_path = self.input_dir / f"{file_name}_data.json"
        self.data = pd.read_json(data_path)
        logging.info("Dados carregados.")


    def _transform_nested_data(
            self,
            data: pd.DataFrame,
            columns: list[str] 
            ) -> list[pd.DataFrame]:

        logging.info("Expandindo as listas e transformando os dicionários em colunas para as colunas '%s'.", columns)
        nested_data = []

        for column in columns:

            df = data[column].explode()
            df = pd.json_normalize(df)
            nested_data.append(df)
        
        logging.info("Dados expandidos e normalizados.")
        return nested_data

    def _save_silver_data(
            self,
            file_name: str,
            data: pd.DataFrame | None = None
            ) -> None:

        if data is None:
            data = self.data
    
        logging.info("Salvando dados silver no arquivo %s", file_name)

        self.output_dir.mkdir(parents=True, exist_ok=True)

        output_name = self.output_dir / f"{file_name}.parquet"

        data.to_parquet(output_name)

        logging.info("Dados silver salvos.")

    def _drop_columns(
            self,
            df: pd.DataFrame,
            columns_names: list[str]
            ) -> pd.DataFrame:

        logging.info(f"Removendo colunas {columns_names}")

        df = df.drop(columns=columns_names)

        logging.info(f"Colunas {columns_names} removidas - {len(df.columns)} colunas restantes.")
        return df

    def _normalize_datetime_columns(
            self,
            columns_names = list[str]
    ) -> pd.DataFrame:

        logging.info(f"Convertendo colunas para datetime: {columns_names}.")
        for name in columns_names:
            self.data[name] = pd.to_datetime(
                self.data[name],
                utc=True
            ).dt.tz_convert('America/Sao_Paulo')

        logging.info("Colunas convertidas para datetime")

        
    def run_pipeline_CMEAnalysis(self) -> None:

        logging.info("Executando transformações nos dados de CME")
        self._read_data("CMEAnalysis")

        columns_names_to_drop = ["catalog", "isMostAccurate", "dataLevel", "tilt", "minorHalfWidth", "link", "versionId"]
        columns_to_normalize_datetime = ["time21_5", "submissionTime"]

        self.data = self._drop_columns(
            df=self.data,
            columns_names=columns_names_to_drop
            )

        self._normalize_datetime_columns(columns_names=columns_to_normalize_datetime)

        self._save_silver_data(file_name="CMEAnalysis")
