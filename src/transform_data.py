import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(acstime)s - %(levelname)s - %(message)s"
)

class Transform_data_pipeline:

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
            column: str
            ) -> pd.DataFrame:

        logging.info("Expandindo as listas e transformando os dicionários em colunas para a coluna '%s'.", column)

        df = data[column].explode()
        df = pd.json_normalize(df)

        logging.info("Dados expandidos e normalizados.")
        return df

    def _save_silver_data(
            self,
            file_name: str,
            data: pd.DataFrame | None = None
            ) -> None:

        if data is None:
            data = self.data
    
        logging.info("Salvando dados no arquivo %s", file_name)

        self.output_dir.mkdir(parents=True, exist_ok=True)

        output_name = self.output_dir / f"{file_name}.parquet"

        data.to_parquet(output_name)

        logging.info("Dados salvos.")

    def _drop_columns(
            self,
            df: pd.DataFrame,
            columns_names: list[str]
            ) -> pd.DataFrame:

        logging.info(f"Removendo colunas {columns_names}")

        df = df.drop(columns=columns_names)

        logging.info(f"Colunas {columns_names} removidas - {len(df.columns)} colunas restantes.")
        return df
        
    def _pipeline_CME(self) -> None:
        self._read_data("CME")

        CMEAnalysis_data = self._transform_nested_data(
            data=self.data,
            column="cmeAnalyses"
            )

        self._save_silver_data(
            data=CMEAnalysis_data,
            file_name="CMEAnalysis"
        )

        self._drop_columns(
            df = self.data,
            columns_names=["cmeAnalyses"]
        )

        self._save_silver_data("CME")

    def run(self) -> None:
        for transform in self.transform_data:
            method_name = f"_pipeline_{transform}"
            method = getattr(self, method_name)
            method()