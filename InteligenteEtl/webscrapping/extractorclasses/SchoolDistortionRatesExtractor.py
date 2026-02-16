from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import SchoolDistortionRatesScrapper
from datastructures import ProcessedDataCollection, YearDataPoint, DataTypes
import pandas as pd


class SchoolDistortionRatesExtractor(AbstractDataExtractor):

    DATA_CATEGORY = "Educação"
    DTYPE = DataTypes.FLOAT
    DATA_NAME = "Taxas de distorção idade-série"

    # Nomes das colunas no XLSX (row 8 = header com códigos)
    COL_YEAR = "NU_ANO_CENSO"
    COL_MUNICIPALITY_CODE = "CO_MUNICIPIO"
    COL_MUNICIPALITY_NAME = "NO_MUNICIPIO"
    COL_UF = "SG_UF"
    COL_LOCATION = "NO_CATEGORIA"        # Localização (Total, Urbana, Rural)
    COL_ADMIN_DEP = "NO_DEPENDENCIA"     # Dependência Administrativa (Total, Federal, Estadual, Municipal, Privada)
    COL_TOTAL_EF = "FUN_CAT_0"           # Total - Ensino Fundamental

    __scrapper_class: SchoolDistortionRatesScrapper

    def __init__(self):
        self.__scrapper_class = SchoolDistortionRatesScrapper()

    def extract_processed_collection(self) -> list[ProcessedDataCollection]:
        data_points: list[YearDataPoint] = self.__scrapper_class.extract_database()
        time_series_years: list[int] = YearDataPoint.get_years_from_list(data_points)

        joined_df: pd.DataFrame = self._concat_data_points(data_points)
        joined_df = self.__process_df(joined_df)

        if joined_df is None or joined_df.empty:
            print("Warning: No data after processing.")
            return []

        joined_df = self.__rename_and_add_cols(joined_df)
        joined_df = joined_df.dropna()

        collection = ProcessedDataCollection(
            category=self.DATA_CATEGORY,
            dtype=self.DTYPE,
            data_name=self.DATA_NAME,
            time_series_years=time_series_years,
            df=joined_df
        )

        return [collection]

    def __process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra dados: Localização='Total' e Dependência Administrativa='Municipal'."""
        try:
            # Filtrar: Localização = Total, Dep. Administrativa = Municipal
            df_filtered = df[
                (df[self.COL_LOCATION] == 'Total') &
                (df[self.COL_ADMIN_DEP] == 'Municipal')
            ]

            # Selecionar apenas as colunas necessárias
            result_df = df_filtered[[
                self.COL_MUNICIPALITY_CODE,
                self.COL_TOTAL_EF,
                self.YEAR_COLUMN  # coluna 'ano' adicionada por _concat_data_points
            ]].copy()

            # Renomear código do município para o padrão do projeto
            result_df = result_df.rename({
                self.COL_MUNICIPALITY_CODE: self.CITY_CODE_COL,
            }, axis="columns")

            result_df[self.CITY_CODE_COL] = result_df[self.CITY_CODE_COL].astype("int")
            result_df = result_df.reset_index(drop=True)
            return result_df

        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

    def __rename_and_add_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renomeia coluna de valor e adiciona colunas de identificação."""
        df = df.rename({
            self.COL_TOTAL_EF: self.DATA_VALUE_COLUMN
        }, axis="columns")

        df[self.DATA_IDENTIFIER_COLUMN] = self.DATA_NAME
        df[self.DTYPE_COLUMN] = self.DTYPE.value

        return df
