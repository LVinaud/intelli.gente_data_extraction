from datastructures import ProcessedDataCollection, DataTypes, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import TechEquipamentScrapper
import pandas as pd


class TechEquipamentExtractor(AbstractDataExtractor):

    EXTRACTED_CITY_COL = "CO_MUNICIPIO"
    DTYPE = DataTypes.INT  # resposta binária (0/1), somada por município → int

    DATA_TOPIC = "Educação"
    DATA_POINTS = [  # nomes dos dados que vamos extrair
        "IN_LABORATORIO_INFORMATICA",
        "IN_EQUIP_LOUSA_DIGITAL",
        "IN_EQUIP_MULTIMIDIA",
        "IN_DESKTOP_ALUNO",
        "IN_COMP_PORTATIL_ALUNO",
        "IN_TABLET_ALUNO",
        "IN_INTERNET_APRENDIZAGEM",
    ]

    __SCRAPPER_CLASS: TechEquipamentScrapper

    def __init__(self):
        self.__SCRAPPER_CLASS = TechEquipamentScrapper()

    def __agregate_dfs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrupa por município e ano, somando os indicadores binários."""
        grouped_obj = df.groupby([self.EXTRACTED_CITY_COL, self.YEAR_COLUMN])
        columns_to_sum = [col for col in df.columns if col not in [self.YEAR_COLUMN, self.EXTRACTED_CITY_COL]]
        return grouped_obj[columns_to_sum].sum().reset_index()

    def __change_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = df.columns
        for col in cols:
            if col.lower() == self.EXTRACTED_CITY_COL.lower() or col == self.YEAR_COLUMN:
                continue
            df[col] = df[col].astype("int")
        return df

    def __get_data_collection(self, data_name: str, df: pd.DataFrame,
                               time_series_years: list[int]) -> ProcessedDataCollection:
        new_df = pd.DataFrame()
        new_df[self.YEAR_COLUMN] = df[self.YEAR_COLUMN]
        new_df[self.CITY_CODE_COL] = df[self.EXTRACTED_CITY_COL]
        new_df[self.DTYPE_COLUMN] = self.DTYPE.value
        new_df[self.DATA_IDENTIFIER_COLUMN] = data_name
        new_df[self.DATA_VALUE_COLUMN] = df[data_name]

        return ProcessedDataCollection(
            category=self.DATA_TOPIC,
            dtype=self.DTYPE,
            data_name=data_name,
            time_series_years=time_series_years,
            df=new_df
        )

    def extract_processed_collection(self, years_to_extract: int = 3) -> list[ProcessedDataCollection]:
        data_points: list[YearDataPoint] = self.__SCRAPPER_CLASS.extract_database(years_to_extract)
        time_series_years: list[int] = YearDataPoint.get_years_from_list(data_points)

        joined_df: pd.DataFrame = self._concat_data_points(data_points)
        joined_df = joined_df.dropna()
        joined_df = self.__change_dtypes(joined_df)
        aggregated_df: pd.DataFrame = self.__agregate_dfs(joined_df)

        return [
            self.__get_data_collection(data_point, aggregated_df, time_series_years)
            for data_point in self.DATA_POINTS
        ]