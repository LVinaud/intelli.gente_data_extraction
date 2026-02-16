from datastructures import ProcessedDataCollection, DataTypes, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import TechEquipamentScrapper
import pandas as pd


class TechEquipamentExtractor(AbstractDataExtractor):
    """
    Extrator unificado para indicadores do Censo Escolar (microdados da educação básica).
    Gera 13 CSVs com variáveis de 4 indicadores, todos filtrados para escolas municipais em atividade.
    """

    EXTRACTED_CITY_COL = "CO_MUNICIPIO"
    DATA_TOPIC = "Educação"

    # Grupo 1: Equipamentos de tecnologia (binárias 0/1 → soma = contagem de escolas)
    BINARY_DATA_POINTS = [
        "IN_LABORATORIO_INFORMATICA",
        "IN_EQUIP_LOUSA_DIGITAL",
        "IN_EQUIP_MULTIMIDIA",
        "IN_DESKTOP_ALUNO",
        "IN_COMP_PORTATIL_ALUNO",
        "IN_TABLET_ALUNO",
        "IN_INTERNET_APRENDIZAGEM",
        "IN_INTERNET",
    ]

    # Grupo 2: Quantidades (soma de valores inteiros)
    QUANTITY_DATA_POINTS = [
        "QT_MAT_FUND",
        "QT_MAT_BAS",
        "QT_DESKTOP_ALUNO",
        "QT_COMP_PORTATIL_ALUNO",
    ]

    # Nome especial para a variável derivada (contagem de escolas por município)
    TOTAL_ESCOLAS_NAME = "TOTAL_ESCOLAS_MUNICIPAIS"

    __SCRAPPER_CLASS: TechEquipamentScrapper

    def __init__(self):
        self.__SCRAPPER_CLASS = TechEquipamentScrapper()

    def __agregate_dfs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrupa por município e ano, somando todas as colunas de dados."""
        all_data_cols = self.BINARY_DATA_POINTS + self.QUANTITY_DATA_POINTS
        grouped_obj = df.groupby([self.EXTRACTED_CITY_COL, self.YEAR_COLUMN])

        # Soma dos indicadores e quantidades
        summed_df = grouped_obj[all_data_cols].sum().reset_index()

        # Contagem de escolas por município (total de linhas por grupo)
        count_df = grouped_obj.size().reset_index(name=self.TOTAL_ESCOLAS_NAME)

        # Juntar soma + contagem
        result = summed_df.merge(count_df, on=[self.EXTRACTED_CITY_COL, self.YEAR_COLUMN])

        return result

    def __change_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        all_data_cols = self.BINARY_DATA_POINTS + self.QUANTITY_DATA_POINTS
        for col in all_data_cols:
            if col in df.columns:
                df[col] = df[col].astype("int")
        return df

    def __get_data_collection(self, data_name: str, df: pd.DataFrame,
                               time_series_years: list[int],
                               dtype: DataTypes) -> ProcessedDataCollection:
        new_df = pd.DataFrame()
        new_df[self.YEAR_COLUMN] = df[self.YEAR_COLUMN]
        new_df[self.CITY_CODE_COL] = df[self.EXTRACTED_CITY_COL]
        new_df[self.DTYPE_COLUMN] = dtype.value
        new_df[self.DATA_IDENTIFIER_COLUMN] = data_name
        new_df[self.DATA_VALUE_COLUMN] = df[data_name]

        return ProcessedDataCollection(
            category=self.DATA_TOPIC,
            dtype=dtype,
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

        collections = []

        # Grupo 1: Binários (soma = contagem de escolas com o equipamento)
        for data_point in self.BINARY_DATA_POINTS:
            collections.append(
                self.__get_data_collection(data_point, aggregated_df, time_series_years, DataTypes.INT)
            )

        # Total de escolas municipais (variável derivada)
        collections.append(
            self.__get_data_collection(self.TOTAL_ESCOLAS_NAME, aggregated_df, time_series_years, DataTypes.INT)
        )

        # Grupo 2: Quantidades (soma de valores inteiros)
        for data_point in self.QUANTITY_DATA_POINTS:
            collections.append(
                self.__get_data_collection(data_point, aggregated_df, time_series_years, DataTypes.INT)
            )

        return collections