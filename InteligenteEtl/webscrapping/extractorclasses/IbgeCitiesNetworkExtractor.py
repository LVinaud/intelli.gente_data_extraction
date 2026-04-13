from datastructures import ProcessedDataCollection
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import IbgeCitiesNetworkScrapper
from datastructures import YearDataPoint, DataTypes
import pandas as pd


class IbgeCitiesNetworkExtractor(AbstractDataExtractor):

   DATA_POINTS = {
      "Nível da Hierarquia para as Regiões de Influência das Cidades (VAR09)" : {"column":"nivel_ori","dtype":DataTypes.INT, "sigla":"VAR09"},
      "Classe Denominação da Hierarquia para as Regiões de Influência das Cidades (VAR10)": {"column":"classe_ori","dtype":DataTypes.INT, "sigla":"VAR10"}
   }
   EXTRACTED_CITY_CODE_COL = "cod_ori"
   DATA_CATEGORY = "Arranjos Urbanos"

   # Conversão para valor numérico ordinal (1 = maior hierarquia, 11 = menor)
   NIVEL_TO_NUM = {
      "1A": 1, "1B": 2, "1C": 3,
      "2A": 4, "2B": 5, "2C": 6,
      "3A": 7, "3B": 8,
      "4A": 9, "4B": 10,
      "5": 11,
   }
   CLASSE_TO_NUM = {
      "Grande Metrópole Nacional": 1,
      "Metrópole Nacional":        2,
      "Metrópole":                 3,
      "Capital Regional A":        4,
      "Capital Regional B":        5,
      "Capital Regional C":        6,
      "Centro Sub-Regional A":     7,  "Centro Subregional A": 7,
      "Centro Sub-Regional B":     8,  "Centro Subregional B": 8,
      "Centro de Zona A":          9,
      "Centro de Zona B":          10,
      "Centro Local":              11,
   }

   __SCRAPER_CLASS = IbgeCitiesNetworkScrapper()

   def __get_processed_collection(self, df: pd.DataFrame, data_name: str, time_series: list[int]) -> ProcessedDataCollection:
      data_col_name: str = self.DATA_POINTS[data_name]["column"]
      dtype: DataTypes  = self.DATA_POINTS[data_name]["dtype"]
      sigla: str        = self.DATA_POINTS[data_name]["sigla"]

      df = df.loc[:, [self.EXTRACTED_CITY_CODE_COL, data_col_name, self.YEAR_COLUMN]]
      df = df.dropna()

      df = df.rename(
         {
            self.EXTRACTED_CITY_CODE_COL: self.CITY_CODE_COL,
            data_col_name: self.DATA_VALUE_COLUMN
         },
         axis="columns"
      )
      conversion_map = self.NIVEL_TO_NUM if sigla == "VAR09" else self.CLASSE_TO_NUM
      df[self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].map(conversion_map)
      df[self.DATA_IDENTIFIER_COLUMN] = sigla

      df = df[~df.duplicated(subset=[self.CITY_CODE_COL, self.YEAR_COLUMN], keep="first")]
      df = df.reset_index(drop=True)
      df = self.update_city_code(df, self.CITY_CODE_COL)

      return ProcessedDataCollection(
         category=self.DATA_CATEGORY,
         dtype=dtype,
         data_name=sigla,
         time_series_years=time_series,
         df=df
      )

   def extract_processed_collection(self) -> list[ProcessedDataCollection]:
      data_points: list[YearDataPoint] = self.__SCRAPER_CLASS.extract_database()
      time_series_years: list[int]     = YearDataPoint.get_years_from_list(data_points)
      df: pd.DataFrame                 = self._concat_data_points(data_points)

      data_collections: list[ProcessedDataCollection] = []
      for data_point in self.DATA_POINTS:
         collection = self.__get_processed_collection(df, data_point, time_series_years)
         data_collections.append(collection)

      return data_collections
