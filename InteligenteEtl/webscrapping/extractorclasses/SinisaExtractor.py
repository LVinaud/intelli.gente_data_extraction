import pandas as pd

from datastructures import DataTypes, ProcessedDataCollection, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import SinisaScrapper


class SinisaExtractor(AbstractDataExtractor):
   """
   Extrator do SINISA.
   Converte os dados extraídos pelo SinisaScrapper no formato do Data Warehouse.
   """

   DATA_TOPIC = "Água e Esgoto"

   RAW_CITY_CODE_COL = "codigo_municipio"
   RAW_YEAR_COL = "ano"
   RAW_INDICATOR_COL = "indicador"
   RAW_VALUE_COL = "valor"

   __SCRAPPER_CLASS: SinisaScrapper

   def __init__(self) -> None:
      self.__SCRAPPER_CLASS = SinisaScrapper()

   def _infer_dtype_from_series(self, series: pd.Series) -> DataTypes:
      non_null = series.dropna()
      if non_null.empty:
         return DataTypes.UNKNOWN

      if all(isinstance(v, bool) for v in non_null):
         return DataTypes.BOOL

      numeric = pd.to_numeric(non_null, errors="coerce")
      if numeric.notna().all():
         as_int = numeric.dropna().apply(lambda x: float(x).is_integer())
         if as_int.all():
            return DataTypes.INT
         return DataTypes.FLOAT

      return DataTypes.STRING

   def _cast_value_column(self, series: pd.Series, dtype: DataTypes) -> pd.Series:
      if dtype == DataTypes.BOOL:
         def parse_bool(value: object) -> bool | None:
            if pd.isna(value):
               return None
            if isinstance(value, bool):
               return value
            lowered = str(value).strip().lower()
            if lowered in {"sim", "s", "yes", "true", "1"}:
               return True
            if lowered in {"nao", "não", "n", "no", "false", "0"}:
               return False
            return None
         return series.apply(parse_bool)

      if dtype == DataTypes.INT:
         numeric = pd.to_numeric(series, errors="coerce")
         return numeric.astype("Int64")

      if dtype == DataTypes.FLOAT:
         return pd.to_numeric(series, errors="coerce")

      return series.apply(lambda x: None if pd.isna(x) else str(x))

   def _create_processed_collection(
      self,
      indicator_name: str,
      df: pd.DataFrame,
      dtype: DataTypes,
      time_series_years: list[int],
   ) -> ProcessedDataCollection | None:
      processed_df = pd.DataFrame()
      processed_df[self.CITY_CODE_COL] = df[self.RAW_CITY_CODE_COL].astype("int64")
      processed_df[self.YEAR_COLUMN] = df[self.RAW_YEAR_COL].astype("int64")
      processed_df[self.DATA_IDENTIFIER_COLUMN] = indicator_name
      processed_df[self.DTYPE_COLUMN] = dtype.value
      processed_df[self.DATA_VALUE_COLUMN] = df[self.RAW_VALUE_COL]

      processed_df = processed_df.dropna(subset=[self.DATA_VALUE_COLUMN])
      if processed_df.empty:
         return None
      processed_df = processed_df.reset_index(drop=True)
      processed_df = self.update_city_code(processed_df, self.CITY_CODE_COL)
      processed_df[self.CITY_CODE_COL] = pd.to_numeric(
         processed_df[self.CITY_CODE_COL], errors="coerce"
      )
      processed_df[self.YEAR_COLUMN] = pd.to_numeric(
         processed_df[self.YEAR_COLUMN], errors="coerce"
      )
      processed_df = processed_df.dropna(subset=[self.CITY_CODE_COL, self.YEAR_COLUMN])
      if processed_df.empty:
         return None
      processed_df[self.CITY_CODE_COL] = processed_df[self.CITY_CODE_COL].astype("int64")
      processed_df[self.YEAR_COLUMN] = processed_df[self.YEAR_COLUMN].astype("int64")

      return ProcessedDataCollection(
         category=self.DATA_TOPIC,
         dtype=dtype,
         data_name=indicator_name,
         time_series_years=time_series_years,
         df=processed_df,
      )

   def extract_processed_collection(self) -> list[ProcessedDataCollection]:
      data_points: list[YearDataPoint] = self.__SCRAPPER_CLASS.extract_database()
      if not data_points:
         return []

      joined_df = self._concat_data_points(data_points, add_year_col=False)
      required_cols = {
         self.RAW_CITY_CODE_COL,
         self.RAW_YEAR_COL,
         self.RAW_INDICATOR_COL,
         self.RAW_VALUE_COL,
      }
      if not required_cols.issubset(set(joined_df.columns)):
         return []

      joined_df[self.RAW_CITY_CODE_COL] = pd.to_numeric(
         joined_df[self.RAW_CITY_CODE_COL], errors="coerce"
      )
      joined_df[self.RAW_YEAR_COL] = pd.to_numeric(
         joined_df[self.RAW_YEAR_COL], errors="coerce"
      )
      joined_df = joined_df.dropna(
         subset=[self.RAW_CITY_CODE_COL, self.RAW_YEAR_COL, self.RAW_INDICATOR_COL]
      )

      collections: list[ProcessedDataCollection] = []
      for indicator_name in sorted(joined_df[self.RAW_INDICATOR_COL].astype("str").unique().tolist()):
         indicator_df = joined_df[joined_df[self.RAW_INDICATOR_COL] == indicator_name].copy()
         if indicator_df.empty:
            continue

         dtype = self._infer_dtype_from_series(indicator_df[self.RAW_VALUE_COL])
         indicator_df[self.RAW_VALUE_COL] = self._cast_value_column(indicator_df[self.RAW_VALUE_COL], dtype)
         indicator_df = indicator_df.dropna(subset=[self.RAW_VALUE_COL])
         if indicator_df.empty:
            continue

         years = sorted(indicator_df[self.RAW_YEAR_COL].astype("int").unique().tolist())
         collection = self._create_processed_collection(
            indicator_name=indicator_name,
            df=indicator_df,
            dtype=dtype,
            time_series_years=years,
         )
         if collection is not None:
            collections.append(collection)

      return collections
