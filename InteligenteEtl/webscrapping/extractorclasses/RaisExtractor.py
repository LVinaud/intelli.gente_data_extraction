import os
from pathlib import Path
import pandas as pd
from datastructures import ProcessedDataCollection, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses.RaisScrapper import RaisScrapper, RaisDataInfo


class RaisExtractor(AbstractDataExtractor):
   EXTRACTED_CITY_CODE_COL = "Município"
   EXTRACTED_DATA_VALUE_COL = "Total"

   def __init__(
      self,
      headless: bool = True,
      webscrapping_delay_multiplier: int = 1,
      save_csv: bool = True,
      output_dir: str = "data/rais",   # igual o estilo do Anatel: salva em pasta de dados
      output_sep: str = ";",
      output_encoding: str = "utf-8",
   ):
      self.headless = headless
      self.webscrapping_delay_multiplier = webscrapping_delay_multiplier
      self.save_csv = save_csv
      self.output_dir = output_dir
      self.output_sep = output_sep
      self.output_encoding = output_encoding

   def _save_processed_csv(self, df: pd.DataFrame, data_point: RaisDataInfo) -> str:
      out_dir = Path(self.output_dir)
      out_dir.mkdir(parents=True, exist_ok=True)

      year = int(df[self.YEAR_COLUMN].iloc[0]) if self.YEAR_COLUMN in df.columns and len(df) else "NA"
      fname = f"rais_{data_point.name}_{year}.csv"
      out_path = out_dir / fname

      df.to_csv(out_path, index=False, sep=self.output_sep, encoding=self.output_encoding)
      return str(out_path)

   def extract_processed_collection(self) -> list[ProcessedDataCollection]:
      return [self.__get_data_point(dp) for dp in RaisDataInfo]

   def __get_data_point(self, data_point: RaisDataInfo) -> ProcessedDataCollection:
      scr = RaisScrapper(
         data_point,
         headless=self.headless,
         webscrapping_delay_multiplier=self.webscrapping_delay_multiplier,
      )

      data_points: list[YearDataPoint] = scr.extract_database()
      time_series_years: list[int] = YearDataPoint.get_years_from_list(data_points)

      joined_df: pd.DataFrame = self._concat_data_points(data_points, add_year_col=True)
      joined_df = self.__filter_rows(joined_df)

      dtype_str = data_point.value["dtype"].value
      joined_df[self.EXTRACTED_DATA_VALUE_COL] = joined_df[self.EXTRACTED_DATA_VALUE_COL].astype(dtype_str)

      s = joined_df[self.EXTRACTED_CITY_CODE_COL].astype(str).str.strip()

      # sempre: "XX-<nome>" onde XX pode vir como "RO" ou "Ro"
      parts = s.str.split("-", n=1, expand=True)

      joined_df["_uf"] = parts[0].astype(str).str.strip().str.upper()
      joined_df["_city_name"] = parts[1].astype(str).str.strip()

      # remove linhas ruins
      joined_df = joined_df[(joined_df["_uf"].str.len() == 2) & (joined_df["_city_name"] != "")].copy()

      # agora casa (a sua função já está normalizando acento)
      from citiesinfo import match_city_names_with_codes
      joined_df = match_city_names_with_codes(joined_df, "_city_name", "_uf")

      # limpa auxiliares e coluna original
      joined_df = joined_df.drop(columns=["_uf", "_city_name", self.EXTRACTED_CITY_CODE_COL], errors="ignore")

      joined_df = self.__rename_and_add_cols(joined_df, data_point)

      # salva o CSV final “padrão DW” (igual Anatel faz)
      if self.save_csv:
         self._save_processed_csv(joined_df, data_point)

      return ProcessedDataCollection(
         category=data_point.value["topic"],
         dtype=data_point.value["dtype"],
         data_name=data_point.value["data_identifier"],
         time_series_years=time_series_years,
         df=joined_df,
      )

   def __filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
      col = self.EXTRACTED_CITY_CODE_COL
      if col not in df.columns:
         return df

      s = df[col].astype(str).str.strip()

      # padrão UF-NOME (2 letras + hífen + algo)
      keep = s.str.match(r"^[A-Za-z]{2}-", na=False)

      return df.loc[keep].copy()

   def __rename_and_add_cols(self, df: pd.DataFrame, data_point: RaisDataInfo) -> pd.DataFrame:
      # renomeia Total -> valor
      df = df.rename({self.EXTRACTED_DATA_VALUE_COL: self.DATA_VALUE_COLUMN}, axis="columns")

      # metadados
      df[self.DTYPE_COLUMN] = data_point.value["dtype"].value
      df[self.DATA_IDENTIFIER_COLUMN] = data_point.value["data_identifier"]

      # garante apenas colunas do schema (strict)
      df = df[[self.YEAR_COLUMN, self.CITY_CODE_COL, self.DATA_IDENTIFIER_COLUMN, self.DTYPE_COLUMN, self.DATA_VALUE_COLUMN]].copy()
      return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Rodar RAIS Extractor via terminal")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--delay-mult", type=int, default=1)
    args = parser.parse_args()

    ext = RaisExtractor(headless=args.headless, webscrapping_delay_multiplier=args.delay_mult)
    collections = ext.extract_processed_collection()