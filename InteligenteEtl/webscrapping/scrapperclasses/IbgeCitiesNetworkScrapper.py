import requests, re, zipfile, io
from .AbstractScrapper import AbstractScrapper
import pandas as pd
from typing import Iterable
from datastructures import YearDataPoint


class IbgeCitiesNetworkScrapper(AbstractScrapper):

   URL_2018 = "https://www.ibge.gov.br/geociencias/cartas-e-mapas/redes-geograficas/15798-regioes-de-influencia-das-cidades.html?=&t=downloads"
   URL_2007_ZIP = "https://geoftp.ibge.gov.br/organizacao_do_territorio/divisao_regional/regioes_de_influencia_das_cidades/Regioes_de_influencia_das_cidades_2007/banco_de_dados/banco_de_dados_dos_centros.zip"

   # Colunas padrao de saída (mesmo formato do 2018)
   COL_COD   = "cod_ori"
   COL_NIVEL = "nivel_ori"
   COL_CLASSE = "classe_ori"

   def __get_2018_links(self) -> Iterable[tuple[str, int]]:
      response = requests.get(self.URL_2018)
      page_html: str = response.content.decode()

      file_regex_pattern = r"REGIC\d{4}_Ligacoes_entre_Cidades\.xlsx"
      matches = list(re.finditer(file_regex_pattern, page_html))

      year_regex_pattern = r"(\d{4})"
      get_data_year = lambda x: int(re.findall(year_regex_pattern, x)[0])

      for match in matches:
         match_end = match.end()
         sliced_html = page_html[:match_end]
         url_start = sliced_html.rfind('"')
         file_link = page_html[url_start + 1:match_end]
         yield file_link, get_data_year(file_link)

   def __extract_2018(self) -> list[YearDataPoint]:
      data_points: list[YearDataPoint] = []
      for link, year in self.__get_2018_links():
         df = pd.read_excel(link)
         data_points.append(YearDataPoint(df, year))
      return data_points

   def __extract_2007(self) -> YearDataPoint:
      response = requests.get(self.URL_2007_ZIP)
      z = zipfile.ZipFile(io.BytesIO(response.content))
      with z.open("banco_de_dados_dos_centros.xls") as f:
         df = pd.read_excel(f, sheet_name="Banco de dados dos centros", engine="xlrd")

      # Normalizar colunas para o mesmo formato do 2018
      df = df[["codmundv", "RIC09", "RIC10"]].rename(columns={
         "codmundv": self.COL_COD,
         "RIC09":    self.COL_NIVEL,
         "RIC10":    self.COL_CLASSE,
      })
      return YearDataPoint(df, 2007)

   def extract_database(self) -> list[YearDataPoint]:
      data_points: list[YearDataPoint] = []
      data_points.append(self.__extract_2007())
      data_points.extend(self.__extract_2018())
      return data_points
