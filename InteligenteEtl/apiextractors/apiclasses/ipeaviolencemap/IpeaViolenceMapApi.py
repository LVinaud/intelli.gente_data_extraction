from apiextractors.apiclasses.AbstractApiInterface import AbstractApiInterface
from apiextractors.apidataclasses import DataLine, RawDataCollection
from datastructures.DataCollection import ProcessedDataCollection
from datastructures import DataTypes
import requests, os, json
import pandas as pd

class IpeaViolenceMapApi(AbstractApiInterface):
   """
   Classe para extrair dados da API do Ipea Atlas da Violencia.
   Documentacao da API: https://www.ipea.gov.br/atlasviolencia/api

   Observacao sobre THOM (Taxa de Homicidios):
   A serie 20 ("Taxa de homicidios") da API do IPEA AV, que fornecia a taxa ja
   calculada em nivel municipal, foi descontinuada (retorna vazia).
   Por isso, THOM e computada aqui como:

       THOM(city, ano) = Homicidios(city, ano) [IPEA serie 328]
                         / POP_TOT(city, Censo mais proximo do ano)
                         * 100000

   POP_TOT e a Populacao residente dos Censos IBGE (agregado 200 para
   1991/2000/2010 e agregado 4714 para 2022, ambos variavel 93). Para cada
   ano da serie de homicidios, usamos o Censo cronologicamente mais proximo
   (ties vao para o Censo mais recente).
   """

   BASE_URL = "https://www.ipea.gov.br/atlasviolencia/"
   CITY_LEVEL_DATA_CODE = "4"

   # Mapeamento Censo -> (agregado, variavel) da API de agregados do IBGE.
   # Variavel 93 = "Populacao residente" em todos os casos.
   CENSO_AGREGADOS: dict[int, tuple[int, int]] = {
      1991: (200, 93),
      2000: (200, 93),
      2010: (200, 93),
      2022: (4714, 93),
   }

   THOM_SIGLA = "THOM"
   THOM_CATEGORY = "Seguranca Publica"
   POP_TOT_SIGLA = "POP_TOT"

   _data_map: dict[str, dict[str,dict]]

   def __init__(self,path_to_datamap:str="")->None:
      if not path_to_datamap:
         __CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
         api_referen_json_path: str = os.path.join(__CURRENT_DIR,"IpeaViolenceApiDataMap.json")
         self._db_to_api_data_map(api_referen_json_path)
      else:
         self._db_to_api_data_map(path_to_datamap)

   def _db_to_api_data_map(self, datamap_path:str)->None:
      with open(datamap_path,"r") as f:
         file_content= json.load(f)
      if not isinstance(file_content,dict):
         raise RuntimeError("Arquivo JSON de Datamap nao e lido como um dicionario python")
      self._data_map = file_content

   # ---------- IPEA AV ----------
   def __get_api_response(self, data_series_id: int) -> list[dict]:
      SERIES_URL = f"api/v1/valores-series/{data_series_id}/{self.CITY_LEVEL_DATA_CODE}"
      path = self.BASE_URL + SERIES_URL
      response = requests.get(path, timeout=60)
      if response.status_code != 200:
         raise RuntimeError(f"Falha na request IPEA, erro: {response.status_code}")
      response.encoding = 'utf-8'
      return response.json()

   def __parse_api_response(self, response: list[dict], dtype: DataTypes) -> list[DataLine]:
      def valid(x):
         v = x.get("valor")
         return v not in (None, "", "null", "NA")

      filtered_response = list(filter(valid, response))
      parse_dates_to_years = lambda x: x[:x.find("-")]
      dict_to_dataline = lambda x: DataLine(
         city_id=int(x["cod"]),
         year=int(parse_dates_to_years(x["periodo"])),
         value=x["valor"],
         data_type=dtype,
      )
      return list(map(dict_to_dataline, filtered_response))

   # ---------- IBGE POP_TOT ----------
   def __fetch_pop_tot_censo(self, censo_year: int) -> pd.DataFrame:
      """Busca POP_TOT do Censo `censo_year` via IBGE agregados.

      Retorna df com colunas: municipio_cod_ibge (int), pop_tot (float), ano (int).
      """
      if censo_year not in self.CENSO_AGREGADOS:
         raise ValueError(f"Censo {censo_year} nao mapeado em CENSO_AGREGADOS")
      ag, var = self.CENSO_AGREGADOS[censo_year]
      url = (
         f"https://servicodados.ibge.gov.br/api/v3/agregados/{ag}"
         f"/periodos/{censo_year}/variaveis/{var}?localidades=N6[all]"
      )
      r = requests.get(url, timeout=180)
      if r.status_code != 200:
         raise RuntimeError(f"Falha IBGE POP_TOT Censo {censo_year} (status {r.status_code})")
      data = r.json()
      if not data or not data[0].get("resultados"):
         raise RuntimeError(f"Resposta IBGE POP_TOT vazia para Censo {censo_year}")

      rows = []
      for res in data[0]["resultados"]:
         for s in res.get("series", []):
            cod = s["localidade"]["id"]
            val = s["serie"].get(str(censo_year))
            if val in (None, "", "..", "-"):
               continue
            try:
               rows.append({
                  "municipio_cod_ibge": int(cod),
                  "pop_tot": float(val),
                  "ano": int(censo_year),
               })
            except ValueError:
               continue
      df = pd.DataFrame(rows)
      df = df[df["pop_tot"] > 0].copy()
      return df

   def __save_pop_tot_censo_csv(self, df: pd.DataFrame, censo_year: int, out_dir: str) -> str:
      """Salva o CSV POP_TOT_{censo_year}.csv no formato padrao DW."""
      os.makedirs(out_dir, exist_ok=True)
      out = df.rename(columns={"municipio_cod_ibge": "codigo_ibge", "pop_tot": "variavel_valor"}).copy()
      out["sigla"] = self.POP_TOT_SIGLA
      out["ano"] = int(censo_year)
      out = out[["codigo_ibge", "sigla", "ano", "variavel_valor"]]
      path = os.path.join(out_dir, f"{self.POP_TOT_SIGLA}_{censo_year}.csv")
      out.to_csv(path, index=False)
      return path

   # ---------- THOM helpers ----------
   @staticmethod
   def _closest_censo(year: int, censos: list[int]) -> int:
      """Retorna o censo mais proximo do ano; empates vao para o censo mais recente."""
      return min(censos, key=lambda c: (abs(c - year), -c))

   def __save_thom_csvs(self, df: pd.DataFrame, out_dir: str) -> str:
      os.makedirs(out_dir, exist_ok=True)
      df_out = df.rename(columns={"municipio_cod_ibge": "codigo_ibge", "variavel_sigla": "sigla"})
      df_out = df_out[["codigo_ibge", "sigla", "ano", "variavel_valor"]]
      for year in sorted(df_out["ano"].unique()):
         year_df = df_out[df_out["ano"] == year]
         path = os.path.join(out_dir, f"{self.THOM_SIGLA}_{int(year)}.csv")
         year_df.to_csv(path, index=False)
      return out_dir

   # ---------- pipeline ----------
   def extract_processed_collection(
      self,
      thom_out_dir: str = "ipea_thom_output",
      pop_out_dir: str = "pop_tot_output",
   ) -> list[ProcessedDataCollection]:
      """Extrai:
        - POP_TOT para cada Censo (1991, 2000, 2010, 2022)
        - THOM por municipio-ano usando o Censo mais proximo como denominador.

      Retorna uma lista de ProcessedDataCollection:
        - Uma por Censo com sigla POP_TOT (para ingestao no DW)
        - Uma com sigla THOM (serie historica completa de taxas)
      """
      # 1) POP_TOT por Censo
      pop_by_censo: dict[int, pd.DataFrame] = {}
      pop_collections: list[ProcessedDataCollection] = []
      for censo_year in sorted(self.CENSO_AGREGADOS.keys()):
         pop_df = self.__fetch_pop_tot_censo(censo_year)
         pop_by_censo[censo_year] = pop_df

         self.__save_pop_tot_censo_csv(pop_df, censo_year, pop_out_dir)

         # empacota como ProcessedDataCollection para ingestao no DW
         db_df = pop_df.rename(columns={"pop_tot": "variavel_valor"}).copy()
         db_df["variavel_sigla"] = self.POP_TOT_SIGLA
         db_df = db_df[["municipio_cod_ibge", "variavel_sigla", "ano", "variavel_valor"]]
         db_df["municipio_cod_ibge"] = db_df["municipio_cod_ibge"].astype(int)
         db_df["ano"] = db_df["ano"].astype(int)
         pop_collections.append(ProcessedDataCollection(
            category="Demografia",
            dtype=DataTypes.INT,
            data_name=f"{self.POP_TOT_SIGLA}_{censo_year}",
            time_series_years=[censo_year],
            df=db_df,
         ))

      # 2) Homicidios historico (IPEA serie 328)
      first_cat = next(iter(self._data_map))
      hom_series_id = int(self._data_map[first_cat]["HOM_COUNT"]["id"])
      raw = self.__get_api_response(hom_series_id)
      hom_lines = self.__parse_api_response(raw, DataTypes.FLOAT)

      hom_df = pd.DataFrame([
         {"municipio_cod_ibge": int(dl.city_id), "ano": int(dl.year), "hom": float(dl.value)}
         for dl in hom_lines
      ])

      # 3) anexa o censo mais proximo de cada ano e calcula THOM
      censos = sorted(self.CENSO_AGREGADOS.keys())
      hom_df["censo_ref"] = hom_df["ano"].apply(lambda y: self._closest_censo(y, censos))

      # concat de todos os pop_df com coluna "censo_ref" para merge
      pop_all = pd.concat([
         df.rename(columns={"ano": "censo_ref"})[["municipio_cod_ibge", "censo_ref", "pop_tot"]]
         for df in pop_by_censo.values()
      ], ignore_index=True)

      merged = hom_df.merge(pop_all, on=["municipio_cod_ibge", "censo_ref"], how="inner")
      merged["variavel_valor"] = (merged["hom"] / merged["pop_tot"]) * 100000.0
      merged["variavel_sigla"] = self.THOM_SIGLA

      thom_df = merged[["municipio_cod_ibge", "variavel_sigla", "ano", "variavel_valor"]].copy()
      thom_df["municipio_cod_ibge"] = thom_df["municipio_cod_ibge"].astype(int)
      thom_df["ano"] = thom_df["ano"].astype(int)
      thom_df = thom_df.dropna(subset=["variavel_valor"])

      time_series_years = sorted(thom_df["ano"].unique().tolist())
      self.__save_thom_csvs(thom_df, thom_out_dir)

      thom_collection = ProcessedDataCollection(
         category=self.THOM_CATEGORY,
         dtype=DataTypes.FLOAT,
         data_name=self.THOM_SIGLA,
         time_series_years=time_series_years,
         df=thom_df,
      )

      return pop_collections + [thom_collection]
