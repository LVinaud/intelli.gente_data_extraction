import pandas as pd
import os
import unicodedata
import re
from etl_config import get_config

"""
Módulo para pegar informações sobre os municípios (atualmente 5570) vindos de bases oficiais do IBGE.
Os dados estão reunidos no arquivo "info_municipios_ibge.csv" e são extraidos por meio de funções nesse módulo.
"""

__CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
__CSV_FILE_PATH = os.path.join(__CURRENT_DIR,"info_municipios_ibge.csv")
__CITY_CODE_COL =  get_config("CITY_CODE_COL")
__CITY_NAME_COL = get_config("CITY_NAME_COL")



def get_city_codes()->list[int]:
   """
   Retorna a lista de códigos de todos os municípios
   """
   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   list_of_codes: list[int] = list(df[__CITY_CODE_COL].astype("int64"))
   return list_of_codes

def get_city_names()->list[str]:
   """
   Retorna a lista de nomes de todos os municípios
   """
   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   list_of_city_names: list[str] = list(df[__CITY_NAME_COL])
   return list_of_city_names

def get_city_codes_names_map(codes_as_keys:bool = False)->dict[str,int]:
   """
   Retorna um dict com o nome de um município como key e o código como o valor por padrão, existe um argumento para trocar isso

   Args:
      codes_as_keys (bool): por padrão falso, retorna o nome do município como key. Se for true retorna o código como chave e o  nome como valor
   """
   list_of_codes:list[int] = get_city_codes()
   list_of_names:list[str] = get_city_names()

   if not codes_as_keys:
      return {name:code for name, code in zip(list_of_names,list_of_codes)}
   else:
      return {code:name for name, code in zip(list_of_names,list_of_codes)}

def get_number_of_cities()->int:
   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   return len(df[__CITY_CODE_COL])

def get_city_code_from_string(city_name:str,city_state:str)->int:
   """
   Warning: Essa função não deve ser usada para transformar dataframes completos, pois ele é ineficiente

   Dado o nome de um município e a sigla do Estado dele, retorna o código do IBGE que representa esse município.

   Args:
      city_name (str): nome do município
      city_state (str): Sigla do estado a qual o município pertence (ex: SP,RS...)

   Return:
      (int): Código do município do IBGE (7 Dígitos) do município
   """
   parse_string = lambda x: x.lower().replace(" ","") #parsing nas strings
   city_name = parse_string(city_name)

   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)

   df = df[df["sigla_uf"] == city_state] #filtra por estado
   df["nome_municipio"] = df["nome_municipio"].apply(parse_string) #parsing na coluna de nome de municípios

   df = df[ df["nome_municipio"] == city_name]

   if df.empty or df.shape[0] > 1:
      return -1
   
   return df["codigo_municipio"].iloc[-1]

def match_city_names_with_codes(df_with_city_names: pd.DataFrame, city_names_col: str, states_col: str) -> pd.DataFrame:
   """
   Igual ao original, mas robusto a acentos (São Paulo vs Sao Paulo), hífens/apóstrofos
   e espaços múltiplos. Mantém o comportamento: se não casar, some (inner join).
   """

   def normalize(s: str) -> str:
      if pd.isna(s):
         return ""
      s = str(s).strip().lower()

      # remove acentos/diacríticos
      s = unicodedata.normalize("NFKD", s)
      s = "".join(ch for ch in s if not unicodedata.combining(ch))

      # normaliza pontuação comum (hífen, apóstrofo etc.)
      s = re.sub(r"[-'`´’\.]", "", s)

      # remove espaços
      s = re.sub(r"\s+", "", s)
      return s

   df_ref: pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)

   df_ref["nome_municipio_norm"] = df_ref["nome_municipio"].apply(normalize)
   df_ref["sigla_uf_norm"] = df_ref["sigla_uf"].astype(str).str.strip().str.upper()

   df_filtered = df_ref.loc[:, ["nome_municipio_norm", "sigla_uf_norm", "codigo_municipio"]]

   df_with_city_names = df_with_city_names.copy()
   df_with_city_names["_city_norm"] = df_with_city_names[city_names_col].apply(normalize)
   df_with_city_names["_uf_norm"] = df_with_city_names[states_col].astype(str).str.strip().str.upper()

   merged = df_with_city_names.merge(
      df_filtered,
      how="inner",
      left_on=["_city_norm", "_uf_norm"],
      right_on=["nome_municipio_norm", "sigla_uf_norm"],
   )

   # limpa colunas auxiliares
   merged = merged.drop(columns=["_city_norm", "_uf_norm", "nome_municipio_norm", "sigla_uf_norm"], errors="ignore")

   return merged





