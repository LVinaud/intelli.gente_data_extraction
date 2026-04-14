from webscrapping.extractorclasses import *
from webscrapping.scrapperclasses import * #um dos poucos casos que fazer isso é uma boa ideia!
from webscrapping.extractorclasses import DatasusDataExtractor, IbgePibCidadesDataExtractor, CityPaymentsExtractor
from webscrapping.scrapperclasses import DatasusDataInfo,IbgePibCidadesScrapper
from webscrapping.extractorclasses import  FormalJobsExtractor, IdhExtractor, IbgeCitiesNetworkExtractor, IbgeMunicExtractor, AnatelExtractor
from webscrapping.extractorclasses import HigherEducaPositionsExtractor, SchoolDistortionRatesExtractor
from apiextractors import IbgeAgregatesApi, IpeaViolenceMapApi
from datastructures import  YearDataPoint
import pandas as pd

def run_datasus(data_info: DatasusDataInfo)->None:
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(data_info)
   for collect in processed_data:
      print(collect.df.info())
      df_renamed = collect.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']]
      for year in sorted(df_renamed['ano'].unique()):
         year_df = df_renamed[df_renamed['ano'] == year]
         year_df.to_csv(f"{collect.data_name}_{year}.csv", index=False)

def run_all_datasus()->None:
   for data_info in DatasusDataInfo:
      print(f"\n{'='*50}")
      print(f"Extraindo: {data_info.name} -> {data_info.value['data_name']}")
      print(f"{'='*50}")
      try:
         run_datasus(data_info)
      except Exception as e:
         print(f"ERRO ao extrair {data_info.name}: {type(e).__name__}: {e}")

def run_ibge_city_gdp()->None:
   extractor = IbgePibCidadesDataExtractor()
   list_ = extractor.extract_processed_collection()

   for collec in list_:
      collec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{collec.data_name}.csv")

def run_MUNIC_base()->list[YearDataPoint]:
   extractor = IbgeMunicExtractor()
   collections = extractor.extract_processed_collection()
   for collect in collections:
      print(collect.df.info())
      collect.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{collect.data_name}.csv")

def run_api_agregados():
   api = IbgeAgregatesApi()
   data_points = api.extract_processed_collection()
   api.print_processed_data(data_points)
   api.save_processed_data_in_csv(data_points,1)

def run_pop_mfe_analf():
   """
   Testa extração de POP_TOT_MFE, POP_ANALF (tabela 9542) e POP_OCVE (tabela 2031)
   via API do IBGE.
   Filtra o datamap para extrair apenas as categorias 'população' e 'trabalho'.
   Constrói o DF final manualmente a partir dos RawDataCollection.
   """
   import pandas as pd
   api = IbgeAgregatesApi()
   # filtra o datamap para extrair só as variáveis novas
   api._data_map = {
      "população": api._data_map["população"],
      "trabalho": api._data_map["trabalho"],
   }

   raw_data = api.extract_raw_data()
   for collection in raw_data:
      data_name = collection.data_name
      rows = []
      for point in collection.data_lines:
         if point.value is None:
            continue
         rows.append({
            'codigo_ibge': int(point.city_id),
            'sigla': data_name,
            'ano': int(point.year),
            'variavel_valor': point.value
         })
      if not rows:
         print(f"Sem dados para {data_name}")
         continue
      df = pd.DataFrame(rows)
      print(f"\n=== {data_name} ===")
      print(df.info())
      print(df.head())
      for year in sorted(df['ano'].unique()):
         year_df = df[df['ano'] == year]
         year_df.to_csv(f"{data_name}_{year}.csv", index=False)
         print(f"Gerado: {data_name}_{year}.csv ({len(year_df)} linhas)")

def run_api_ipea():
   api_extractor = IpeaViolenceMapApi()
   list_data_collect = api_extractor.extract_processed_collection()
   api_extractor.save_processed_data_in_csv(list_data_collect,0)

def run_CAPAG():
   obj = CityPaymentsExtractor()
   list_ = obj.extract_processed_collection()
   list_[0].df.to_csv(f"CAPAG_PROCESSADO{1}.csv",index=False)

def run_formal_jobs():
   obj = FormalJobsExtractor()
   collection_list = obj.extract_processed_collection()
   for collect in collection_list:
      print(collect.df.info())
      print(collect.df.head())
      df_renamed = collect.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']]
      for year in sorted(df_renamed['ano'].unique()):
         year_df = df_renamed[df_renamed['ano'] == year]
         year_df.to_csv(f"POP_OCVE_{year}.csv", index=False)

def run_IDH():
   extractor = IdhExtractor()
   collections = extractor.extract_processed_collection()
   for collect in collections:
      print(collect.df.info())
      df_renamed = collect.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']]
      for year in sorted(df_renamed['ano'].unique()):
         year_df = df_renamed[df_renamed['ano'] == year]
         year_df.to_csv(f"{collect.data_name}_{year}.csv", index=False)

def run_ANATEL():
   extractor = AnatelExtractor()
   list_ = extractor.extract_processed_collection()
   for collect in list_:
      print(collect.df.info())
      collect.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{collect.data_name}.csv")

def ibge_cities_network():
   extractor = IbgeCitiesNetworkExtractor()
   list_ = extractor.extract_processed_collection()
   for collection in list_:
      print(collection.df.info())
      collection.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{collection.data_name}.csv",index=False)

def run_snis():
   extractor = SnisExtractor()
   list_ = extractor.extract_processed_collection()

   for ele in list_:
      ele.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{ele.data_name}.csv")

def run_tech_equipament():
    extractor = TechEquipamentExtractor()
    collection = extractor.extract_processed_collection()
    for colec in collection:
      colec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{colec.data_name}.csv")

def run_Idbe():
   extractor = idebFinalYearsExtractor()
   collection = extractor.extract_processed_collection()
   print(len(collection))

   for colec in collection:
      print(colec.data_name)
      colec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{colec.data_name}.csv")

def run_higher_educa():
   extractor = HigherEducaPositionsExtractor()
   collection = extractor.extract_processed_collection()
   for colec in collection:
      print(colec.df.info())
      colec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{colec.data_name}.csv")

def run_school_distortion():
   extractor = SchoolDistortionRatesExtractor()
   collection = extractor.extract_processed_collection()
   for colec in collection:
      print(colec.df.info())
      colec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(f"{colec.data_name}.csv")

def parse_csv():
   import os
   files:list[str] = os.listdir(os.getcwd())

   for file in files:
      print(file)
      if ".csv" in file:
         df = pd.read_csv(file,index_col=[0])
         df.to_csv(os.path.join(os.getcwd(),file),index=False)
 
if __name__ == "__main__":
   #run_Idbe()
   #run_ibge_city_gdp()
   #run_MUNIC_base()
   #run_all_datasus()
   #run_ANATEL()
   #run_tech_equipament()
   #run_IDH()
   #run_higher_educa()
   #run_formal_jobs()
   run_pop_mfe_analf()
   