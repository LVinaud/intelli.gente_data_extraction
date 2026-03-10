from datastructures import ProcessedDataCollection
from .AbstractDataExtractor import AbstractDataExtractor
from datastructures import YearDataPoint, DataTypes
import pandas as pd
from webscrapping.scrapperclasses.IbgeMunicScrapper import IbgeMunicScrapper
import datamaps


class IbgeMunicExtractor(AbstractDataExtractor):
   
   __scrapper_class: IbgeMunicScrapper = IbgeMunicScrapper()

   def __map_binary_to_bool(self, df:pd.DataFrame)->None:
       df[self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].map({'Sim' : 1, 
                                      'Parcialmente adaptada' : 1, 
                                      'Totalmente adaptada' : 1, 
                                      'Não' : 0, 
                                      '-' : 0, 
                                      'Sem adaptação' : 0, 
                                      'Recusa' : 0, 
                                      'Não informou' : 0, 
                                      'Não sabe' : 0, 
                                      'Não sabe informar' : 0, 
                                      'Legislação não faz referencia ao tipo de bem tombado' : 0})

   def extract_processed_collection(self)->list[ProcessedDataCollection]:
        data_infomations = datamaps.munic_get_data_information()
        data_codes_per_year = datamaps.munic_get_data_codes_per_year()

        data_points:list[YearDataPoint] = self.__scrapper_class.extract_database()
        
        data_collections:list[ProcessedDataCollection] = []
        
        for data_point in data_points:
            year = data_point.data_year
            number_of_cities = len(data_point.df.index)
            year_column = number_of_cities*[year]
            city_code_column = data_point.df['CODMUN']

            for data_name in data_codes_per_year[str(year)]:
                data_id_column = number_of_cities*[data_codes_per_year[str(year)][data_name]]
                value_column = data_point.df[data_codes_per_year[str(year)][data_name]]

                df = pd.DataFrame({self.CITY_CODE_COL : city_code_column, 
                                   self.DATA_IDENTIFIER_COLUMN : data_id_column, 
                                   self.YEAR_COLUMN : year_column,
                                   self.DATA_VALUE_COLUMN : value_column})
                if data_infomations[data_name]['tipo'] == 'bool':
                    self.__map_binary_to_bool(df)
                
                df = self.update_city_code(df, self.CITY_CODE_COL) #atualiza código do município de 6 para 7 dígitos
                data_collections.append(ProcessedDataCollection(
                    category=data_infomations[data_name]['categoria'],
                    dtype=DataTypes.from_string(data_infomations[data_name]['tipo']),
                    data_name=data_name + " - " + data_codes_per_year[str(year)][data_name],
                    time_series_years=[year],
                    df = df
                ))
        return data_collections
