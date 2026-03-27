import os
import re

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # We want to replace `<df>.to_csv(...)` with standardized code
    # ONLY if it's the final output format.
    # AnatelExtractor.py
    if 'AnatelExtractor.py' in filepath:
        content = content.replace("final_df.to_csv(output_file, index=False, sep=';')", 
                                  "final_df[['municipio_cod_ibge', 'variavel_sigla', 'ano', 'variavel_valor']].rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'}).to_csv(output_file, index=False, sep=';')")
    
    # RaisExtractor, EmecExtractor, CnucExtractor
    if 'RaisExtractor.py' in filepath or 'EmecExtractor.py' in filepath or 'CnucExtractor.py' in filepath:
        content = content.replace("df.to_csv(out_path, index=False, sep=self.output_sep, encoding=self.output_encoding)",
                                  "df.rename(columns={self.CITY_CODE_COL: 'codigo_ibge', self.DATA_IDENTIFIER_COLUMN: 'sigla', self.YEAR_COLUMN: 'ano', self.DATA_VALUE_COLUMN: 'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(out_path, index=False, sep=self.output_sep, encoding=self.output_encoding)")

    # test1.py calls collect.df.to_csv(...) and similar
    if 'test1.py' in filepath:
        content = content.replace("collect.df.to_csv(", 
                                  "collect.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(")
        content = content.replace("collec.df.to_csv(", 
                                  "collec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(")
        content = content.replace("collection.df.to_csv(", 
                                  "collection.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(")
        content = content.replace("ele.df.to_csv(", 
                                  "ele.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(")
        content = content.replace("colec.df.to_csv(", 
                                  "colec.df.rename(columns={'municipio_cod_ibge':'codigo_ibge', 'variavel_sigla':'sigla', 'ano':'ano', 'variavel_valor':'variavel_valor'})[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']].to_csv(")

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

base_dir = '/home/joao/Desktop/inteli.gente/intelli.gente_data_extraction/'
process_file(base_dir + 'InteligenteEtl/webscrapping/extractorclasses/AnatelExtractor.py')
process_file(base_dir + 'InteligenteEtl/webscrapping/extractorclasses/RaisExtractor.py')
process_file(base_dir + 'InteligenteEtl/webscrapping/extractorclasses/EmecExtractor.py')
process_file(base_dir + 'InteligenteEtl/webscrapping/extractorclasses/CnucExtractor.py')
process_file(base_dir + 'test/test1.py')
