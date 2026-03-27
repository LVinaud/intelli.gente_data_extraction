import pandas as pd
import os

def split_indicators(csv_path: str):
    print(f"Lendo o arquivo: {csv_path}")
    df = pd.read_csv(csv_path)
    
    # Garantindo que o nome do indicador esteja em maiúsculo na coluna 'sigla'
    df['sigla'] = df['sigla'].str.upper()
    
    # Encontrando todos os indicadores únicos
    indicadores = df['sigla'].unique()
    print(f"Encontrados {len(indicadores)} indicadores: {', '.join(indicadores)}")
    
    output_dir = os.path.dirname(csv_path)
    
    for indicador in indicadores:
        # Filtrando o dataframe apenas para esse indicador
        df_indicador = df[df['sigla'] == indicador]
        
        # O nome do arquivo será o NOME_DO_INDICADOR.csv
        out_filename = f"{indicador}.csv"
        out_path = os.path.join(output_dir, out_filename)
        
        # Salvando o novo CSV
        df_indicador.to_csv(out_path, index=False)
        print(f"Salvo: {out_path} ({len(df_indicador)} linhas)")

if __name__ == "__main__":
    caminho_csv = "/home/joao/Desktop/inteli.gente/intelli.gente_data_extraction/test/anatel_indicators.csv"
    if os.path.exists(caminho_csv):
        split_indicators(caminho_csv)
    else:
        print(f"Arquivo não encontrado: {caminho_csv}")
