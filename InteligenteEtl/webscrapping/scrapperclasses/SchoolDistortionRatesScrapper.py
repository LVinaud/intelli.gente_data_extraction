import os
import re
import zipfile
import requests
import pandas as pd
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper
from etl_config import get_current_year


class SchoolDistortionRatesScrapper(AbstractScrapper):

    # Headers para simular um browser real (INEP bloqueia requests sem User-Agent)
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-distorcao-idade-serie",
    }

    # Primeiro ano com dados disponíveis
    MIN_YEAR = 2011

    # Padrões de URL por período
    # 2016-presente: padrão novo
    # 2011-2015: padrão antigo (subpasta distorcao_idade_serie)
    URL_PATTERNS = {
        "new": "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/TDI_{year}_MUNICIPIOS.zip",
        "old": "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/distorcao_idade_serie/tdi_municipios_{year}.zip",
    }

    def __init__(self):
        self.files_folder_path = self._create_downloaded_files_dir()

    def __build_download_urls(self) -> list[str]:
        """Constroi as URLs de download, tentando ambos os padrões."""
        urls = []
        current = get_current_year()
        for year in range(self.MIN_YEAR, current + 1):
            if year >= 2016:
                urls.append(self.URL_PATTERNS["new"].format(year=year))
            else:
                urls.append(self.URL_PATTERNS["old"].format(year=year))
        return urls

    def __download_and_extract_zipfiles(self, urls: list[str]) -> None:
        """Baixa os ZIPs via requests e extrai no diretório de dados."""
        download_dir = self.DOWNLOADED_FILES_PATH
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)

        for url in urls:
            year_match = re.search(r'(\d{4})', url.split('/')[-1])
            year_label = year_match.group(1) if year_match else "unknown"
            zip_filename = f"TDI_{year_label}_MUNICIPIOS.zip"
            zip_path = os.path.join(download_dir, zip_filename)

            print(f"Downloading {url.split('/')[-1]} ({year_label})...")
            try:
                response = requests.get(url, headers=self.HEADERS, timeout=120)

                if response.status_code == 404:
                    # Tentar padrão alternativo
                    if "distorcao" not in url:
                        alt = self.URL_PATTERNS["old"].format(year=year_label)
                    else:
                        alt = self.URL_PATTERNS["new"].format(year=year_label)
                    print(f"  Trying alt: {alt.split('/')[-1]}...")
                    response = requests.get(alt, headers=self.HEADERS, timeout=120)

                if response.status_code != 200:
                    print(f"  ✗ HTTP {response.status_code} — skipping {year_label}")
                    continue

                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    print(f"  ✗ Server returned HTML — skipping {year_label}")
                    continue

                if not response.content[:4].startswith(b'PK'):
                    print(f"  ✗ Not a valid ZIP — skipping {year_label}")
                    continue

                with open(zip_path, "wb") as f:
                    f.write(response.content)
                print(f"  ✓ Downloaded ({len(response.content) / 1024 / 1024:.1f} MB)")

                # Extrair para subpasta do ano
                year_dir = os.path.join(download_dir, f"TDI_{year_label}")
                os.makedirs(year_dir, exist_ok=True)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(year_dir)
                os.remove(zip_path)
                print(f"  ✓ Extracted to TDI_{year_label}/")

            except requests.RequestException as e:
                print(f"  ✗ Download failed for year {year_label}: {e}")
            except zipfile.BadZipFile:
                print(f"  ✗ Bad ZIP file for year {year_label}")
                if os.path.exists(zip_path):
                    os.remove(zip_path)

    def __find_spreadsheet(self, folder_path: str) -> str | None:
        """Encontra o arquivo .xlsx ou .xls de municípios dentro da pasta."""
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')) and 'munic' in file.lower():
                    return os.path.join(root, file)
        return None

    def __normalize_columns(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Normaliza nomes de colunas para um padrão consistente."""
        rename_map = {}

        for col in df.columns:
            col_upper = str(col).upper()
            if 'PK_COD_MUNIC' in col_upper:
                rename_map[col] = 'CO_MUNICIPIO'
            elif col_upper in ('DEPENDAD', 'DEPEND'):
                rename_map[col] = 'NO_DEPENDENCIA'
            elif col_upper == 'SIGLA':
                rename_map[col] = 'SG_UF'
            elif col_upper == 'ANO':
                rename_map[col] = 'NU_ANO_CENSO'
            elif col_upper == 'TIPOLOCA':
                rename_map[col] = 'NO_CATEGORIA'
            # TDI columns: normalizar para o padrão mais recente
            elif col_upper == 'TDI_FUN':
                rename_map[col] = 'FUN_CAT_0'

        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    def __process_file(self, file_path: str) -> pd.DataFrame | None:
        """Lê e processa um arquivo de TDI, lidando com diferentes formatos."""
        print(f"  Processing {os.path.basename(file_path)}...")

        try:
            # Primeiro, verificar se existe row com código (row 8)
            df_raw = pd.read_excel(file_path, header=None, nrows=12)

            # Procurar row com nomes de código de colunas
            header_row = None
            for i in range(10):
                vals = [str(v).upper() for v in df_raw.iloc[i].values if pd.notna(v)]
                has_code_col = any(k in v for v in vals for k in ['CO_MUNIC', 'PK_COD', 'NU_ANO', 'NO_REGIAO'])
                if has_code_col:
                    header_row = i
                    break

            if header_row is not None:
                # Formato com nomes de código (2013+)
                df = pd.read_excel(file_path, header=header_row)
            else:
                # Formato antigo (2011-2012): usar nomes descritivos da row 5
                # Row 5 tem: Ano, Região, UF, Código do Município, Nome do Município, Localização
                # Row 6 tem sub-headers dos TDI
                # Dados começam na row 8
                df = pd.read_excel(file_path, header=None, skiprows=8)
                # Construir nomes de colunas manualmente
                # Usar row 5 e 6 para entender, mas nomear diretamente
                col_names_row5 = [str(v) if pd.notna(v) else '' for v in df_raw.iloc[5].values]
                col_names_row6 = [str(v) if pd.notna(v) else '' for v in df_raw.iloc[6].values]

                # Nomear as primeiras colunas
                new_cols = list(df.columns)
                # Col 0=Ano, 1=Região, 2=UF, 3=Código Município, 4=Nome Município
                # Col 5=Localização, 6=Dependência, 7=Total Fundamental TDI
                col_mapping = {
                    0: 'NU_ANO_CENSO',
                    1: 'NO_REGIAO',
                    2: 'SG_UF',
                    3: 'CO_MUNICIPIO',
                    4: 'NO_MUNICIPIO',
                    5: 'NO_CATEGORIA',
                    6: 'NO_DEPENDENCIA',
                    7: 'FUN_CAT_0',
                }
                for idx, name in col_mapping.items():
                    if idx < len(new_cols):
                        new_cols[idx] = name

                df.columns = new_cols

            return df

        except Exception as e:
            print(f"  ✗ Error processing {file_path}: {e}")
            return None

    def __data_dir_process(self, folder_path: str) -> YearDataPoint | None:
        """Processa a pasta extraída, encontrando a planilha de municípios."""
        file_path = self.__find_spreadsheet(folder_path)
        if not file_path:
            print(f"  No spreadsheet found in {folder_path}")
            return None

        year = self.__extract_year_from_path(folder_path)
        df = self.__process_file(file_path)

        if df is not None and not df.empty and year:
            df = self.__normalize_columns(df, year)
            print(f"  ✓ Year {year}: {len(df)} rows")
            return YearDataPoint(df=df, data_year=year)
        return None

    def __extract_year_from_path(self, path: str) -> int | None:
        ano_match = re.search(r'\d{4}', os.path.basename(path))
        if not ano_match:
            ano_match = re.search(r'\d{4}', path)
        return int(ano_match.group(0)) if ano_match else None

    def extract_database(self) -> list[YearDataPoint]:
        year_data_points = []

        urls = self.__build_download_urls()
        print(f"Downloading {len(urls)} years ({self.MIN_YEAR}-{get_current_year()})...")
        self.__download_and_extract_zipfiles(urls)

        # Processar as pastas extraídas
        for item in sorted(os.listdir(self.DOWNLOADED_FILES_PATH)):
            item_path = os.path.join(self.DOWNLOADED_FILES_PATH, item)
            if not os.path.isdir(item_path):
                continue

            year_data_point = self.__data_dir_process(item_path)
            if year_data_point:
                year_data_points.append(year_data_point)
            else:
                print(f"  Processing failed for folder {item}")

        self._delete_download_files_dir()
        return year_data_points
