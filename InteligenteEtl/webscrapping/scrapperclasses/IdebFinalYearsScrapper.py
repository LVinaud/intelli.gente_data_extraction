import os
import re
import requests
import zipfile
import pandas as pd
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper


class IdebFinalYearsScrapper(AbstractScrapper):
    """
    Scrapper para o IDEB - Anos Finais (Ensino Fundamental Regular).
    Baixa o ZIP do INEP que contém um XLSX com os dados históricos do IDEB por município.
    """

    # URL do ZIP — há apenas um arquivo que contém todos os anos históricos
    # O padrão é: divulgacao_anos_finais_municipios_{latest_year}.zip
    DOWNLOAD_URL = "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_{year}.zip"

    # Página do INEP com os resultados do IDEB
    PAGE_URL = "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados"

    # Headers
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados",
    }

    YEAR_REGEX_PATTERN = r"\d{4}"
    EXTRACTED_CITY_COL = "CO_MUNICIPIO"

    def __init__(self):
        self.files_folder_path = self._create_downloaded_files_dir()

    def __find_download_url(self) -> str | None:
        """Tenta URLs de download para anos recentes até encontrar uma válida."""
        from etl_config import get_current_year
        # Tentar os últimos anos (o arquivo mais recente contém todos os dados históricos)
        for year in range(get_current_year(), get_current_year() - 5, -1):
            url = self.DOWNLOAD_URL.format(year=year)
            try:
                response = requests.head(url, headers=self.HEADERS, timeout=15, allow_redirects=True)
                if response.status_code == 200:
                    print(f"Link encontrado: {url}")
                    return url
                else:
                    print(f"  Year {year}: HTTP {response.status_code}")
            except Exception as e:
                print(f"  Year {year}: {e}")
        return None

    def __download_and_extract(self, url: str) -> bool:
        """Baixa o ZIP via requests e extrai no diretório de dados."""
        download_dir = self.DOWNLOADED_FILES_PATH
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)

        filename = url.split('/')[-1]
        zip_path = os.path.join(download_dir, filename)

        print(f"Downloading {filename}...")
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=120)

            if response.status_code != 200:
                print(f"  ✗ HTTP {response.status_code}")
                return False

            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type:
                print(f"  ✗ Server returned HTML instead of ZIP")
                return False

            if not response.content[:4].startswith(b'PK'):
                print(f"  ✗ File is not a valid ZIP")
                return False

            with open(zip_path, "wb") as f:
                f.write(response.content)
            print(f"  ✓ Downloaded ({len(response.content) / 1024 / 1024:.1f} MB)")

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(download_dir)
            os.remove(zip_path)
            print(f"  ✓ Extracted and removed {filename}")
            return True

        except requests.RequestException as e:
            print(f"  ✗ Download failed: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False
        except zipfile.BadZipFile:
            print(f"  ✗ Bad ZIP file")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False

    def __find_xlsx(self) -> str | None:
        """Encontra o XLSX de anos finais municipios dentro das pastas extraídas."""
        for root, dirs, files in os.walk(self.DOWNLOADED_FILES_PATH):
            for file in files:
                if file.endswith(".xlsx") and "anos_finais_municipios" in file.lower():
                    return os.path.join(root, file)
        return None

    def __process_xlsx(self, xlsx_path: str) -> list[YearDataPoint]:
        """
        Lê o XLSX com header na row 9 (códigos de coluna), filtra para rede Municipal
        (e Pública para DF), e separa os dados do IDEB por ano em YearDataPoints.
        """
        print(f"Processing {os.path.basename(xlsx_path)}...")

        # Header na row 9 (0-indexed) = contém os códigos das colunas
        df = pd.read_excel(xlsx_path, header=9)

        print(f"  Raw rows: {len(df)}")

        # Filtrar: Municipal para todos os estados, Pública para DF
        if 'SG_UF' in df.columns:
            df_df = df[(df['SG_UF'] == 'DF') & (df['REDE'] == 'Pública')]
            df_others = df[(df['SG_UF'] != 'DF') & (df['REDE'] == 'Municipal')]
            df = pd.concat([df_df, df_others])
        else:
            df = df[df['REDE'] == 'Municipal']

        print(f"  After filter (Municipal/Pública DF): {len(df)} municipalities")

        # Encontrar colunas VL_OBSERVADO_XXXX (IDEB = N×P)
        ideb_cols = [col for col in df.columns if col.startswith('VL_OBSERVADO_')]

        if not ideb_cols:
            print("  ✗ No VL_OBSERVADO columns found!")
            return []

        print(f"  IDEB columns found: {ideb_cols}")

        # Separar por ano — cada VL_OBSERVADO_XXXX vira um YearDataPoint
        year_data_points = []
        for col in ideb_cols:
            year_match = re.search(r'\d{4}', col)
            if not year_match:
                continue

            year = int(year_match.group(0))

            year_df = pd.DataFrame()
            year_df[self.EXTRACTED_CITY_COL] = df['CO_MUNICIPIO']
            year_df['valor'] = df[col]

            year_data_points.append(YearDataPoint(df=year_df, data_year=year))

        print(f"  ✓ Created {len(year_data_points)} YearDataPoints")
        return year_data_points

    def extract_database(self) -> list[YearDataPoint]:
        # 1. Encontrar o link de download
        url = self.__find_download_url()
        if not url:
            raise RuntimeError("Não foi possível encontrar o link de download do IDEB")

        # 2. Baixar e extrair
        success = self.__download_and_extract(url)
        if not success:
            raise RuntimeError("Falha ao baixar/extrair o ZIP do IDEB")

        # 3. Encontrar o XLSX
        xlsx_path = self.__find_xlsx()
        if not xlsx_path:
            raise RuntimeError("XLSX de anos finais municipios não encontrado")

        # 4. Processar
        year_data_points = self.__process_xlsx(xlsx_path)

        # 5. Limpar
        self._delete_download_files_dir()

        return year_data_points
