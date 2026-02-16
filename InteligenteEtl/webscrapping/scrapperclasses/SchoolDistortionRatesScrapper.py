import os
import re
import zipfile
import requests
import pandas as pd
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper
from etl_config import get_current_year


class SchoolDistortionRatesScrapper(AbstractScrapper):

    # URL base do INEP para os dados de TDI por município
    # Padrão: https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/TDI_{year}_MUNICIPIOS.zip
    BASE_DOWNLOAD_URL = "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{year}/TDI_{year}_MUNICIPIOS.zip"

    # Headers para simular um browser real (INEP bloqueia requests sem User-Agent)
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-distorcao-idade-serie",
    }

    def __init__(self, years_to_extract: int = 10):
        self.files_folder_path = self._create_downloaded_files_dir()
        self.years_to_extract = years_to_extract

    def __build_download_urls(self) -> list[str]:
        """Constroi as URLs de download para cada ano, seguindo o padrão fixo do INEP."""
        urls = []
        current = get_current_year()
        for year in range(current, current - self.years_to_extract, -1):
            url = self.BASE_DOWNLOAD_URL.format(year=year)
            urls.append(url)
        return urls

    def __download_and_extract_zipfiles(self, urls: list[str]) -> None:
        """Baixa os ZIPs via requests (não Selenium) e extrai no diretório de dados."""
        download_dir = self.DOWNLOADED_FILES_PATH
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)

        for url in urls:
            year_match = re.search(r'(\d{4})', url.split('/')[-1])
            year_label = year_match.group(1) if year_match else "unknown"
            zip_filename = f"TDI_{year_label}_MUNICIPIOS.zip"
            zip_path = os.path.join(download_dir, zip_filename)

            print(f"Downloading {url}...")
            try:
                response = requests.get(url, headers=self.HEADERS, timeout=120)

                if response.status_code != 200:
                    print(f"  ✗ HTTP {response.status_code} — skipping year {year_label}")
                    continue

                # Verificar se é realmente um ZIP (INEP pode retornar HTML mesmo com 200)
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    print(f"  ✗ Server returned HTML instead of ZIP — skipping year {year_label}")
                    continue

                # Verificar magic bytes do ZIP (PK\x03\x04)
                if not response.content[:4].startswith(b'PK'):
                    print(f"  ✗ Downloaded file is not a valid ZIP — skipping year {year_label}")
                    continue

                with open(zip_path, "wb") as f:
                    f.write(response.content)
                print(f"  ✓ Downloaded {zip_filename} ({len(response.content) / 1024 / 1024:.1f} MB)")

                # Extrair o ZIP
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(download_dir)
                os.remove(zip_path)
                print(f"  ✓ Extracted and removed {zip_filename}")

            except requests.RequestException as e:
                print(f"  ✗ Download failed for year {year_label}: {e}")
            except zipfile.BadZipFile:
                print(f"  ✗ Bad ZIP file for year {year_label} — removing")
                if os.path.exists(zip_path):
                    os.remove(zip_path)

    def __data_dir_process(self, folder_path: str) -> YearDataPoint | None:
        """Processa a pasta extraída, encontrando o XLSX de municípios e lendo os dados."""
        files_list = os.listdir(folder_path)

        for file in files_list:
            if file.endswith(".xlsx") and "TDI_MUNICIPIOS" in file:
                file_path = os.path.join(folder_path, file)
                print(f"Processing {file}...")

                # Ler com header na row 8 (0-indexed) que contém os nomes de código das colunas
                # (NU_ANO_CENSO, NO_REGIAO, SG_UF, CO_MUNICIPIO, NO_MUNICIPIO, NO_CATEGORIA, NO_DEPENDENCIA, FUN_CAT_0, ...)
                df = pd.read_excel(file_path, header=8)

                if df is not None and not df.empty:
                    year = self.__extract_year_from_path(folder_path)
                    if year:
                        return YearDataPoint(df=df, data_year=year)
                    else:
                        print(f"  Failed to extract year from path: {folder_path}")
                else:
                    print(f"  Processing failed for: {file_path}")

        print(f"No relevant .xlsx files found in {folder_path}")
        return None

    def __extract_year_from_path(self, path: str) -> int | None:
        ano_match = re.search(r'\d{4}', os.path.basename(path))
        if not ano_match:
            ano_match = re.search(r'\d{4}', path)
        if ano_match:
            year = int(ano_match.group(0))
            print(f"  Year extracted: {year}")
            return year
        else:
            print("  Failed to extract year.")
            return None

    def extract_database(self) -> list[YearDataPoint]:
        year_data_points = []

        # Construir os links e baixar
        urls = self.__build_download_urls()
        self.__download_and_extract_zipfiles(urls)

        # Processar as pastas extraídas
        inner_items = os.listdir(self.DOWNLOADED_FILES_PATH)
        for item in inner_items:
            item_path = os.path.join(self.DOWNLOADED_FILES_PATH, item)
            if not os.path.isdir(item_path):
                continue

            year_data_point = self.__data_dir_process(item_path)
            if year_data_point:
                year_data_points.append(year_data_point)
            else:
                print(f"Processing failed for folder {item_path}")

        self._delete_download_files_dir()
        return year_data_points
