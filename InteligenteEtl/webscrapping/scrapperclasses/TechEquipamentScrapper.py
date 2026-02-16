import os
import re
import requests
import zipfile
import pandas as pd
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper


class TechEquipamentScrapper(AbstractScrapper):
    """
    Scrapper para microdados do Censo Escolar (educação básica).
    Baixa ZIPs do INEP e processa o CSV de microdados, filtrando escolas municipais em atividade.
    Usado por TechEquipamentExtractor e CensoEscolarExtractor.
    """

    BASE_DOWNLOAD_URL = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-escolar/resultados",
    }

    # Todas as colunas necessárias para os 4 indicadores do Censo Escolar
    RELEVANT_COLS = [
        "CO_MUNICIPIO",
        "TP_DEPENDENCIA",
        "TP_SITUACAO_FUNCIONAMENTO",
        # Grupo 1: Equipamentos de tecnologia (binárias 0/1)
        "IN_LABORATORIO_INFORMATICA",
        "IN_EQUIP_LOUSA_DIGITAL",
        "IN_EQUIP_MULTIMIDIA",
        "IN_DESKTOP_ALUNO",
        "IN_COMP_PORTATIL_ALUNO",
        "IN_TABLET_ALUNO",
        "IN_INTERNET_APRENDIZAGEM",
        # Grupo 2: Internet (binária 0/1)
        "IN_INTERNET",
        # Grupo 3: Matrículas
        "QT_MAT_FUND",
        "QT_MAT_BAS",
        # Grupo 4: Quantidades de computadores
        "QT_DESKTOP_ALUNO",
        "QT_COMP_PORTATIL_ALUNO",
    ]

    # Colunas binárias de indicadores de tecnologia
    BINARY_INDICATOR_COLS = [
        "IN_LABORATORIO_INFORMATICA",
        "IN_EQUIP_LOUSA_DIGITAL",
        "IN_EQUIP_MULTIMIDIA",
        "IN_DESKTOP_ALUNO",
        "IN_COMP_PORTATIL_ALUNO",
        "IN_TABLET_ALUNO",
        "IN_INTERNET_APRENDIZAGEM",
        "IN_INTERNET",
    ]

    # Colunas de quantidades (somatórias)
    QUANTITY_COLS = [
        "QT_MAT_FUND",
        "QT_MAT_BAS",
        "QT_DESKTOP_ALUNO",
        "QT_COMP_PORTATIL_ALUNO",
    ]

    # Todas as colunas de dados (exceto filtros e ID)
    ALL_DATA_COLS = BINARY_INDICATOR_COLS + QUANTITY_COLS

    def __init__(self):
        super().__init__()
        self.files_folder_path = self._create_downloaded_files_dir()

    def __build_download_urls(self, years_to_extract: int) -> list[str]:
        from etl_config import get_current_year
        urls = []
        current = get_current_year()
        for year in range(current, current - years_to_extract, -1):
            url = self.BASE_DOWNLOAD_URL.format(year=year)
            urls.append(url)
        return urls

    def __download_and_extract_zipfiles(self, urls: list[str]) -> None:
        download_dir = self.DOWNLOADED_FILES_PATH
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)

        for url in urls:
            filename = url.split('/')[-1]
            zip_path = os.path.join(download_dir, filename)

            print(f"Downloading {filename}...")
            try:
                response = requests.get(url, headers=self.HEADERS, timeout=600, stream=True)

                if response.status_code != 200:
                    print(f"  ✗ HTTP {response.status_code} — skipping")
                    continue

                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    print(f"  ✗ Server returned HTML instead of ZIP — skipping")
                    continue

                total_size = 0
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        total_size += len(chunk)

                print(f"  ✓ Downloaded ({total_size / 1024 / 1024:.1f} MB)")

                with open(zip_path, "rb") as f:
                    magic = f.read(4)
                if not magic.startswith(b'PK'):
                    print(f"  ✗ File is not a valid ZIP — removing")
                    os.remove(zip_path)
                    continue

                print(f"  Extracting {filename}...")
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(download_dir)
                os.remove(zip_path)
                print(f"  ✓ Extracted and removed {filename}")

            except requests.RequestException as e:
                print(f"  ✗ Download failed: {e}")
                if os.path.exists(zip_path):
                    os.remove(zip_path)
            except zipfile.BadZipFile:
                print(f"  ✗ Bad ZIP file — removing")
                if os.path.exists(zip_path):
                    os.remove(zip_path)

    def __find_microdados_csv(self, base_path: str) -> str | None:
        for root, dirs, files in os.walk(base_path):
            for file in files:
                if file.lower().startswith("microdados_ed_basica") and file.lower().endswith(".csv"):
                    return os.path.join(root, file)
        return None

    def __process_csv(self, csv_path: str) -> pd.DataFrame | None:
        print(f"  Reading {os.path.basename(csv_path)}...")
        try:
            df = pd.read_csv(csv_path, sep=";", encoding="latin-1", usecols=self.RELEVANT_COLS)
        except Exception as e:
            print(f"  ✗ Error reading CSV: {e}")
            return None

        print(f"  Raw rows: {len(df)}")

        # Filtrar: Municipal (3) + Em atividade (1)
        df = df[
            (df["TP_DEPENDENCIA"] == 3) &
            (df["TP_SITUACAO_FUNCIONAMENTO"] == 1)
        ]
        print(f"  After filter (municipal, em atividade): {len(df)} schools")

        # Manter apenas CO_MUNICIPIO + colunas de dados
        keep_cols = ["CO_MUNICIPIO"] + self.ALL_DATA_COLS
        df = df[keep_cols]

        # Preencher NaN com 0
        df[self.ALL_DATA_COLS] = df[self.ALL_DATA_COLS].fillna(0)

        return df

    def __extract_year_from_path(self, path: str) -> int | None:
        ano_match = re.search(r'\d{4}', os.path.basename(path))
        if not ano_match:
            ano_match = re.search(r'\d{4}', path)
        return int(ano_match.group(0)) if ano_match else None

    def extract_database(self, years_to_extract: int = 3) -> list[YearDataPoint]:
        urls = self.__build_download_urls(years_to_extract)
        self.__download_and_extract_zipfiles(urls)

        year_data_points = []

        for item in os.listdir(self.DOWNLOADED_FILES_PATH):
            item_path = os.path.join(self.DOWNLOADED_FILES_PATH, item)
            if not os.path.isdir(item_path):
                continue

            csv_path = self.__find_microdados_csv(item_path)
            if not csv_path:
                print(f"  No microdados CSV found in {item}")
                continue

            year = self.__extract_year_from_path(csv_path)
            df = self.__process_csv(csv_path)

            if df is not None and year:
                print(f"  ✓ Year {year}: {len(df)} rows")
                year_data_points.append(YearDataPoint(df=df, data_year=year))
            else:
                print(f"  Processing failed for {item}")

        self._delete_download_files_dir()
        return year_data_points