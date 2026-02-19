import os, time, re, requests, zipfile
import pandas as pd
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper


class HigherEducaPositionsScrapper(AbstractScrapper):
    """
    Classe que realiza o Webscrapping dos dados de vagas no ensino superior do INEP.

    Os dados dessa fonte tem uma peculiaridade, tem cerca de 3300 municípios na base original e depois da filtragem
    para registros que obedecem a especificação (ex: cursos não online) apenas cerca de 1100 municípios sobram. Porém os 
    2200 municípios removidos não são "nulos" por si, os dados foram coletados pelo mas a qntd de vagas buscadas é 0, portanto esse código leva isso
    em consideração
    """
    URL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"

    # Headers para simular browser real (INEP bloqueia headless/sem User-Agent)
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior",
    }

    def __init__(self):
        self._create_downloaded_files_dir()

    # Primeiro ano com formato moderno (CURSOS CSV com colunas padronizadas)
    MIN_YEAR = 2009

    def extract_database(self) -> list[YearDataPoint]:
        links: list[str] = self.__get_file_links()
        print(f"Links encontrados ({len(links)} anos): {links[0]} ... {links[-1]}")
        self.__download_and_extract_zipfiles(links)
        time.sleep(2)

        year_data_points = []

        inner_folder = os.listdir(self.DOWNLOADED_FILES_PATH)

        for folder in inner_folder:
            folder_correct_path = os.path.join(self.DOWNLOADED_FILES_PATH, folder)
            if not os.path.isdir(folder_correct_path):
                continue

            year_data_point = self.__data_dir_process(folder_correct_path)
            if year_data_point:
                year_data_points.append(year_data_point)
            else:
                print(f"Processamento falhou na pasta {folder}")

        self._delete_download_files_dir()
        return year_data_points

    def __get_file_links(self) -> list[str]:
        """Extrai links de download direto do HTML da página via requests.
        Filtra apenas anos >= MIN_YEAR (formato moderno com CURSOS CSV)."""
        regex_pattern = r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_(\d{4})\.zip'
        response = requests.get(self.URL, headers=self.HEADERS)
        html = response.text
        matches = re.findall(regex_pattern, html)
        # Filtrar por ano >= MIN_YEAR e reconstruir URLs
        filtered_links = [
            f"https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{year}.zip"
            for year in sorted(set(matches))
            if int(year) >= self.MIN_YEAR
        ]
        return filtered_links

    def __download_and_extract_zipfiles(self, urls: list[str]) -> None:
        """Baixa os ZIPs via requests (não Selenium) e extrai no diretório de dados."""
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

                # Verificar content-type
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    print(f"  ✗ Server returned HTML instead of ZIP — skipping")
                    continue

                # Download com streaming (arquivos grandes)
                total_size = 0
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        total_size += len(chunk)

                print(f"  ✓ Downloaded {filename} ({total_size / 1024 / 1024:.1f} MB)")

                # Verificar magic bytes do ZIP (PK\x03\x04)
                with open(zip_path, "rb") as f:
                    magic = f.read(4)
                if not magic.startswith(b'PK'):
                    print(f"  ✗ File is not a valid ZIP — removing")
                    os.remove(zip_path)
                    continue

                # Extrair o ZIP
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

    def __data_dir_process(self, folder_path: str) -> YearDataPoint:
        dados_folder = self.__find_dados_folder(folder_path)

        if not dados_folder:
            print(f"A pasta 'dados' não foi encontrada em {folder_path}")
            return None

        data_files_list = os.listdir(dados_folder)

        is_courses_file = lambda x: "CURSOS" in x.upper()
        filtered_list = list(filter(is_courses_file, data_files_list))

        if len(filtered_list) != 1:
            print(f"Número inesperado de arquivos 'CURSOS' em {dados_folder}: {filtered_list}")
            return None

        csv_file = filtered_list[0]
        full_csv_file_path = os.path.join(dados_folder, csv_file)
        print(f"Processing {csv_file}...")
        df = self.__process_df(full_csv_file_path)

        if df is not None:
            year = self.__extract_year_from_path(folder_path)
            if year:
                return YearDataPoint(df=df, data_year=year)
            else:
                print(f"Não foi possível extrair o ano do caminho: {folder_path}")
        else:
            print(f"DataFrame é None após processamento do arquivo: {full_csv_file_path}")

        return None

    def __find_dados_folder(self, base_path):
        for root, dirs, files in os.walk(base_path):
            for dir_name in dirs:
                if dir_name.lower() == "dados":
                    return os.path.join(root, dir_name)
        return None

    def __process_df(self, csv_file_path: str) -> pd.DataFrame:
        RELEVANT_COLS = ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA",
                         "TP_CATEGORIA_ADMINISTRATIVA", "QT_VG_TOTAL", "CO_MUNICIPIO", "TP_MODALIDADE_ENSINO"]

        try:
            df = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)
            filtered_df = self.__filter_df(df)
            return filtered_df
        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

    def __filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        city_codes_before_filter = set(df["CO_MUNICIPIO"])

        cols_and_filter_vals = {
            "TP_GRAU_ACADEMICO": [1, 2, 3, 4],
            "TP_NIVEL_ACADEMICO": [1, 2],
            "TP_ORGANIZACAO_ACADEMICA": [1, 2, 3, 4, 5],
            "TP_CATEGORIA_ADMINISTRATIVA": [1, 2, 3, 4, 5, 7],
            "TP_MODALIDADE_ENSINO": [1]
        }

        df = df.dropna(axis="index", subset=["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO",
                                               "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA"])

        for col in ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA"]:
            df[col] = df[col].astype("int")

        for key, val in cols_and_filter_vals.items():
            filtered_series = df[key].apply(lambda x: x in val)
            df = df[filtered_series]

        df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype("int")
        city_codes_after_filter = set(df["CO_MUNICIPIO"])

        # Municípios removidos pela filtragem recebem valor 0 (dados coletados, mas sem cursos que entrem na especificação)
        removed_city_codes: set[int] = city_codes_before_filter.difference(city_codes_after_filter)

        removed_city_lines = [[code, None, None, None, None, None, 0]
                              for code in removed_city_codes]
        removed_values_df = pd.DataFrame(removed_city_lines, columns=df.columns)

        df = pd.concat([df, removed_values_df])
        df = df[df["CO_MUNICIPIO"] != 0]  # código de município 0 deve ser filtrado

        return df

    def __extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', os.path.basename(path))
        if not ano_match:
            ano_match = re.search(r'\d{4}', path)
        return int(ano_match.group(0)) if ano_match else None
