import os
import re
import zipfile
import requests
import pandas as pd

from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper

# O servidor do INEP (download.inep.gov.br) costuma apresentar um certificado
# inválido/expirado, causando SSLCertVerificationError. Para não bloquear o
# pipeline, desabilitamos a verificação SSL para esse host (padrão do projeto).
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass


class CfedScrapper(AbstractScrapper):
    """
    Scrapper para o indicador CFED (Campus Federais) usando os microdados do Censo
    da Educação Superior do INEP.

    Baixa o ZIP de cada ano e lê por streaming apenas o arquivo CURSOS, pegando só
    as colunas necessárias para contar IES federais distintas por município (CO_IES,
    CO_MUNICIPIO, TP_CATEGORIA_ADMINISTRATIVA, TP_MODALIDADE_ENSINO).

    Observações:
      - TP_CATEGORIA_ADMINISTRATIVA=1 → Pública Federal
      - CO_MUNICIPIO é o município do LOCAL DE OFERTA do curso, não a sede da IES
      - MIN_YEAR=2009 (antes disso o layout dos microdados é instável)
      - Os ZIPs são grandes (~40-300 MB). Para economizar disco, cada ZIP é deletado
        após o processamento.
    """

    URL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"
    ZIP_URL_TMPL = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{year}.zip"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/zip, application/octet-stream, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior",
    }

    MIN_YEAR = 2009
    CURSOS_COLS = [
        "NU_ANO_CENSO",
        "CO_IES",
        "TP_CATEGORIA_ADMINISTRATIVA",
        "TP_ORGANIZACAO_ACADEMICA",
        "CO_MUNICIPIO",
        "TP_MODALIDADE_ENSINO",
    ]

    def __init__(self, min_year: int | None = None, max_year: int | None = None) -> None:
        self._create_downloaded_files_dir()
        if min_year is not None:
            self.MIN_YEAR = int(min_year)
        self._max_year = int(max_year) if max_year is not None else None

    def _create_downloaded_files_dir(self) -> None:
        if not os.path.isdir(self.DOWNLOADED_FILES_PATH):
            os.makedirs(self.DOWNLOADED_FILES_PATH)

    def extract_database(self) -> list[YearDataPoint]:
        years = self._list_available_years()
        print(f"Anos disponíveis ({len(years)}): {years[0]}..{years[-1]}")

        out: list[YearDataPoint] = []
        for year in years:
            try:
                df = self._download_and_process_year(year)
                if df is not None and len(df) > 0:
                    out.append(YearDataPoint(df=df, data_year=year))
            except Exception as e:
                print(f"[CFED] {year}: falhou ({type(e).__name__}: {e})")
        return out

    def _list_available_years(self) -> list[int]:
        r = requests.get(self.URL, headers=self.HEADERS, timeout=60, verify=False)
        r.raise_for_status()
        matches = re.findall(
            r"https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_(\d{4})\.zip",
            r.text,
        )
        years = sorted({int(y) for y in matches if int(y) >= self.MIN_YEAR})
        if self._max_year is not None:
            years = [y for y in years if y <= self._max_year]
        return years

    def _download_and_process_year(self, year: int) -> pd.DataFrame | None:
        url = self.ZIP_URL_TMPL.format(year=year)
        zip_path = os.path.join(self.DOWNLOADED_FILES_PATH, f"ces_{year}.zip")

        print(f"[CFED] {year}: baixando {url}")
        try:
            with requests.get(url, headers=self.HEADERS, stream=True, timeout=900, verify=False) as r:
                if r.status_code != 200:
                    print(f"[CFED] {year}: HTTP {r.status_code} — pulando")
                    return None
                total = 0
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
                print(f"[CFED] {year}: download ok ({total/1024/1024:.1f} MB)")
        except requests.RequestException as e:
            print(f"[CFED] {year}: download falhou: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return None

        # valida ZIP
        try:
            with open(zip_path, "rb") as f:
                if not f.read(4).startswith(b"PK"):
                    print(f"[CFED] {year}: arquivo não é um ZIP válido")
                    os.remove(zip_path)
                    return None
        except Exception:
            os.remove(zip_path)
            return None

        try:
            df = self._read_cursos_from_zip(zip_path)
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

        return df

    def _read_cursos_from_zip(self, zip_path: str) -> pd.DataFrame | None:
        with zipfile.ZipFile(zip_path) as z:
            # procura arquivo CURSOS dentro do ZIP (nome varia por ano)
            cursos_names = [
                n for n in z.namelist()
                if n.upper().endswith(".CSV") and "CURSO" in n.upper()
            ]
            if not cursos_names:
                print(f"[CFED] nenhum CSV CURSOS encontrado em {zip_path}")
                return None
            cursos_name = cursos_names[0]

            with z.open(cursos_name) as f:
                # tenta ler só as colunas que precisamos; cai pra leitura completa se der erro
                try:
                    df = pd.read_csv(
                        f,
                        sep=";",
                        encoding="latin-1",
                        usecols=lambda c: c in self.CURSOS_COLS,
                        low_memory=False,
                    )
                except ValueError:
                    # fallback: layout mais antigo pode não ter alguma coluna
                    f.seek(0)
                    df = pd.read_csv(
                        f,
                        sep=";",
                        encoding="latin-1",
                        low_memory=False,
                    )
                    df = df[[c for c in self.CURSOS_COLS if c in df.columns]].copy()

        return df
