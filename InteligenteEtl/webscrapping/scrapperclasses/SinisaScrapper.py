from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import unicodedata
from urllib.parse import urljoin, urlparse
import zipfile

import pandas as pd
import requests

from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper


@dataclass(frozen=True)
class SinisaDocumentLink:
   url: str
   text: str
   kind: str
   module: str | None


class _AnchorParser(HTMLParser):
   def __init__(self) -> None:
      super().__init__()
      self._href: str | None = None
      self._text_parts: list[str] = []
      self.links: list[tuple[str, str]] = []

   def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
      if tag.lower() != "a":
         return
      href = None
      for key, value in attrs:
         if key.lower() == "href":
            href = value
            break
      if href:
         self._href = href
         self._text_parts = []

   def handle_data(self, data: str) -> None:
      if self._href is not None:
         self._text_parts.append(data)

   def handle_endtag(self, tag: str) -> None:
      if tag.lower() != "a" or self._href is None:
         return
      text = " ".join(part.strip() for part in self._text_parts if part.strip()).strip()
      self.links.append((self._href, text))
      self._href = None
      self._text_parts = []


class SinisaScrapper(AbstractScrapper):
   """
   Scrapper do SINISA para baixar e parsear planilhas publicadas no gov.br.
   Retorna lista de YearDataPoint no formato longo:
      [codigo_municipio, ano, indicador, valor]
   """

   SINISA_HOME_URL = (
      "https://www.gov.br/cidades/pt-br/acesso-a-informacao/"
      "acoes-e-programas/saneamento/sinisa"
   )
   DEFAULT_RESULTS_URL = (
      "https://www.gov.br/cidades/pt-br/acesso-a-informacao/"
      "acoes-e-programas/saneamento/sinisa/resultados-sinisa"
   )

   DOWNLOADABLE_EXTENSIONS = (".zip", ".xlsx", ".xls", ".csv", ".ods", ".pdf")
   PLANILHA_SOURCE_EXTENSIONS = (".zip", ".csv", ".xlsx", ".xls", ".ods")
   PLANILHA_FINAL_EXTENSIONS = (".csv", ".xlsx", ".xls", ".ods")
   USER_AGENT = "inteligente-sinisa-scrapper/1.0"

   VALID_FILE_KINDS = ("planilhas", "relatorios", "glossarios", "atestados", "all")
   VALID_MODULES = ("gestao_municipal", "agua", "esgoto", "residuos", "aguas_pluviais")

   KIND_PATTERNS: dict[str, tuple[str, ...]] = {
      "planilhas": ("planilha", "informacoes e indicadores", "indicadores"),
      "relatorios": ("relatorio",),
      "glossarios": ("glossario",),
      "atestados": ("atestado", "adimplencia", "regularidade"),
   }
   MODULE_PATTERNS: dict[str, tuple[str, ...]] = {
      "gestao_municipal": ("gestao municipal", "gestao_municipal"),
      "agua": ("agua", "abastecimento"),
      "esgoto": ("esgoto", "esgotamento"),
      "residuos": ("residuo", "residuos"),
      "aguas_pluviais": ("pluvial", "aguas pluviais", "aguaspluviais"),
   }

   CITY_CODE_COL = "codigo_municipio"
   YEAR_COL = "ano"
   INDICATOR_COL = "indicador"
   VALUE_COL = "valor"

   _CITY_CODE_CANDIDATES = (
      "codigo_municipio",
      "cod_municipio",
      "municipio_codigo",
      "id_municipio",
      "id_municipio_ibge",
      "cod_ibge",
      "ibge",
   )
   _YEAR_CANDIDATES = (
      "ano",
      "ano_referencia",
      "anoreferencia",
      "ano_base",
      "ano_ref",
      "year",
   )
   _NON_INDICATOR_COLS = {
      "municipio",
      "nome_municipio",
      "uf",
      "estado",
      "sigla_uf",
      "regiao",
      "microrregiao",
      "mesorregiao",
      "prestador",
      "prestador_nome",
      "servico",
      "sistema",
      "localidade",
      "codigo_localidade",
      "descricao",
      "tipo",
      "classe",
   }

   def __init__(
      self,
      results_url: str | None = None,
      file_kinds: list[str] | None = None,
      modules: list[str] | None = None,
      extract_archives: bool = True,
      overwrite: bool = False,
      timeout: int = 120,
   ) -> None:
      super().__init__()
      self._results_url = results_url
      self._file_kinds = self._normalize_file_kinds(file_kinds or ["planilhas"])
      self._modules = self._normalize_modules(modules)
      self._extract_archives = extract_archives
      self._overwrite = overwrite
      self._timeout = timeout

   def extract_database(self) -> list[YearDataPoint]:
      base_dir, raw_dir, extracted_dir = self._prepare_dirs()
      try:
         docs = self._list_documents(self._results_url, self._file_kinds, self._modules)
         if not docs:
            return []

         tabular_files: list[tuple[Path, str | None]] = []
         for doc in docs:
            if doc.kind != "planilhas":
               continue
            if not self._is_planilha_source(doc.url):
               continue

            downloaded_path = self._download_document(doc, raw_dir)
            if downloaded_path is None:
               continue

            if downloaded_path.suffix.lower() == ".zip":
               if self._extract_archives and extracted_dir is not None:
                  extracted = self._extract_zip(
                     downloaded_path,
                     extracted_dir,
                     set(self.PLANILHA_FINAL_EXTENSIONS),
                  )
                  for extracted_file in extracted:
                     tabular_files.append((extracted_file, doc.module))
            elif downloaded_path.suffix.lower() in self.PLANILHA_FINAL_EXTENSIONS:
               tabular_files.append((downloaded_path, doc.module))

         if not tabular_files:
            return []

         long_frames: list[pd.DataFrame] = []
         for file_path, module in tabular_files:
            df = self._read_tabular_file(file_path)
            if df is None or df.empty:
               continue

            long_df = self._dataframe_to_long(df, file_path, module)
            if not long_df.empty:
               long_frames.append(long_df)

         if not long_frames:
            return []

         merged_df = pd.concat(long_frames, axis="index", ignore_index=True)
         merged_df[self.YEAR_COL] = pd.to_numeric(merged_df[self.YEAR_COL], errors="coerce")
         merged_df = merged_df.dropna(subset=[self.YEAR_COL, self.CITY_CODE_COL, self.VALUE_COL])
         merged_df[self.YEAR_COL] = merged_df[self.YEAR_COL].astype("int")
         merged_df[self.CITY_CODE_COL] = merged_df[self.CITY_CODE_COL].astype("int")

         return self._create_datapoints_per_year(merged_df)
      finally:
         self._cleanup_dir(base_dir)

   def _prepare_dirs(self) -> tuple[Path, Path, Path | None]:
      base_dir = Path(self.DOWNLOADED_FILES_PATH) / "sinisa"
      raw_dir = base_dir / "raw"
      raw_dir.mkdir(parents=True, exist_ok=True)
      extracted_dir = None
      if self._extract_archives:
         extracted_dir = base_dir / "extracted"
         extracted_dir.mkdir(parents=True, exist_ok=True)
      return base_dir, raw_dir, extracted_dir

   def _cleanup_dir(self, path: Path) -> None:
      if path.exists():
         shutil.rmtree(path, ignore_errors=True)

   def _normalize_file_kinds(self, file_kinds: list[str]) -> list[str]:
      parsed = [kind.strip().lower() for kind in file_kinds]
      invalid = [kind for kind in parsed if kind not in self.VALID_FILE_KINDS]
      if invalid:
         raise ValueError(f"Tipos inválidos para file_kinds: {', '.join(invalid)}")
      return list(dict.fromkeys(parsed))

   def _normalize_modules(self, modules: list[str] | None) -> list[str] | None:
      if not modules:
         return None
      parsed = [module.strip().lower() for module in modules]
      invalid = [module for module in parsed if module not in self.VALID_MODULES]
      if invalid:
         raise ValueError(f"Módulos inválidos: {', '.join(invalid)}")
      return list(dict.fromkeys(parsed))

   def _list_documents(
      self,
      results_url: str | None,
      file_kinds: list[str],
      modules: list[str] | None,
   ) -> list[SinisaDocumentLink]:
      page_urls = self._resolve_results_urls(results_url)
      links: list[SinisaDocumentLink] = []
      seen: set[str] = set()

      for page_url in page_urls:
         try:
            html = self._fetch_text(page_url)
         except Exception:
            continue
         for doc in self._extract_links(html, page_url):
            if doc.url in seen:
               continue
            seen.add(doc.url)
            links.append(doc)

      filtered: list[SinisaDocumentLink] = []
      for doc in links:
         if "all" not in file_kinds and doc.kind not in file_kinds:
            continue
         if modules and doc.module not in modules:
            continue
         filtered.append(doc)
      return filtered

   def _resolve_results_urls(self, results_url: str | None) -> list[str]:
      if results_url:
         return [results_url]

      candidates = {self.DEFAULT_RESULTS_URL}
      seeds = [self.SINISA_HOME_URL, self.DEFAULT_RESULTS_URL]
      for seed in seeds:
         try:
            html = self._fetch_text(seed)
         except Exception:
            continue
         for href, _ in self._extract_anchors(html, seed):
            if self._is_downloadable(href):
               continue
            if "/resultados-sinisa/" not in href:
               continue
            if "/arquivos/" in href:
               continue
            candidates.add(href.rstrip("/"))
      return sorted(candidates, reverse=True)

   def _extract_links(self, html: str, base_url: str) -> list[SinisaDocumentLink]:
      anchors = self._extract_anchors(html, base_url)
      docs: list[SinisaDocumentLink] = []
      seen_urls: set[str] = set()

      for url, text in anchors:
         if url in seen_urls:
            continue
         if not self._is_downloadable(url):
            continue
         seen_urls.add(url)
         docs.append(
            SinisaDocumentLink(
               url=url,
               text=text,
               kind=self._infer_kind(text=text, url=url),
               module=self._infer_module(text=text, url=url),
            )
         )
      return docs

   def _extract_anchors(self, html: str, base_url: str) -> list[tuple[str, str]]:
      parser = _AnchorParser()
      parser.feed(html)
      anchors: list[tuple[str, str]] = []
      for href, text in parser.links:
         normalized = self._normalize_url(href, base_url)
         if normalized:
            anchors.append((normalized, text))
      return anchors

   def _normalize_url(self, href: str, base_url: str) -> str | None:
      href = href.strip()
      if not href or href.startswith("#"):
         return None
      if href.startswith(("mailto:", "javascript:")):
         return None

      absolute = urljoin(base_url, href)
      if absolute.endswith("/view"):
         absolute = absolute[:-5]
      return absolute

   def _is_downloadable(self, url: str) -> bool:
      return urlparse(url).path.lower().endswith(self.DOWNLOADABLE_EXTENSIONS)

   def _is_planilha_source(self, url: str) -> bool:
      return urlparse(url).path.lower().endswith(self.PLANILHA_SOURCE_EXTENSIONS)

   def _normalize_text(self, text: str) -> str:
      lowered = text.lower().strip()
      normalized = unicodedata.normalize("NFD", lowered)
      without_accents = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
      return re.sub(r"[^a-z0-9]+", " ", without_accents).strip()

   def _infer_kind(self, text: str, url: str) -> str:
      haystack = self._normalize_text(f"{text} {Path(urlparse(url).path).name}")
      for kind, patterns in self.KIND_PATTERNS.items():
         if any(pattern in haystack for pattern in patterns):
            return kind
      return "other"

   def _infer_module(self, text: str, url: str) -> str | None:
      haystack = self._normalize_text(f"{text} {Path(urlparse(url).path).name}")
      for module, patterns in self.MODULE_PATTERNS.items():
         if any(pattern in haystack for pattern in patterns):
            return module
      return None

   def _fetch_text(self, url: str) -> str:
      response = requests.get(
         url,
         headers={
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
         },
         timeout=self._timeout,
      )
      response.raise_for_status()

      content_type = response.headers.get("Content-Type", "").lower()
      response.encoding = response.apparent_encoding or response.encoding or "utf-8"
      text = response.text

      # Alguns endpoints redirecionam para binário; nesse caso não tentamos parsear como HTML.
      if "html" not in content_type and "xml" not in content_type:
         lowered = text.lower()
         if "<html" not in lowered and "<!doctype html" not in lowered:
            raise RuntimeError(f"Conteúdo retornado não parece HTML/XML: {content_type}")

      return text

   def _download_document(self, doc: SinisaDocumentLink, raw_dir: Path) -> Path | None:
      file_path = raw_dir / Path(urlparse(doc.url).path).name
      if file_path.exists() and not self._overwrite:
         return file_path

      attempts = 2
      for _ in range(attempts):
         try:
            response = requests.get(
               doc.url,
               headers={
                  "User-Agent": self.USER_AGENT,
                  "Accept": "*/*",
               },
               timeout=self._timeout,
               stream=True,
            )
            response.raise_for_status()

            with file_path.open("wb") as fp:
               for chunk in response.iter_content(chunk_size=8192):
                  if chunk:
                     fp.write(chunk)
            return file_path
         except Exception:
            continue
      return None

   def _extract_zip(self, zip_path: Path, extracted_dir: Path, allowed_exts: set[str]) -> list[Path]:
      destination = extracted_dir / zip_path.stem
      if destination.exists() and self._overwrite:
         shutil.rmtree(destination, ignore_errors=True)
      destination.mkdir(parents=True, exist_ok=True)

      extracted_files: list[Path] = []
      destination_root = destination.resolve()
      with zipfile.ZipFile(zip_path, "r") as zf:
         for member in zf.infolist():
            if member.is_dir():
               continue

            suffix = Path(member.filename).suffix.lower()
            if allowed_exts and suffix not in allowed_exts:
               continue

            target = (destination / member.filename).resolve()
            if not self._is_within_dir(destination_root, target):
               continue

            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member, "r") as src, target.open("wb") as dst:
               shutil.copyfileobj(src, dst)
            extracted_files.append(target)
      return extracted_files

   def _is_within_dir(self, base_dir: Path, target: Path) -> bool:
      try:
         target.relative_to(base_dir)
      except ValueError:
         return False
      return True

   def _read_tabular_file(self, file_path: Path) -> pd.DataFrame | None:
      suffix = file_path.suffix.lower()
      if suffix in (".xlsx", ".xls", ".ods"):
         try:
            return pd.read_excel(file_path, sheet_name=0)
         except Exception:
            return None

      if suffix == ".csv":
         for encoding in ("utf-8-sig", "latin-1", "cp1252"):
            try:
               return pd.read_csv(file_path, encoding=encoding, sep=None, engine="python")
            except Exception:
               continue
      return None

   def _dataframe_to_long(self, df: pd.DataFrame, file_path: Path, module: str | None) -> pd.DataFrame:
      df = df.copy()
      df = self._normalize_columns(df)
      city_col = self._find_city_code_col(df)
      if city_col is None:
         return pd.DataFrame()

      year_col, year_const = self._find_year_info(df, file_path)
      if year_col is None and year_const is None:
         return pd.DataFrame()

      city_series = df[city_col].apply(self._normalize_city_code)
      if year_col is not None:
         year_series = df[year_col].apply(self._normalize_year)
      else:
         year_series = pd.Series([year_const] * len(df), index=df.index, dtype="float64")

      valid_rows = city_series.notna() & year_series.notna()
      if valid_rows.sum() == 0:
         return pd.DataFrame()

      indicator_cols = [
         col for col in df.columns
         if col not in {city_col, year_col} and col.lower() not in self._NON_INDICATOR_COLS
      ]
      if not indicator_cols:
         return pd.DataFrame()

      long_parts: list[pd.DataFrame] = []
      file_prefix = self._normalize_indicator_name(file_path.stem)
      module_prefix = module.upper() if module else "GERAL"

      for indicator_col in indicator_cols:
         parsed_vals = df[indicator_col].apply(self._parse_data_value)
         has_value = parsed_vals.notna() & valid_rows
         if has_value.sum() == 0:
            continue

         non_null_values = parsed_vals[has_value]
         if len(non_null_values) > 0 and all(isinstance(x, str) for x in non_null_values):
            unique_vals = len(set(non_null_values))
            if unique_vals > 20:
               # Muito provavelmente coluna descritiva em vez de indicador.
               continue

         indicator_name = f"SINISA_{module_prefix}_{file_prefix}_{self._normalize_indicator_name(indicator_col)}"
         part = pd.DataFrame({
            self.CITY_CODE_COL: city_series[has_value].astype("int"),
            self.YEAR_COL: year_series[has_value].astype("int"),
            self.INDICATOR_COL: indicator_name,
            self.VALUE_COL: parsed_vals[has_value],
         })
         long_parts.append(part)

      if not long_parts:
         return pd.DataFrame()
      return pd.concat(long_parts, axis="index", ignore_index=True)

   def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
      used: dict[str, int] = {}
      new_cols: list[str] = []

      for col in df.columns:
         base = self._normalize_indicator_name(str(col))
         if not base:
            base = "coluna"
         if base not in used:
            used[base] = 1
            new_cols.append(base)
         else:
            suffix = used[base]
            used[base] += 1
            new_cols.append(f"{base}_{suffix}")

      df.columns = new_cols
      return df

   def _normalize_indicator_name(self, name: str) -> str:
      normalized = unicodedata.normalize("NFD", str(name).strip().lower())
      no_accents = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
      cleaned = re.sub(r"[^a-z0-9]+", "_", no_accents).strip("_")
      return cleaned.upper()

   def _find_city_code_col(self, df: pd.DataFrame) -> str | None:
      cols_map = {col.lower(): col for col in df.columns}
      for candidate in self._CITY_CODE_CANDIDATES:
         if candidate in cols_map:
            return cols_map[candidate]

      best_col = None
      best_hits = 0
      sample_size = min(len(df), 500)
      sample = df.head(sample_size)
      for col in df.columns:
         parsed = sample[col].apply(self._normalize_city_code)
         hits = int(parsed.notna().sum())
         if hits > best_hits:
            best_hits = hits
            best_col = col
      if sample_size > 0 and best_hits >= max(10, int(sample_size * 0.5)):
         return best_col
      return None

   def _find_year_info(self, df: pd.DataFrame, file_path: Path) -> tuple[str | None, int | None]:
      cols_map = {col.lower(): col for col in df.columns}
      for candidate in self._YEAR_CANDIDATES:
         if candidate in cols_map:
            return cols_map[candidate], None
      return None, self._infer_year_from_text(str(file_path))

   def _infer_year_from_text(self, text: str) -> int | None:
      current_year = datetime.now().year
      matches = re.findall(r"(19\d{2}|20\d{2})", text)
      for match in matches:
         year = int(match)
         if 1980 <= year <= (current_year + 1):
            return year
      return None

   def _normalize_city_code(self, value: object) -> int | None:
      if pd.isna(value):
         return None
      digits = re.sub(r"\D", "", str(value))
      if len(digits) == 7:
         return int(digits)
      if len(digits) == 6:
         # atualização para 7 dígitos é feita no extractor com update_city_code
         return int(digits)
      return None

   def _normalize_year(self, value: object) -> int | None:
      if pd.isna(value):
         return None
      match = re.search(r"(19\d{2}|20\d{2})", str(value))
      if not match:
         return None
      year = int(match.group(1))
      if year < 1980 or year > datetime.now().year + 1:
         return None
      return year

   def _parse_data_value(self, value: object) -> object | None:
      if pd.isna(value):
         return None

      if isinstance(value, bool):
         return value
      if isinstance(value, (int, float)):
         return value

      text = str(value).strip()
      if not text or text in {"-", "--", "---", "N/A", "n/a", "NA"}:
         return None

      lowered = text.lower()
      if lowered in {"sim", "s", "yes", "true"}:
         return True
      if lowered in {"nao", "não", "n", "no", "false"}:
         return False

      numeric_text = text.replace("%", "").replace(".", "").replace(",", ".")
      try:
         if "." in numeric_text:
            return float(numeric_text)
         return int(numeric_text)
      except Exception:
         return text

   def _create_datapoints_per_year(self, df: pd.DataFrame) -> list[YearDataPoint]:
      years = sorted(df[self.YEAR_COL].unique().tolist())
      datapoints: list[YearDataPoint] = []
      for year in years:
         year_df = df[df[self.YEAR_COL] == year].reset_index(drop=True)
         datapoints.append(YearDataPoint(df=year_df, data_year=int(year)))
      return datapoints
