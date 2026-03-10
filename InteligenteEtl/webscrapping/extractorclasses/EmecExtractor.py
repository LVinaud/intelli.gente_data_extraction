from pathlib import Path
import re
import unicodedata
import pandas as pd

from datastructures import ProcessedDataCollection
from .AbstractDataExtractor import AbstractDataExtractor


def _norm_text(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).replace("\ufeff", "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().casefold()
    return s


def _read_emec_csv_local(path: str) -> pd.DataFrame:
    # detecta separador olhando a 1a linha
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        header = f.readline()

    sep = "," if header.count(",") > header.count(";") else ";"

    return pd.read_csv(
        path,
        sep=sep,
        encoding="utf-8-sig",
        dtype=str,
        engine="python",
        keep_default_na=False,  # evita NaN automático em strings
    )


def _extract_ibge7_from_padded(val: str) -> str:
    """
    Seu CSV traz CODIGO_MUNICIPIO_IBGE como algo tipo:
      000000004106902  (15 dígitos)
    O código IBGE do município é sempre os ÚLTIMOS 7 dígitos: 4106902.

    Também funciona para:
      1600303
      1600303.0
      '000000001600303'
      etc.
    """
    s = "" if val is None else str(val)
    digits = re.sub(r"\D", "", s)  # fica só dígitos
    if len(digits) < 7:
        return ""
    last7 = digits[-7:]
    if last7 == "0000000":
        return ""
    return last7


def _extract_digits(val: str) -> str:
    s = "" if val is None else str(val)
    digits = re.sub(r"\D", "", s)
    return digits if digits else ""


class EmecExtractor(AbstractDataExtractor):
    # nomes EXATOS do seu CSV
    IES_CODE_COL = "CODIGO_DA_IES"
    CITY_CODE_RAW_COL = "CODIGO_MUNICIPIO_IBGE"
    CATEGORY_COL = "CATEGORIA_DA_IES"
    SITUATION_COL = "SITUACAO_IES"

    def __init__(
        self,
        save_csv: bool = True,
        output_dir: str = "data/emec_out",
        raw_dir: str = "data/emec_raw",
        raw_filename: str = "emec_raw.csv",
        year: int = 2022,
        data_identifier: str = "emec_ies_publicas_por_municipio",
        dtype_value: str = "int",
        topic: str = "educacao",
        output_sep: str = ";",
        output_encoding: str = "utf-8",
    ):
        self.save_csv = save_csv
        self.output_dir = output_dir
        self.raw_dir = raw_dir
        self.raw_filename = raw_filename

        self.year = int(year)
        self.data_identifier = data_identifier
        self.dtype_value = dtype_value
        self.topic = topic

        self.output_sep = output_sep
        self.output_encoding = output_encoding

    def _raw_path(self) -> str:
        p = Path(self.raw_dir) / self.raw_filename
        if not p.exists():
            raise RuntimeError(f"CSV bruto não existe: {p.resolve()}")
        return str(p)

    def _save_processed_csv(self, df: pd.DataFrame) -> str:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"emec_{self.data_identifier}_{self.year}.csv"
        df.to_csv(out_path, index=False, sep=self.output_sep, encoding=self.output_encoding)
        return str(out_path)

    def extract_processed_collection(self) -> list[ProcessedDataCollection]:
        return [self._extract_one()]

    def _extract_one(self) -> ProcessedDataCollection:
        raw_df = _read_emec_csv_local(self._raw_path())

        required = [self.IES_CODE_COL, self.CITY_CODE_RAW_COL, self.CATEGORY_COL]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            raise RuntimeError(f"CSV EMEC sem colunas esperadas {missing}. Colunas: {list(raw_df.columns)}")

        # pega só o que precisa; SITUACAO_IES é opcional (mas existe no seu exemplo)
        cols = [self.IES_CODE_COL, self.CITY_CODE_RAW_COL, self.CATEGORY_COL]
        if self.SITUATION_COL in raw_df.columns:
            cols.append(self.SITUATION_COL)

        df = raw_df[cols].copy()

        # filtra apenas públicas
        df["_cat_norm"] = df[self.CATEGORY_COL].map(_norm_text)
        df = df[df["_cat_norm"].eq(_norm_text("Pública"))].copy()

        # se quiser contar só IES ativas (recomendado), descomente:
        if self.SITUATION_COL in df.columns:
            df["_sit_norm"] = df[self.SITUATION_COL].map(_norm_text)
            df = df[df["_sit_norm"].eq(_norm_text("Ativa"))].copy()

        # limpa códigos
        df["_ibge7"] = df[self.CITY_CODE_RAW_COL].map(_extract_ibge7_from_padded)
        df["_ies"] = df[self.IES_CODE_COL].map(_extract_digits)

        df = df[df["_ibge7"].str.match(r"^\d{7}$", na=False)].copy()
        df = df[df["_ies"].ne("")].copy()

        # conta IES ÚNICAS por município
        counts = (
            df.groupby("_ibge7")["_ies"]
            .nunique()
            .reset_index(name=self.DATA_VALUE_COLUMN)
        )

        final = pd.DataFrame({
            self.CITY_CODE_COL: counts["_ibge7"].astype(int),
            self.DATA_IDENTIFIER_COLUMN: self.data_identifier,
            self.YEAR_COLUMN: self.year,
            self.DATA_VALUE_COLUMN: counts[self.DATA_VALUE_COLUMN].astype(int),
        })[
            [self.CITY_CODE_COL, self.DATA_IDENTIFIER_COLUMN, self.YEAR_COLUMN, self.DATA_VALUE_COLUMN]
        ].copy()

        if self.save_csv:
            self._save_processed_csv(final)

        return ProcessedDataCollection(
            category=self.topic,
            dtype=self.dtype_value,
            data_name=self.data_identifier,
            time_series_years=[self.year],
            df=final,
        )