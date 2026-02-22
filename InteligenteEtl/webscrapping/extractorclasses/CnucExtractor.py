import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from datastructures import ProcessedDataCollection, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor

from webscrapping.scrapperclasses.CnucScrapper import CnucScrapper, CnucDataInfo

import re
import unicodedata

def _norm_col(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\ufeff", "")      # BOM
    s = s.replace("\u00ad", "")           # soft hyphen invisível
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().casefold()
    return s

def _find_col(df: pd.DataFrame, wanted: str) -> str:
    w = _norm_col(wanted)
    cols = list(df.columns)

    # match exato normalizado
    for c in cols:
        if _norm_col(c) == w:
            return c

    # match por substring (pra pegar variações)
    for c in cols:
        if w in _norm_col(c):
            return c

    raise RuntimeError(f"Não achei coluna parecida com '{wanted}'. Colunas: {cols}")


BIOME_COLS_CANON = [
    "Amazônia",
    "Caatinga",
    "Cerrado",
    "Mata Atlântica",
    "Pampa",
    "Pantanal",
    "Área Marinha",
]

BIOME_CODE: Dict[str, int] = {
    "Desconhecido": 0,
    "Amazônia": 1,
    "Caatinga": 2,
    "Cerrado": 3,
    "Mata Atlântica": 4,
    "Pampa": 5,
    "Pantanal": 6,
    "Área Marinha": 7,
}


def _to_float(x) -> float:
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if not s:
        return 0.0
    # comum no Brasil: 1.234,56
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


_MUN_RE = re.compile(r"^(.*?)\s*\(\s*([A-Za-z]{2})\s*\)\s*$")


def _parse_municipios_abrangidos(val: str) -> List[Tuple[str, str]]:
    """
    Ex: "Abaiara (CE), Araripe (CE), ..."
    Retorna lista [(city_name, uf), ...]
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []

    s = str(val)
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out = []
    seen = set()

    for p in parts:
        m = _MUN_RE.match(p)
        if not m:
            continue
        city = m.group(1).strip()
        uf = m.group(2).strip().upper()
        key = (city.casefold(), uf)
        if city and uf and key not in seen:
            seen.add(key)
            out.append((city, uf))

    return out


class CnucExtractor(AbstractDataExtractor):
    """
    Gera 2 indicadores:
      - Número de UCs por município (INT)
      - Bioma do município (INT code + debug com nome)
    """

    # colunas do CSV bruto
    RAW_UC_CODE_COL = "Código UC"
    RAW_MUN_COL = "Municípios Abrangidos"

    def __init__(
        self,
        save_csv: bool = True,
        output_dir: str = "data/cnuc_processed",
        output_sep: str = ";",
        output_encoding: str = "utf-8",
        raw_sep: str = ";",
        raw_encoding: str = "latin-1",
    ):
        self.save_csv = save_csv
        self.output_dir = output_dir
        self.output_sep = output_sep
        self.output_encoding = output_encoding
        self.raw_sep = raw_sep
        self.raw_encoding = raw_encoding

    def extract_processed_collection(self) -> list[ProcessedDataCollection]:
        # se você quiser: retornar ambos sempre, mesmo que os anos sejam diferentes
        return [self._get_data_point(dp) for dp in CnucDataInfo]

    def _save_processed_csv(self, df: pd.DataFrame, name: str) -> str:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / name
        df.to_csv(out_path, index=False, sep=self.output_sep, encoding=self.output_encoding)
        return str(out_path)

    def _load_raw_df(self, csv_path: str) -> pd.DataFrame:
    # 1) tenta UTF-8 com BOM (é o caso do CNUC)
        try:
            return pd.read_csv(
                csv_path,
                sep=self.raw_sep,
                encoding="utf-8-sig",
                on_bad_lines="skip",
                low_memory=False,
            )
        except UnicodeDecodeError:
            # 2) fallback caso algum arquivo antigo venha latin-1
            return pd.read_csv(
                csv_path,
                sep=self.raw_sep,
                encoding="latin-1",
                on_bad_lines="skip",
                low_memory=False,
            )

    def _explode_uc_municipalities(self, raw: pd.DataFrame) -> pd.DataFrame:
        if self.RAW_UC_CODE_COL not in raw.columns or self.RAW_MUN_COL not in raw.columns:
            raise RuntimeError(
                f"CSV bruto não tem colunas esperadas: '{self.RAW_UC_CODE_COL}' e '{self.RAW_MUN_COL}'. "
                f"Cols disponíveis: {list(raw.columns)}"
            )

        # detecta quais colunas de bioma existem no arquivo
        biome_cols = [c for c in BIOME_COLS_CANON if c in raw.columns]

        tmp = raw[[self.RAW_UC_CODE_COL, self.RAW_MUN_COL] + biome_cols].copy()
        tmp[self.RAW_UC_CODE_COL] = tmp[self.RAW_UC_CODE_COL].astype(str).str.strip()

        # calcula bioma dominante por UC (maior área)
        if biome_cols:
            for c in biome_cols:
                tmp[c] = tmp[c].map(_to_float)

            def pick_biome(row):
                best_name = "Desconhecido"
                best_val = 0.0
                for c in biome_cols:
                    v = float(row.get(c, 0.0) or 0.0)
                    if v > best_val:
                        best_val = v
                        best_name = c
                return best_name, best_val

            picked = tmp.apply(pick_biome, axis=1, result_type="expand")
            tmp["_dominant_biome"] = picked[0]
            tmp["_dominant_biome_area"] = picked[1]
        else:
            tmp["_dominant_biome"] = "Desconhecido"
            tmp["_dominant_biome_area"] = 0.0

        # explode municípios
        records = []
        for _, row in tmp.iterrows():
            uc = row[self.RAW_UC_CODE_COL]
            muni_list = _parse_municipios_abrangidos(row[self.RAW_MUN_COL])
            if not muni_list:
                continue

            for city_name, uf in muni_list:
                records.append(
                    {
                        "_uc_code": uc,
                        "_city_name": city_name,
                        "_uf": uf,
                        "_dominant_biome": row["_dominant_biome"],
                        "_dominant_biome_area": float(row["_dominant_biome_area"] or 0.0),
                    }
                )

        exploded = pd.DataFrame.from_records(records)
        if exploded.empty:
            raise RuntimeError("Explode de 'Municípios Abrangidos' resultou em DF vazio.")
        return exploded

    def _match_city_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        from citiesinfo import match_city_names_with_codes

        df = df.copy()
        df["_uf"] = df["_uf"].astype(str).str.strip().str.upper()
        df["_city_name"] = df["_city_name"].astype(str).str.strip()

        # seu helper já lida com acentos etc.
        df = match_city_names_with_codes(df, "_city_name", "_uf")
        # espera-se que ele crie a coluna self.CITY_CODE_COL
        if self.CITY_CODE_COL not in df.columns:
            raise RuntimeError(
                f"match_city_names_with_codes não gerou '{self.CITY_CODE_COL}'. Colunas: {list(df.columns)}"
            )
        return df

    def _compute_uc_count(self, df: pd.DataFrame) -> pd.DataFrame:
        # conta UCs distintas por município
        out = (
            df.groupby(self.CITY_CODE_COL)["_uc_code"]
            .nunique()
            .reset_index(name=self.DATA_VALUE_COLUMN)
        )
        return out

    def _compute_city_biome(self, df: pd.DataFrame) -> pd.DataFrame:
        # conta frequência de bioma dominante por município
        g = (
            df.groupby([self.CITY_CODE_COL, "_dominant_biome"])
            .agg(
                n=("_uc_code", "nunique"),
                area=("_dominant_biome_area", "sum"),
            )
            .reset_index()
        )

        # escolhe bioma por: maior n, depois maior area, depois nome (determinístico)
        g = g.sort_values(
            by=[self.CITY_CODE_COL, "n", "area", "_dominant_biome"],
            ascending=[True, False, False, True],
        )

        best = g.drop_duplicates(subset=[self.CITY_CODE_COL], keep="first").copy()
        best["biome_name"] = best["_dominant_biome"].astype(str)
        best["biome_code"] = best["biome_name"].map(lambda x: BIOME_CODE.get(x, 0)).astype(int)

        out = best[[self.CITY_CODE_COL, "biome_code", "biome_name"]].copy()
        return out

    def _get_data_point(self, data_point: CnucDataInfo) -> ProcessedDataCollection:
        spec = data_point.value["spec"]
        year = int(spec.year)

        scr = CnucScrapper(data_point)
        raw_csv_path = scr.scrape_csv()
        raw_df = self._load_raw_df(raw_csv_path)

        exploded = self._explode_uc_municipalities(raw_df)
        exploded = self._match_city_codes(exploded)

        # cria dataframe final conforme indicador
        if data_point.name == "CONSERVATION_UNITS_COUNT":
            metric = self._compute_uc_count(exploded)
            metric[self.YEAR_COLUMN] = year
            metric[self.DTYPE_COLUMN] = data_point.value["dtype"].value
            metric[self.DATA_IDENTIFIER_COLUMN] = data_point.value["data_identifier"]

            final = metric[
                [self.YEAR_COLUMN, self.CITY_CODE_COL, self.DATA_IDENTIFIER_COLUMN, self.DTYPE_COLUMN, self.DATA_VALUE_COLUMN]
            ].copy()

            if self.save_csv:
                self._save_processed_csv(final, f"cnuc_uc_count_{year}.csv")

            return ProcessedDataCollection(
                category=data_point.value["topic"],
                dtype=data_point.value["dtype"],
                data_name=data_point.value["data_identifier"],
                time_series_years=[year],
                df=final,
            )

        elif data_point.name == "MUNICIPALITY_BIOME":
            metric = self._compute_city_biome(exploded)

            # DW: usa biome_code como valor (INT)
            final = pd.DataFrame({
                self.YEAR_COLUMN: year,
                self.CITY_CODE_COL: metric[self.CITY_CODE_COL],
                self.DATA_IDENTIFIER_COLUMN: data_point.value["data_identifier"],
                self.DTYPE_COLUMN: "int",  # garante DW numérico
                self.DATA_VALUE_COLUMN: metric["biome_code"].astype(int),
            })

            if self.save_csv:
                self._save_processed_csv(final, f"cnuc_biome_code_{year}.csv")
                # debug com o nome do bioma escolhido
                dbg = metric.copy()
                dbg[self.YEAR_COLUMN] = year
                self._save_processed_csv(dbg, f"cnuc_biome_debug_{year}.csv")

            return ProcessedDataCollection(
                category=data_point.value["topic"],
                dtype=data_point.value["dtype"], 
                data_name=data_point.value["data_identifier"],
                time_series_years=[year],
                df=final,
            )

        else:
            raise ValueError(f"Data point não tratado: {data_point.name}")


if __name__ == "__main__":
    ext = CnucIndicatorsExtractor(save_csv=True)
    cols = ext.extract_processed_collection()
    for c in cols:
        print("\n===", c.data_name, "===")
        print(c.df.head())