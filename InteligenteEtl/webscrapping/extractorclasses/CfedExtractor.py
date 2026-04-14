from pathlib import Path
import pandas as pd

from datastructures import ProcessedDataCollection, YearDataPoint, DataTypes
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses.CfedScrapper import CfedScrapper


class CfedExtractor(AbstractDataExtractor):
    """
    CFED — Número de Campus de Institutos e Universidades Federais (Rede Federal)
    por município, por ano do Censo da Educação Superior (INEP).

    Definição operacional (alinhada com portal.mec.gov.br/rede-federal-inicial):
      CFED(municipio, ano) = # IES distintas (CO_IES) onde
        TP_CATEGORIA_ADMINISTRATIVA == 1 (Pública Federal)
        AND TP_ORGANIZACAO_ACADEMICA ∈ {1=Universidade, 4=Instituto Federal, 5=CEFET}
        AND CO_MUNICIPIO == municipio (local de oferta do curso)

    Limitação conhecida: uma mesma IES pode ter múltiplos campi dentro do mesmo
    município (ex.: IFRJ-Maracanã e IFRJ-Realengo, ambos no Rio). Nesse caso o
    Censo agrega ambos em uma única entrada (CO_IES=IFRJ, CO_MUNICIPIO=Rio),
    logo contam como 1. É uma aproximação boa para "distintas IES da Rede Federal
    atuando no município", mas pode subestimar a contagem real de campi físicos.

    A série histórica cobre 2009 em diante (layout dos microdados mais antigos é
    instável). Cada ano gera um CSV no formato padrão do DW:
        codigo_ibge,sigla,ano,variavel_valor
    """

    SIGLA = "CFED"
    TOPIC = "Educação"
    DTYPE = DataTypes.INT

    # colunas do CURSOS CSV do INEP
    RAW_YEAR_COL = "NU_ANO_CENSO"
    RAW_IES_COL = "CO_IES"
    RAW_CATEGORY_COL = "TP_CATEGORIA_ADMINISTRATIVA"
    RAW_ORGANIZACAO_COL = "TP_ORGANIZACAO_ACADEMICA"
    RAW_MUNICIPIO_COL = "CO_MUNICIPIO"
    CATEGORY_FEDERAL = 1
    # Rede Federal stricto sensu: Universidade (1), Instituto Federal (4), CEFET (5)
    REDE_FEDERAL_ORGS = {1, 4, 5}

    def __init__(
        self,
        save_csv: bool = True,
        output_dir: str = "data/cfed_processed",
        output_sep: str = ",",
        output_encoding: str = "utf-8",
        min_year: int | None = None,
        max_year: int | None = None,
    ) -> None:
        self.save_csv = save_csv
        self.output_dir = output_dir
        self.output_sep = output_sep
        self.output_encoding = output_encoding
        self._min_year = min_year
        self._max_year = max_year

    def _save_processed_csv(self, df: pd.DataFrame, name: str) -> str:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / name
        df.rename(columns={
            self.CITY_CODE_COL: "codigo_ibge",
            self.DATA_IDENTIFIER_COLUMN: "sigla",
            self.YEAR_COLUMN: "ano",
            self.DATA_VALUE_COLUMN: "variavel_valor",
        })[["codigo_ibge", "sigla", "ano", "variavel_valor"]].to_csv(
            out_path, index=False, sep=self.output_sep, encoding=self.output_encoding
        )
        return str(out_path)

    def extract_processed_collection(self) -> list[ProcessedDataCollection]:
        scr = CfedScrapper(min_year=self._min_year, max_year=self._max_year)
        year_points: list[YearDataPoint] = scr.extract_database()
        if not year_points:
            raise RuntimeError("Nenhum dado extraído do Censo da Educação Superior.")

        per_year_dfs: list[pd.DataFrame] = []
        for yp in year_points:
            df = self._process_year_df(yp.df, yp.data_year)
            if df is not None and len(df) > 0:
                per_year_dfs.append(df)
                if self.save_csv:
                    self._save_processed_csv(df, f"{self.SIGLA}_{yp.data_year}.csv")

        if not per_year_dfs:
            raise RuntimeError("Processamento resultou em DF vazio para todos os anos.")

        final = pd.concat(per_year_dfs, ignore_index=True)
        years_in_data = sorted(final[self.YEAR_COLUMN].unique().tolist())

        return [ProcessedDataCollection(
            category=self.TOPIC,
            dtype=self.DTYPE,
            data_name=self.SIGLA,
            time_series_years=years_in_data,
            df=final,
        )]

    def _process_year_df(self, raw_df: pd.DataFrame, year: int) -> pd.DataFrame | None:
        required = [self.RAW_IES_COL, self.RAW_CATEGORY_COL, self.RAW_ORGANIZACAO_COL, self.RAW_MUNICIPIO_COL]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            print(f"[CFED] {year}: colunas ausentes {missing} — pulando")
            return None

        df = raw_df[required].copy()
        df = df.dropna(subset=required)

        # tipos
        df[self.RAW_CATEGORY_COL] = pd.to_numeric(df[self.RAW_CATEGORY_COL], errors="coerce").astype("Int64")
        df[self.RAW_ORGANIZACAO_COL] = pd.to_numeric(df[self.RAW_ORGANIZACAO_COL], errors="coerce").astype("Int64")
        df[self.RAW_IES_COL] = pd.to_numeric(df[self.RAW_IES_COL], errors="coerce").astype("Int64")
        df[self.RAW_MUNICIPIO_COL] = pd.to_numeric(df[self.RAW_MUNICIPIO_COL], errors="coerce").astype("Int64")
        df = df.dropna(subset=required)

        # filtra Rede Federal: Pública Federal + (Universidade | IF | CEFET)
        df = df[
            (df[self.RAW_CATEGORY_COL] == self.CATEGORY_FEDERAL)
            & (df[self.RAW_ORGANIZACAO_COL].isin(self.REDE_FEDERAL_ORGS))
        ]
        if df.empty:
            print(f"[CFED] {year}: sem IES da Rede Federal após filtro")
            return None

        # filtra códigos IBGE válidos (7 dígitos)
        df = df[(df[self.RAW_MUNICIPIO_COL] >= 1_000_000) & (df[self.RAW_MUNICIPIO_COL] < 10_000_000)]

        # conta IES federais distintas por município
        counts = (
            df.groupby(self.RAW_MUNICIPIO_COL)[self.RAW_IES_COL]
            .nunique()
            .reset_index(name=self.DATA_VALUE_COLUMN)
        )

        final = pd.DataFrame({
            self.CITY_CODE_COL: counts[self.RAW_MUNICIPIO_COL].astype(int),
            self.DATA_IDENTIFIER_COLUMN: self.SIGLA,
            self.YEAR_COLUMN: int(year),
            self.DATA_VALUE_COLUMN: counts[self.DATA_VALUE_COLUMN].astype(int),
        })[[self.CITY_CODE_COL, self.DATA_IDENTIFIER_COLUMN, self.YEAR_COLUMN, self.DATA_VALUE_COLUMN]]

        return final


if __name__ == "__main__":
    ext = CfedExtractor(save_csv=True, output_dir="cfed_output")
    cols = ext.extract_processed_collection()
    for c in cols:
        print("===", c.data_name, "years:", c.time_series_years)
        print(c.df.head())
