import re
from pathlib import Path
from typing import Optional
import requests
from .AbstractScrapper import AbstractScrapper
from dataclasses import dataclass
import pandas as pd
from datastructures import YearDataPoint
from enum import Enum
from typing import Optional
from datastructures import DataTypes


@dataclass(frozen=True)
class CnucQuerySpec:
    year: str
    # preferir "2º semestre" e cair pro "1º" se não achar
    prefer_second_semester: bool = True

    # dataset CKAN (slug)
    ckan_dataset_id: str = "unidadesdeconservacao"

    # se quiser “forçar” um arquivo específico (debug/hotfix)
    direct_csv_url: Optional[str] = None

class CnucDataInfo(Enum):
    # 4061 (MMA 2024): número de UCs no município
    CONSERVATION_UNITS_COUNT = {
        "data_identifier": "Número de Unidade de Conservação",
        "topic": "Ambiente",
        "dtype": DataTypes.INT,
        "spec": CnucQuerySpec(
            year="2024",
            direct_csv_url="https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/83498949-96ac-45b9-8be5-dcf9db4300eb/download/cnuc_2024_10.csv",
        ),
    }

    # 4062 (MMA 2025): bioma(s) associado(s)
    MUNICIPALITY_BIOME = {
        "data_identifier": "Tipo de BIOMA",
        "topic": "Ambiente",
        "dtype": DataTypes.INT,  # no extractor você decide se vira texto ou “biomas_count”, etc.
        "spec": CnucQuerySpec(
            year="2025",
            direct_csv_url="https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/a6e0d1ca-b589-499f-ad48-0c3ee9fb7de1/download/cnuc_2025_08.csv",
        ),
    }


class CnucScrapper(AbstractScrapper):
    """
    Baixa o CSV bruto do CNUC (MMA) por ano/semestre.
    Preferência: 2º semestre -> fallback 1º semestre.
    """

    CKAN_BASE = "https://dados.mma.gov.br"
    CKAN_PACKAGE_SHOW = CKAN_BASE + "/api/3/action/package_show?id={dataset_id}"

    def __init__(
        self,
        data_point_to_extract: CnucDataInfo,
        timeout: int = 240,
    ) -> None:
        self.data_point_to_extract = data_point_to_extract
        self.timeout = timeout

    # --------- CKAN helpers ---------

    def _ckan_package_show(self, dataset_id: str) -> dict:
        url = self.CKAN_PACKAGE_SHOW.format(dataset_id=dataset_id)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        payload = r.json()
        if not payload.get("success"):
            raise RuntimeError(f"CKAN package_show falhou para dataset_id={dataset_id}")
        return payload["result"]

    def _pick_resource_url(self, pkg: dict, spec: CnucQuerySpec) -> str:
        """
        Escolhe o recurso CSV do ano spec.year.
        Regra: tenta "CNUC_{year}_2" (se prefer_second_semester), senão "CNUC_{year}_1",
        senão qualquer CSV contendo o ano no nome.
        """
        resources = pkg.get("resources", [])
        csvs = []
        for r in resources:
            fmt = (r.get("format") or "").strip().upper()
            name = (r.get("name") or "").strip()
            url = (r.get("url") or "").strip()
            if fmt == "CSV" and url and name:
                csvs.append((name, url))

        if not csvs:
            raise RuntimeError("Nenhum recurso CSV encontrado no dataset.")

        year = spec.year

        def norm(s: str) -> str:
            return re.sub(r"\s+", " ", s).strip().lower()

        csvs_norm = [(norm(n), u, n) for (n, u) in csvs]

        # 1) match forte: CNUC_{year}_2º semestre
        if spec.prefer_second_semester:
            for n_norm, u, n_raw in csvs_norm:
                if f"cnuc_{year}" in n_norm and ("2" in n_norm and "semestre" in n_norm):
                    return u

        # 2) match forte: CNUC_{year}_1º semestre
        for n_norm, u, n_raw in csvs_norm:
            if f"cnuc_{year}" in n_norm and ("1" in n_norm and "semestre" in n_norm):
                return u

        # 3) fallback: qualquer CSV com o ano no nome
        for n_norm, u, n_raw in csvs_norm:
            if year in n_norm:
                return u

        raise RuntimeError(f"Não achei CSV para year={year} no dataset (tente direct_csv_url).")

    def extract_database(self):

        spec = self.data_point_to_extract.value["spec"]
        csv_path = self.scrape_csv()

        # ajuste sep/encoding se necessário (provavelmente ; e latin-1, como você testou)
        df = pd.read_csv(csv_path, sep=";", encoding="latin-1", on_bad_lines="skip")
        return [YearDataPoint(df=df, data_year=int(spec.year))]

    def _download(self, url: str, out_path: Path) -> str:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=self.timeout) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)

        if not out_path.exists() or out_path.stat().st_size == 0:
            raise RuntimeError(f"Download resultou em arquivo vazio: {out_path}")

        return str(out_path)

    # --------- API igual ao seu padrão ---------

    def scrape_csv(self) -> str:
        spec: CnucQuerySpec = self.data_point_to_extract.value["spec"]

        self._create_downloaded_files_dir()
        download_dir = Path(self.DOWNLOADED_FILES_PATH)

        # se você travou URL (ótimo pra começar e “congelar” o insumo por ano)
        if spec.direct_csv_url:
            fname = f"cnuc_{spec.year}.csv"
            return self._download(spec.direct_csv_url, download_dir / fname)

        # modo automático via CKAN
        pkg = self._ckan_package_show(spec.ckan_dataset_id)
        url = self._pick_resource_url(pkg, spec)

        # tenta manter nome “bonito”
        fname = f"cnuc_{spec.year}.csv"
        return self._download(url, download_dir / fname)