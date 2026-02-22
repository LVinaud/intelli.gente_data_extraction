import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from datastructures import YearDataPoint, DataTypes
from .AbstractScrapper import AbstractScrapper
from etl_config import get_env_var


@dataclass(frozen=True)
class RaisQuerySpec:
    year: str
    companies_section: bool = False
    series_url: Optional[str] = None
    series_link_text: Optional[str] = None

    location_dimension_text: str = "Município"
    rais_negativa_text: Optional[str] = None

    altera_setorial_id: int = 32
    altera_cnae_id: int = 37
    cnae_secao_text: str = "CNAE 2.0 Seção"

    categories_to_select: List[str] = field(default_factory=list)


class RaisDataInfo(Enum):
    """
    Agora no formato do extractor antigo: inclui metadata + spec.
    """
    TECH_JOBS = {
        "data_identifier": "Empregos em TIC",
        "topic": "Inovação",
        "dtype": DataTypes.INT,
        "companies_section": False,
        "spec": RaisQuerySpec(
            year="2024",
            companies_section=False,
            series_url="https://bi.mte.gov.br/bgcaged/caged_rais_vinculo_id/caged_rais_vinculo_basico_tab.php",
            altera_setorial_id=32,
            altera_cnae_id=37,
            categories_to_select=[
                "Atividades Profissionais, Científicas e Técnicas",
                "Informação e Comunicação",
            ],
        ),
    }

    TECH_COMPANIES = {
        "data_identifier": "Empresas de TICs no município",
        "topic": "Inovação",
        "dtype": DataTypes.INT,
        "companies_section": True,
        "spec": RaisQuerySpec(
            year="2024",
            companies_section=True,
            series_link_text="Ano corrente a 2022",
            altera_setorial_id=25,
            altera_cnae_id=35,
            rais_negativa_text="Não",
            categories_to_select=[
                "Atividades Profissionais, Científicas e Técnicas",
                "Informação e Comunicação",
            ],
        ),
    }


class RaisScrapper(AbstractScrapper):
   URL = "https://bi.mte.gov.br/bgcaged/login.php"
   USERNAME = get_env_var("RAIS_USERNAME") or "basico"
   PSSWD = get_env_var("RAIS_PSSWD") or "12345678"

   def __init__(
      self,
      data_point_to_extract: RaisDataInfo,
      headless: bool = True,
      webscrapping_delay_multiplier: int = 1,
      wait_timeout: int = 30,
      download_timeout: int = 240,
   ) -> None:
      self.data_point_to_extract = data_point_to_extract
      self.headless = headless
      self.webscrapping_delay_multiplier = max(1, int(webscrapping_delay_multiplier))
      self.wait_timeout = wait_timeout
      self.download_timeout = download_timeout

   def _sleep(self, seconds: float) -> None:
      time.sleep(seconds * self.webscrapping_delay_multiplier)

   def _wait(self, driver: webdriver.Chrome, timeout: Optional[int] = None) -> WebDriverWait:
      return WebDriverWait(driver, timeout or self.wait_timeout)

   def _build_driver(self, download_dir: str) -> webdriver.Chrome:
      chrome_options = Options()
      chrome_options.add_experimental_option("prefs", {
         "credentials_enable_service": False,
         "profile.password_manager_enabled": False,
         "download.default_directory": download_dir,
         "download.prompt_for_download": False,
         "download.directory_upgrade": True,
         "safebrowsing.enabled": True,
      })
      chrome_options.add_argument("--disable-save-password-bubble")
      chrome_options.add_argument("--disable-infobars")
      chrome_options.add_argument("--start-maximized")

      if self.headless:
         chrome_options.add_argument("--headless=new")
         chrome_options.add_argument("--disable-gpu")
         chrome_options.add_argument("--no-sandbox")
         chrome_options.add_argument("--disable-dev-shm-usage")

      driver = webdriver.Chrome(options=chrome_options)

      try:
         driver.execute_cdp_cmd("Page.setDownloadBehavior", {
               "behavior": "allow",
               "downloadPath": download_dir,
         })
      except Exception:
         pass

      return driver

   def _login(self, driver: webdriver.Chrome) -> None:
      driver.get(self.URL)

      self._wait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "input")))
      inputs = driver.find_elements(By.TAG_NAME, "input")

      submit_element = None
      for ele in inputs:
         t = (ele.get_attribute("type") or "").strip().lower()
         if t == "text":
               ele.clear()
               ele.send_keys(self.USERNAME)
         elif t == "password":
               ele.clear()
               ele.send_keys(self.PSSWD)
         elif t == "submit":
               submit_element = ele

      if submit_element is None:
         raise RuntimeError("Botão de login não encontrado.")

      submit_element.click()
      self._wait(driver, 20).until(EC.url_changes(self.URL))

   def _open_rais_home(self, driver: webdriver.Chrome) -> None:
      rais_link = self._wait(driver, 20).until(
         EC.element_to_be_clickable((By.XPATH, "//a[@href='rais.php']"))
      )
      rais_link.click()
      self._wait(driver, 20).until(EC.url_contains("rais.php"))

   def _open_series(self, driver: webdriver.Chrome, spec: RaisQuerySpec) -> None:
      if spec.series_url:
         driver.get(spec.series_url)
         driver.switch_to.default_content()
         return

      if not spec.series_link_text:
         raise RuntimeError("Spec sem series_url e sem series_link_text.")

      if spec.companies_section:
         # Estabelecimentos (não usa headerindex)
         section = self._wait(driver, 20).until(
            EC.element_to_be_clickable((
                  By.XPATH,
                  "//div[contains(@class,'area') and .//text()[contains(., 'RAIS ESTABELECIMENTO')]]"
            ))
         )
         driver.execute_script("arguments[0].click();", section)
      else:
         # Vínculos
         section = self._wait(driver, 20).until(
            EC.element_to_be_clickable((
                  By.XPATH,
                  "//div[contains(@class,'area') and .//text()[contains(., 'RAIS VÍNCULOS')]]"
            ))
         )
         driver.execute_script("arguments[0].click();", section)

      self._sleep(2)

      serie_link = self._wait(driver, 20).until(
         EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{spec.series_link_text}')]"))
      )
      driver.execute_script("arguments[0].click();", serie_link)
      self._sleep(1.5)

   def _apply_filters_in_principal(self, driver: webdriver.Chrome, spec: RaisQuerySpec) -> None:
      self._wait(driver, 20).until(EC.presence_of_element_located((By.NAME, "principal")))
      driver.switch_to.frame("principal")

      li_select = self._wait(driver, 20).until(EC.presence_of_element_located((By.NAME, "LI")))
      Select(li_select).select_by_visible_text(spec.location_dimension_text)
      driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", li_select)

      ano_select = self._wait(driver, 20).until(
         EC.presence_of_element_located((By.XPATH, "//select[@name='YCAno']"))
      )
      s_ano = Select(ano_select)
      try:
         s_ano.deselect_all()
      except Exception:
         pass
      s_ano.select_by_visible_text(spec.year)
      driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", ano_select)

      if spec.rais_negativa_text:
         neg_select = self._wait(driver, 20).until(
               EC.presence_of_element_located((By.XPATH, "//select[contains(@name,'YCInd Rais Negativa')]"))
         )
         s_neg = Select(neg_select)
         try:
               s_neg.deselect_all()
         except Exception:
               pass
         s_neg.select_by_visible_text(spec.rais_negativa_text)
         driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", neg_select)

      driver.switch_to.default_content()
      self._sleep(0.5)

   def _open_selections_and_choose_cnae_secao(self, driver: webdriver.Chrome, spec: RaisQuerySpec) -> None:
      self._wait(driver, 20).until(EC.presence_of_element_located((By.NAME, "lista")))
      driver.switch_to.frame("lista")

      selecoes = self._wait(driver, 20).until(
         EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Seleções por assunto')]"))
      )
      driver.execute_script("arguments[0].click();", selecoes)

      setorial = self._wait(driver, 20).until(
         EC.presence_of_element_located((By.XPATH, f"//td[contains(@onclick, 'Altera({spec.altera_setorial_id})')]"))
      )
      driver.execute_script("arguments[0].click();", setorial)

      cnae = self._wait(driver, 20).until(
         EC.presence_of_element_located((By.XPATH, f"//td[contains(@onclick, 'Altera({spec.altera_cnae_id})')]"))
      )
      driver.execute_script("arguments[0].click();", cnae)

      cnae_secao = self._wait(driver, 20).until(
         EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{spec.cnae_secao_text}')]"))
      )
      driver.execute_script("arguments[0].click();", cnae_secao)

      driver.switch_to.default_content()
      self._wait(driver, 20).until(
         EC.visibility_of_element_located((By.XPATH, "//select[@name='categorias']"))
      )
      self._sleep(0.3)

   def _select_categories(self, driver: webdriver.Chrome, spec: RaisQuerySpec) -> None:
      if not spec.categories_to_select:
         return

      driver.switch_to.default_content()
      wanted_raw = [c.strip() for c in spec.categories_to_select]

      def norm(s: str) -> str:
         return " ".join(s.replace("\xa0", " ").split()).casefold()

      wanted_norm = set(norm(x) for x in wanted_raw)

      self._wait(driver, 30).until(
         EC.presence_of_element_located((By.XPATH, "//select[@name='categorias']"))
      )

      selects = driver.find_elements(By.XPATH, "//select[@name='categorias']")
      best = None
      best_score = -1

      for sel in selects:
         opts = [o.text for o in sel.find_elements(By.TAG_NAME, "option")]
         opts_norm = [norm(t) for t in opts]
         score = sum(1 for w in wanted_norm if w in opts_norm)
         if score > best_score:
               best_score = score
               best = sel

      if best is None or best_score == 0:
         raise RuntimeError("Select 'categorias' não contém as opções desejadas.")

      driver.execute_script(
         """
         var select = arguments[0];
         var wanted = arguments[1];
         function norm(s){
            return s.replace(/\\u00a0/g,' ').trim().split(/\\s+/).join(' ').toLowerCase();
         }
         for (var i = 0; i < select.options.length; i++) {
            var t = norm(select.options[i].text);
            select.options[i].selected = wanted.includes(t);
         }
         select.dispatchEvent(new Event('change', { bubbles: true }));
         """,
         best,
         list(wanted_norm),
      )

      driver.execute_script("parent.principal.adicionaCategoria()")
      ok = driver.execute_script(
         "return parent.principal.grava_selecao_categorica(arguments[0]);",
         spec.cnae_secao_text
      )

      if ok:
         driver.execute_script("parent.principal.fechar_dlg_selecao();")
         self._sleep(0.5)
      else:
         raise RuntimeError("grava_selecao_categorica retornou False; seleção não foi aplicada.")

   def _execute_and_download_csv(self, driver: webdriver.Chrome, download_dir: str) -> str:
      driver.switch_to.default_content()

      iframe_ifrm = self._wait(driver, 60).until(
         EC.presence_of_element_located(
               (By.XPATH, "//*[@id='iFrm' or @name='iFrm'][self::iframe or self::frame]")
         )
      )
      driver.switch_to.frame(iframe_ifrm)

      executar = self._wait(driver, 30).until(
         EC.element_to_be_clickable((By.XPATH, "//a[contains(@href,'submete(0)')]"))
      )

      old_url = driver.current_url
      janela_principal = driver.current_window_handle

      driver.execute_script("arguments[0].click();", executar)

      self._wait(driver, 180).until(EC.url_changes(old_url))
      driver.switch_to.default_content()

      self._wait(driver, 240).until(EC.presence_of_element_located((By.NAME, "botoes")))
      driver.switch_to.frame("botoes")

      btn_csv = self._wait(driver, 60).until(
         EC.presence_of_element_located((By.XPATH, "//img[@name='EXCEL' or @title='Transfere arquivo CSV']"))
      )

      driver.execute_script("submetedns(arguments[0], 'exporte', 0);", btn_csv)

      try:
         self._wait(driver, 20).until(EC.number_of_windows_to_be(2))
         for h in driver.window_handles:
               if h != janela_principal:
                  driver.switch_to.window(h)
                  self._sleep(2)
                  driver.close()
         driver.switch_to.window(janela_principal)
      except Exception:
         pass

      return self._wait_for_downloaded_csv(download_dir, timeout=self.download_timeout)

   def _wait_for_downloaded_csv(self, folder: str, timeout: int) -> str:
      folder_path = Path(folder)
      start = time.time()
      last_csv: Optional[Path] = None

      while time.time() - start < timeout:
         csvs = sorted(folder_path.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
         partials = list(folder_path.glob("*.crdownload"))

         if csvs:
               last_csv = csvs[0]

         if last_csv and not partials:
               try:
                  s1 = last_csv.stat().st_size
                  time.sleep(1)
                  s2 = last_csv.stat().st_size
                  if s1 == s2 and s2 > 0:
                     return str(last_csv)
               except FileNotFoundError:
                  pass

         time.sleep(1)

      raise TimeoutError(f"CSV não finalizou em {timeout}s (último visto: {last_csv}).")

   # retorna só o caminho do CSV (pra extractor orquestrar)
   def scrape_csv(self) -> str:
      spec: RaisQuerySpec = self.data_point_to_extract.value["spec"]

      self._create_downloaded_files_dir()
      download_dir = self.DOWNLOADED_FILES_PATH

      driver = self._build_driver(download_dir)
      try:
         self._login(driver)
         self._open_rais_home(driver)
         self._open_series(driver, spec)

         self._apply_filters_in_principal(driver, spec)
         self._open_selections_and_choose_cnae_secao(driver, spec)
         self._select_categories(driver, spec)

         return self._execute_and_download_csv(driver, download_dir)
      finally:
         driver.quit()

   # mantém compat: ainda retorna YearDataPoint (mas sem usar extractor)
   def extract_database(self) -> List[YearDataPoint]:
      spec: RaisQuerySpec = self.data_point_to_extract.value["spec"]
      csv_path = self.scrape_csv()
      df = pd.read_csv(csv_path, sep=";", encoding="latin-1", on_bad_lines="skip")

      self._delete_download_files_dir()
      return [YearDataPoint(df=df, data_year=int(spec.year))]