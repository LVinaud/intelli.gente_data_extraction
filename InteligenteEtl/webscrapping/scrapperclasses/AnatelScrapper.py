from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import zipfile
import shutil
try:
    from .AbstractScrapper import AbstractScrapper
except ImportError:
    # Hack to allow running as a script
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from AbstractScrapper import AbstractScrapper

class AnatelScrapper(AbstractScrapper):
    URL = "https://informacoes.anatel.gov.br/paineis/acessos"
    ESTACOES_URL = "https://informacoes.anatel.gov.br/paineis/outorga-e-licenciamento/estacoes-do-smp"

    def download_data(self):
        # Define download directory: .../intelli.gente_data_extraction/anatel_2024
        # Assuming this file is in .../InteligenteEtl/webscrapping/scrapperclasses/
        # base_dir should be .../intelli.gente_data_extraction
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        download_dir = os.path.join(base_dir, 'anatel_2024')
        
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            
        print(f"Download directory: {download_dir}")

        # Configure Chrome options
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        # options.add_argument("--headless") # User requested to see the browser

        driver = webdriver.Chrome(options=options)
        
        try:
            print(f"Navigating to {self.URL}...")
            driver.get(self.URL)
            
            # Wait for page load - User said it takes time
            print("Waiting for page load (30s)...")
            time.sleep(30) 
            
            # Click "Dados Brutos"
            print("Clicking 'Dados Brutos'...")
            wait = WebDriverWait(driver, 30)
            clicked = False
            
            # Strategy 1: CSS Selector
            try:
                print("Strategy 1: CSS Selector a.dados-abertos")
                btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.dados-abertos")))
                btn.click()
                clicked = True
            except Exception as e:
                print(f"Strategy 1 failed: {e}")
            
            # Strategy 2: Partial Link Text
            if not clicked:
                try:
                    print("Strategy 2: Partial Link Text 'Dados Brutos'")
                    btn = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Dados Brutos")))
                    btn.click()
                    clicked = True
                except Exception as e:
                    print(f"Strategy 2 failed: {e}")

            # Strategy 3: JavaScript Click
            if not clicked:
                try:
                    print("Strategy 3: JavaScript Click")
                    btn = driver.find_element(By.CSS_SELECTOR, "a.dados-abertos")
                    driver.execute_script("arguments[0].click();", btn)
                    clicked = True
                except Exception as e:
                    print(f"Strategy 3 failed: {e}")
            
            if not clicked:
                # Save debug info
                driver.save_screenshot(os.path.join(download_dir, "debug_screenshot.png"))
                with open(os.path.join(download_dir, "page_source.html"), "w") as f:
                    f.write(driver.page_source)
                raise Exception("Failed to click 'Dados Brutos' with all strategies.")
            
            time.sleep(5)
            
            # Find download links
            print("Finding download links...")
            links = driver.find_elements(By.TAG_NAME, "a")
            fixed_broadband_url = None
            mobile_url = None
            
            for link in links:
                try:
                    text = link.text.strip()
                    href = link.get_attribute("href")
                    if not href:
                        continue
                        
                    if "Banda Larga Fixa" in text and href.endswith(".zip"):
                        fixed_broadband_url = href
                        print(f"Found Fixed Broadband URL: {href}")
                    elif "Telefonia Móvel" in text and href.endswith(".zip"):
                        mobile_url = href
                        print(f"Found Mobile Telephony URL: {href}")
                except:
                    continue
            
            # Trigger downloads
            if fixed_broadband_url:
                print(f"Downloading Fixed Broadband...")
                driver.get(fixed_broadband_url)
            else:
                print("Error: Fixed Broadband link not found!")

            if mobile_url:
                print(f"Downloading Mobile Telephony...")
                driver.get(mobile_url)
            else:
                print("Error: Mobile Telephony link not found!")
                
             # Wait for downloads to finish
            print("Waiting for downloads to complete...")
            self.wait_for_downloads(download_dir)

            # --- NEW: Download estacoes_municipio_faixa ---
            print(f"Navigating to {self.ESTACOES_URL}...")
            driver.get(self.ESTACOES_URL)
            
            print("Waiting for page load (15s)...")
            time.sleep(15) # User requested wait
            
            print("Clicking 'Exportar Dados'...")
            wait = WebDriverWait(driver, 30)
            try:
                # Selector based on user description and image
                export_btn = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.grafico-a-esquerda button.qsbutton[title='Exportar Dados']")
                ))
                # Use JS click to avoid interception
                driver.execute_script("arguments[0].click();", export_btn)
                
                # Wait for download to start/finish
                print("Waiting for file to download...")
                time.sleep(5) # Give it some time to start
                self.wait_for_any_download(download_dir)
                
                # Identify and rename the file
                # The file has a weird name, so we look for the most recent file that is NOT one of the known ones
                # Or simply the most recent file created
                files = [os.path.join(download_dir, f) for f in os.listdir(download_dir)]
                files.sort(key=os.path.getmtime)
                newest_file = files[-1]
                
                print(f"Newest file found: {newest_file}")
                
                # Determine extension
                _, ext = os.path.splitext(newest_file)
                target_name = "estacoes_municipio_faixa" + ext
                target_path = os.path.join(download_dir, target_name)
                
                # Rename
                if newest_file != target_path:
                    shutil.move(newest_file, target_path)
                    print(f"Renamed {os.path.basename(newest_file)} to {target_name}")
                
            except Exception as e:
                print(f"Failed to download estacoes_municipio_faixa: {e}")
                driver.save_screenshot(os.path.join(download_dir, "error_estacoes.png"))

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            print("Closing browser...")
            driver.quit()
            
        # Extract and Cleanup
        self.process_files(download_dir)

    def wait_for_any_download(self, download_dir):
        # Wait until no .crdownload or .part files exist
        timeout = 120 # 2 minutes max
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            files = os.listdir(download_dir)
            if any(f.endswith('.crdownload') or f.endswith('.part') for f in files):
                time.sleep(1)
                continue
            
            # If no temp files, assume download finished (if it started)
            # We waited 5s before calling this, so it should have started.
            return
            
        print("Timeout waiting for any download.")

    def wait_for_downloads(self, download_dir):
        # Wait until no .crdownload or .part files exist
        # And ensure at least one zip file exists (since we expect zips)
        timeout = 600 # 10 minutes max
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            files = os.listdir(download_dir)
            if any(f.endswith('.crdownload') or f.endswith('.part') for f in files):
                time.sleep(2)
                continue
            
            # Check if zips are present (we expect 2, but maybe only 1 worked)
            zips = [f for f in files if f.endswith('.zip')]
            if len(zips) >= 2: # We expect 2 zips
                # Wait a bit more to ensure file release
                time.sleep(5)
                return
            
            time.sleep(2)
        print("Timeout waiting for downloads.")

    def process_files(self, download_dir):
        print("Processing downloaded files...")
        # Extract zips
        for item in os.listdir(download_dir):
            if item.endswith(".zip"):
                file_path = os.path.join(download_dir, item)
                print(f"Extracting {item}...")
                try:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(download_dir)
                    os.remove(file_path) # Remove zip after extraction
                except zipfile.BadZipFile:
                    print(f"Error: {item} is a bad zip file.")
        
        # Filter relevant files
        # We want to keep:
        # - Acessos_Banda_Larga_Fixa_2024.csv
        # - Acessos_Telefonia_Movel_2024_1S.csv
        # - Acessos_Telefonia_Movel_2024_2S.csv
        # - estacoes_municipio_faixa.xlsx (if present)
        # - broadband_indicators.csv (output file)
        
        keep_patterns = [
            "Acessos_Banda_Larga_Fixa_2024.csv",
            "Acessos_Telefonia_Movel_2024_1S.csv",
            "Acessos_Telefonia_Movel_2024_2S.csv",
            "estacoes_municipio_faixa.xlsx",
            "estacoes_municipio_faixa.csv",
            "broadband_indicators.csv",
            "debug_screenshot.png",
            "page_source.html"
        ]
        
        print("Cleaning up directory...")
        for file in os.listdir(download_dir):
            file_path = os.path.join(download_dir, file)
            if os.path.isfile(file_path):
                if file not in keep_patterns:
                    # Be careful not to delete other important files if they exist
                    # But the user said "excluir todo o resto".
                    # Let's be slightly conservative and only delete CSVs that don't match or other artifacts
                    # Actually, the zip extraction might produce many files.
                    # Let's assume we only want the specific ones.
                    print(f"Deleting {file}...")
                    os.remove(file_path)
        print("Done.")

    def extract_database(self) -> str:
        self.download_data()
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        download_dir = os.path.join(base_dir, 'anatel_2024')
        return download_dir

if __name__ == "__main__":
    scraper = AnatelScrapper()
    scraper.extract_database()

AnatelFixedConnectionScrapper = AnatelScrapper
