# intelli.gente Data Extraction

## Visao Geral

Projeto de Iniciacao Cientifica para coleta automatizada de dados de **cidades inteligentes brasileiras**. Extrai **485 variaveis** de **multiplas fontes publicas** (ANATEL, IBGE, INEP, DataSUS, SINISA, MCTI, etc.) para calcular **135 indicadores** de sustentabilidade urbana, cobrindo 5.570 municipios.

Os dados sao inseridos em um Data Warehouse PostgreSQL com esquema estrela (tabelas fato por topico + dimensoes de municipio e dado).

## Estrutura de Diretorios

```
intelli.gente_data_extraction/
├── main.py                          # Ponto de entrada - le sources_to_extract.json e executa extracoes
├── sources_to_extract.json          # Config: quais classes executar e quais indicadores extrair
├── requirements.txt                 # Dependencias Python
├── Dockerfile                       # Container para execucao
├── Dados - inteligente.xlsx         # Planilha de referencia com 485 variaveis e 135 indicadores
├── find_to_csv.py                   # Utilitario para encontrar CSVs
├── fix_to_csv.py                    # Utilitario para corrigir CSVs
│
├── InteligenteEtl/                  # Pacote principal
│   ├── setup.py / pyproject.toml
│   │
│   ├── webscrapping/
│   │   ├── scrapperclasses/         # Classes que BAIXAM dados brutos (Selenium/requests)
│   │   │   ├── AbstractScrapper.py  # Classe base - download ZIP, leitura CSV/XLSX/ODS
│   │   │   ├── AnatelScrapper.py
│   │   │   ├── DatasusLinkScrapper.py
│   │   │   ├── IbgeMunicScrapper.py
│   │   │   ├── IbgePibCidadesScrapper.py
│   │   │   ├── IbgeCitiesNetworkScrapper.py
│   │   │   ├── IdebFinalYearsScrapper.py
│   │   │   ├── IdhScrapper.py
│   │   │   ├── SinisaScrapper.py
│   │   │   ├── FormalJobsScrapper.py
│   │   │   ├── CityPaymentsScrapper.py
│   │   │   ├── SchoolDistortionRatesScrapper.py
│   │   │   ├── HigherEducaPositionsScrapper.py
│   │   │   ├── TechEquipamentScrapper.py
│   │   │   ├── RaisScrapper.py
│   │   │   ├── CnucScrapper.py
│   │   │   └── SnisScrapper(quebrado).py      # QUEBRADO
│   │   │
│   │   └── extractorclasses/        # Classes que PROCESSAM dados brutos -> formato padrao
│   │       ├── AbstractDataExtractor.py  # Classe base - mapeamento codigos IBGE, padronizacao
│   │       ├── AnatelExtractor.py
│   │       ├── DatasusDataExtractor.py
│   │       ├── IbgeMunicExtractor.py
│   │       ├── IbgePibCidadesDataExtractor.py
│   │       ├── IbgeCitiesNetworkExtractor.py
│   │       ├── IdebFinalYearsExtractor.py
│   │       ├── IdhExtractor.py
│   │       ├── SinisaExtractor.py
│   │       ├── FormalJobsExtractor.py
│   │       ├── CityPaymentsExtractor.py
│   │       ├── SchoolDistortionRatesExtractor.py
│   │       ├── HigherEducaPositionsExtractor.py
│   │       ├── TechEquipamentExtractor.py
│   │       ├── EmecExtractor.py               # Standalone (sem Scrapper)
│   │       ├── RaisExtractor.py
│   │       ├── CnucExtractor.py
│   │       └── SnisExtractor(quebrado).py      # QUEBRADO
│   │
│   ├── apiextractors/               # Extratores baseados em API REST
│   │   ├── apiclasses/
│   │   │   ├── AbstractApiInterface.py   # Classe base para APIs
│   │   │   ├── anatelapi/AnatelApi.py
│   │   │   ├── ibgeagregatesapi/IbgeAgregatesApi.py
│   │   │   └── ipeaviolencemap/IpeaViolenceMapApi.py
│   │   └── apidataclasses/
│   │       ├── DataCollections.py
│   │       └── DataLine.py
│   │
│   ├── datamaps/                    # Mapeamentos de dados (JSON configs)
│   │   ├── ApiDataMaps.py           # Carrega JSONs de mapeamento
│   │   ├── MunicDataMaps.py         # Mapeamento especifico IBGE MUNIC
│   │   ├── AnatelApiDataMap.json
│   │   ├── IbgeAgregatesApiDataMap.json
│   │   ├── munic_data_information.json
│   │   └── munic_data_codes_per_year.json
│   │
│   ├── datastructures/              # Estruturas de dados comuns
│   │   ├── DataCollection.py        # ProcessedDataCollection (saida padrao)
│   │   ├── DataEnums.py             # DataTypes (INT/FLOAT/STRING/BOOL), BaseFileType
│   │   └── YearDataPoint.py         # Wrapper: DataFrame + ano
│   │
│   ├── extractionhandler/
│   │   └── ExtractorClassesHandler.py  # Orquestrador: descobre classes, executa, loga, insere no BD
│   │
│   ├── citiesinfo/
│   │   ├── CitiesInfo.py            # Mapeamento nome<->codigo IBGE dos 5570 municipios
│   │   └── info_municipios_ibge.csv
│   │
│   ├── dbInterface/                 # Interface com PostgreSQL
│   │   ├── data_insertion.py        # Insercao em tabelas fato
│   │   ├── dimension_tables.py      # Consulta tabelas dimensao
│   │   └── utils.py                 # Utilitarios SQL
│   │
│   ├── etl_config/
│   │   ├── etl_config.json          # Constantes: nomes de colunas padrao, limites
│   │   ├── etl_config.py            # Carrega config + classes de Log
│   │   ├── keys.env                 # Credenciais (BD, RAIS)
│   │   └── logging.py
│   │
│   ├── data/                        # Dados intermediarios/processados
│   │   ├── emec_raw/emec_raw.csv
│   │   ├── gini/
│   │   └── rais/
│   │
│   ├── tempfiles/                   # Arquivos temporarios de download
│   │
│   └── *.csv                        # ~250 CSVs extraidos do IBGE MUNIC (variaveis MTIC*, MGOV*, etc.)
│
├── test/                            # Testes
├── intelienv/                       # Virtual environment
└── readme_images/
```

## Arquitetura e Fluxo de Dados

```
sources_to_extract.json
        │
        ▼
ExtractorClassesHandler (orquestrador)
        │
        ├── Descobre classes via introspeccao (globals + inspect)
        ├── Para cada fonte requisitada:
        │
        ▼
   Scrapper (baixa dados brutos)
        │  - Selenium WebDriver ou requests/HTTP
        │  - Download ZIP/XLSX/CSV
        │  - Extrai arquivos de ZIPs
        ▼
   Extractor (processa dados)
        │  - Filtra/agrega por municipio
        │  - Converte codigos IBGE (6→7 digitos)
        │  - Padroniza colunas:
        │     codigo_municipio | dado_identificador | ano | tipo_dado | valor
        ▼
   ProcessedDataCollection
        │  - category: str (nome do dado)
        │  - dtype: DataTypes
        │  - data_name: str (sigla da variavel)
        │  - time_series_years: list[int]
        │  - df: pd.DataFrame
        ▼
   data_insertion.py
        │  - Troca codigo_municipio por FK da dimensao_municipio
        │  - Troca dado_identificador por FK da dimensao_dado
        │  - Insere na tabela fato do topico correspondente
        ▼
   PostgreSQL (Data Warehouse - esquema estrela)
```

### Colunas Padrao do DataFrame de Saida

Definidas em `etl_config.json`:
- `municipio_cod_ibge` / `codigo_municipio` - Codigo IBGE 7 digitos
- `ano` - Ano do dado
- `variavel_sigla` / `dado_identificador` - Sigla da variavel
- `variavel_valor` / `valor` - Valor do dado
- `tipo_dado` - Tipo (int/float/str/bool)

## Extratores Implementados

### Scrapper + Extractor (Web Scraping)

| # | Classe | Fonte | Metodo | Variaveis | Status |
|---|--------|-------|--------|-----------|--------|
| 1 | Anatel | ANATEL | Selenium | Acesso_SCM, Acesso_SCM>=12Mbps, ECFO, EC3G, EC4G, COB5G, TOT_ACESSOS_3G, TOT_ACESSOS_4G_WCMDA, QNTD_EST_SMP | Funcional |
| 2 | Datasus | DataSUS | Selenium + links CSV | GINI, POP_ANALF, obitos maternos, nascidos vivos, medicos SUS, leitos, nascidos baixo peso/pre-natal, obitos infantis | Funcional |
| 3 | IbgeMunic | IBGE MUNIC | Download XLSX FTP | ~232 variaveis dinamicas (MTIC*, MGOV*, MMAM*, MGRD*, MCUL*, MTRA*, etc.) | Funcional |
| 4 | IbgePibCidades | IBGE PIB | HTTP + ZIP | PIB_PERCAP, PIB_AG, PIB_IND, PIB_SRV, PIB_AP | Funcional |
| 5 | IbgeCitiesNetwork | IBGE REGIC | Download XLSX | Nivel Hierarquia, Classe Hierarquia | Funcional |
| 6 | IdebFinalYears | INEP IDEB | ZIP + XLSX | IDEB (VL_OBSERVADO) | Funcional |
| 7 | Idh | Atlas Brasil | Selenium | IDHM | Funcional |
| 8 | Sinisa | SINISA | Portal scraping + ZIP | ~14 indicadores agua/esgoto/residuos | Funcional |
| 9 | FormalJobs | IBGE SNIG | Selenium + CSV | POP_OCVE (populacao ocupada vinculo formal) | Funcional |
| 10 | CityPayments | Tesouro Transparente | Selenium + XLSX | CAPAG | Funcional |
| 11 | SchoolDistortionRates | INEP | ZIP multi-ano | DISTIDSER (taxa distorcao idade-serie) | Funcional |
| 12 | HigherEducaPositions | INEP Censo Superior | ZIP + CSV | QT_VAGA_TOTAL (vagas ensino superior) | Funcional |
| 13 | TechEquipament | INEP Censo Escolar | ZIP + CSV | IN_LABORATORIO_INFORMATICA, IN_DESKTOP_ALUNO, IN_COMP_PORTATIL_ALUNO, IN_TABLET_ALUNO, IN_EQUIP_LOUSA_DIGITAL, IN_EQUIP_MULTIMIDIA, IN_INTERNET, IN_INTERNET_APRENDIZAGEM, QT_MAT_FUND, QT_MAT_BAS, QT_DESKTOP_ALUNO, QT_COMP_PORTATIL_ALUNO, ESC_MUN | Funcional |
| 14 | Emec | EMEC | CSV local | IES publicas por municipio | Funcional |
| 15 | Rais | RAIS (MTe BI) | Selenium + login | EMP_TICM, EMPG_TIC, EMPG_TUR | Funcional |
| 16 | Cnuc | MMA (CKAN API) | API + CSV | UCONS (UCs), TBIOM (bioma) | Funcional |
| 17 | Snis | SNIS (antigo) | Portal antigo | varios indicadores saneamento | **QUEBRADO** |

### API Extractors

| # | Classe | Fonte | Variaveis |
|---|--------|-------|-----------|
| 1 | IbgeAgregatesApi | IBGE Agregados API | POP_TOT, ARV, ILUM, PAVIM, POP_FAV, FAVCOMUNURB e outros do Censo |
| 2 | IpeaViolenceMapApi | IPEA Atlas Violencia | THOM (taxa homicidios) |
| 3 | AnatelApi | ANATEL API | (complementar ao scrapper) |

## Padroes de Codigo

### Criar um Novo Extrator

1. **Criar Scrapper** em `webscrapping/scrapperclasses/`:
   - Herdar de `AbstractScrapper`
   - Implementar metodo que baixa dados brutos (CSV/XLSX/ZIP)
   - Usar `self._download_and_extract_zip()`, `self._read_file()` do AbstractScrapper
   - Retornar DataFrame(s) ou lista de `YearDataPoint`

2. **Criar Extractor** em `webscrapping/extractorclasses/`:
   - Herdar de `AbstractDataExtractor`
   - Implementar `extract_processed_collection()` que retorna `list[ProcessedDataCollection]`
   - Usar `self._get_ibge_city_code()` para mapear nomes de cidade
   - Padronizar saida com colunas: `codigo_municipio`, `dado_identificador`, `ano`, `tipo_dado`, `valor`

3. **Registrar nos `__init__.py`**:
   - Adicionar import em `webscrapping/scrapperclasses/__init__.py`
   - Adicionar import em `webscrapping/extractorclasses/__init__.py`

4. O `ExtractorClassesHandler` descobre automaticamente classes com "Extractor" ou "Api" no nome.

### Criar um Novo API Extractor

1. **Criar classe** em `apiextractors/apiclasses/<nome>/`:
   - Herdar de `AbstractApiInterface`
   - Criar JSON de mapeamento em `datamaps/`
   - Implementar `extract_processed_collection()`

2. **Registrar** em `apiextractors/__init__.py`

### Convencoes

- Codigos IBGE sempre 7 digitos (converter de 6 se necessario)
- Nomes de variaveis seguem siglas da planilha de referencia
- DataTypes: INT, FLOAT, STRING, BOOL
- Selenium com ChromeDriver para sites que precisam de JS
- Credenciais em `etl_config/keys.env` (BD PostgreSQL, RAIS login)
- CSVs extraidos do IBGE MUNIC ficam soltos na raiz de `InteligenteEtl/`

## Dependencias Principais

- **pandas** - Manipulacao de dados
- **selenium** - Automacao de browser
- **requests** - HTTP
- **beautifulsoup4** - Parsing HTML
- **openpyxl** - Leitura XLSX
- **xlrd** - Leitura XLS antigo
- **psycopg2-binary** - PostgreSQL
- **python-dotenv** - Variaveis de ambiente

## Como Executar

```bash
# Ativar venv
source intelienv/bin/activate

# Executar todas as extracoes
python main.py

# Executar com log customizado
python main.py caminho/para/log

# Configurar quais fontes extrair em sources_to_extract.json:
# {"todos": []}                              -> extrai tudo
# {"DatasusDataExtractor": ["GINI_COEF"]}   -> extrai GINI do DataSUS
# {"AnatelExtractor": []}                    -> extrai todos indicadores ANATEL
```

## Cobertura: Fontes com Extrator vs. Fontes Pendentes

### Fontes COM extrator implementado:
- **ANATEL** (9 vars) - AnatelExtractor + AnatelApi
- **DataSUS** (8 vars) - DatasusDataExtractor
- **IBGE MUNIC** (232 vars) - IbgeMunicExtractor
- **IBGE PIB** (5 vars) - IbgePibCidadesDataExtractor
- **IBGE REGIC** (2 vars) - IbgeCitiesNetworkExtractor
- **IBGE CENSO** (parcial ~6 vars) - IbgeAgregatesApi (POP_TOT, ARV, ILUM, PAVIM, POP_FAV, FAVCOMUNURB)
- **INEP IDEB** (1 var) - IdebFinalYearsExtractor
- **INEP CENSO ESCOLAR** (13 vars) - TechEquipamentExtractor
- **INEP CENSO SUPERIOR** (1 var) - HigherEducaPositionsExtractor
- **STN** (1 var CAPAG) - CityPaymentsExtractor
- **SINISA** (14 vars) - SinisaExtractor
- **CAGED/RAIS** (3 vars) - RaisExtractor
- **IPEA** (1 var THOM) - IpeaViolenceMapApi
- **MMA/CNUC** (2 vars) - CnucExtractor
- **Atlas Brasil** (1 var IDHM) - IdhExtractor
- **EMEC/MEC** (1 var) - EmecExtractor
- **Emprego formal IBGE** (1 var) - FormalJobsExtractor

### Fontes SEM extrator (pendentes):
- **Formulario** (170 vars) - Dados coletados manualmente via formulario, nao automatizaveis
- **MCTI** (8 vars: CIPCRED, EMPPqTec, EMPRESHAB, EMPRLB, IEPCRED, INCCRED, MNADT e derivados) - Falta extrator
- **IBGE CENSO** (parcial ~4 vars faltantes: ex. PAVIM, ILUM dependendo do ano)
- **ANM** (1 var: MINER - mineradoras)
- **IPEADATA** (1 var: GINI serie historica - parcialmente coberto via DataSUS)
- **IPEA AV** (1 var: GTA3003 - areas verdes)
- **Data centers** (1 var: DCINST - sem fonte definida)
- **Energia eolica** (1 var: UEEOL - sem fonte definida)

### Variaveis do SNIS (extrator QUEBRADO):
- PO048, PO028 e outros indicadores de saneamento - Substituido parcialmente pelo SINISA
