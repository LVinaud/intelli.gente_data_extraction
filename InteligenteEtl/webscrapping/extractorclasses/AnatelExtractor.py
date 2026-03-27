import pandas as pd
import os
from datastructures import ProcessedDataCollection, DataTypes
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses.AnatelScrapper import AnatelScrapper


class AnatelExtractor(AbstractDataExtractor):
    """
    Classe extratora unificada para os dados da Anatel.
    Combina o scraping (AnatelScrapper) com o processamento dos indicadores
    (anteriormente em extract_indicators_anatel.py) em um fluxo único.

    Indicadores calculados:
        1. Acesso_SCM - Escala de acesso a banda larga fixa
        2. ECFO - Cobertura de fibra ótica
        3. Acesso_SCM>=12Mbps - Acesso a banda larga fixa de alta velocidade
        4. TOT_ACESSOS_3G - Total de acessos 3G
        5. TOT_ACESSOS_4G_WCMDA - Total de acessos 4G
        6. EC3G - Cobertura de rede 3G
        7. EC4G - Cobertura de rede 4G
        8. COB5G - Cobertura de rede 5G
        9. QNTD_EST_SMP - Quantidade de estações SMP
    """

    __scrapper: AnatelScrapper = AnatelScrapper()

    UF_MAP = {
        'RO': 'Rondônia', 'AC': 'Acre', 'AM': 'Amazonas', 'RR': 'Roraima',
        'PA': 'Pará', 'AP': 'Amapá', 'TO': 'Tocantins', 'MA': 'Maranhão',
        'PI': 'Piauí', 'CE': 'Ceará', 'RN': 'Rio Grande do Norte',
        'PB': 'Paraíba', 'PE': 'Pernambuco', 'AL': 'Alagoas',
        'SE': 'Sergipe', 'BA': 'Bahia', 'MG': 'Minas Gerais',
        'ES': 'Espírito Santo', 'RJ': 'Rio de Janeiro', 'SP': 'São Paulo',
        'PR': 'Paraná', 'SC': 'Santa Catarina', 'RS': 'Rio Grande do Sul',
        'MS': 'Mato Grosso do Sul', 'MT': 'Mato Grosso', 'GO': 'Goiás',
        'DF': 'Distrito Federal'
    }

    def __process_fixed_broadband(self, download_dir: str) -> pd.DataFrame:
        """Processa indicadores de banda larga fixa (Acesso_SCM, ECFO, Acesso_SCM>=12Mbps)."""
        input_file = os.path.join(download_dir, 'Acessos_Banda_Larga_Fixa_2025.csv')
        print(f"Loading fixed broadband data from {input_file}...")

        try:
            df = pd.read_csv(input_file, sep=';', decimal=',', encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8 decode failed, trying latin-1...")
            df = pd.read_csv(input_file, sep=';', decimal=',', encoding='latin-1')

        df['Acessos'] = pd.to_numeric(df['Acessos'], errors='coerce').fillna(0)

        # Indicator 1: Acesso_SCM (total de acessos por município)
        print("Calculating Acesso_SCM...")
        acesso_scm = df.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
        acesso_scm.rename(columns={'Acessos': 'Acesso_SCM'}, inplace=True)

        # Indicator 2: ECFO (1 se existe acesso via fibra, 0 caso contrário)
        print("Calculating ECFO...")
        df['is_fiber'] = (df['Meio de Acesso'] == 'Fibra').astype(int)
        cobertura_fibra = df.groupby('Código IBGE Município')['is_fiber'].max().reset_index()
        cobertura_fibra.rename(columns={'is_fiber': 'ECFO'}, inplace=True)

        # Indicator 3: Acesso_SCM>=12Mbps (acessos >= 12Mbps)
        print("Calculating Acesso_SCM>=12Mbps...")
        high_speed_ranges = ['12Mbps a 34Mbps', '> 34Mbps']
        df_high_speed = df[df['Faixa de Velocidade'].isin(high_speed_ranges)]
        acesso_high_speed = df_high_speed.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
        acesso_high_speed.rename(columns={'Acessos': 'Acesso_SCM>=12Mbps'}, inplace=True)

        # Merge dos indicadores de banda larga fixa
        print("Merging fixed broadband indicators...")
        fixed_df = acesso_scm
        fixed_df = fixed_df.merge(cobertura_fibra, on='Código IBGE Município', how='outer')
        fixed_df = fixed_df.merge(acesso_high_speed, on='Código IBGE Município', how='outer')

        fixed_df['Acesso_SCM'] = fixed_df['Acesso_SCM'].fillna(0)
        fixed_df['Acesso_SCM>=12Mbps'] = fixed_df['Acesso_SCM>=12Mbps'].fillna(0)
        fixed_df['ECFO'] = fixed_df['ECFO'].fillna(0).astype(int)

        return fixed_df

    def __process_mobile(self, download_dir: str) -> tuple:
        """Processa indicadores de telefonia móvel (TOT_ACESSOS_3G, TOT_ACESSOS_4G_WCMDA, EC3G, EC4G, COB5G)."""
        mobile_input_file = os.path.join(download_dir, 'Acessos_Telefonia_Movel_2025_2S.csv')
        print(f"Loading mobile data from {mobile_input_file}...")

        chunk_size = 500000
        mobile_columns = ['Mês', 'Tipo de Produto', 'Tecnologia Geração', 'Acessos', 'Código IBGE Município']

        acesso_3g_accum = []
        acesso_4g_accum = []
        coverage_accum = []

        try:
            for chunk in pd.read_csv(mobile_input_file, sep=';', decimal=',', encoding='utf-8-sig',
                                     usecols=mobile_columns, chunksize=chunk_size):
                # Filtra dezembro 2025 e VOZ+DADOS
                chunk = chunk[(chunk['Mês'] == 12) & (chunk['Tipo de Produto'] == 'VOZ+DADOS')]
                if chunk.empty:
                    continue

                chunk['Acessos'] = pd.to_numeric(chunk['Acessos'], errors='coerce').fillna(0)

                # Indicator 4 & 5: Acesso via 3G e 4G separados
                chunk_3g = chunk[chunk['Tecnologia Geração'] == '3G']
                if not chunk_3g.empty:
                    agg_acesso_3g = chunk_3g.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
                    acesso_3g_accum.append(agg_acesso_3g)

                chunk_4g = chunk[chunk['Tecnologia Geração'] == '4G']
                if not chunk_4g.empty:
                    agg_acesso_4g = chunk_4g.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
                    acesso_4g_accum.append(agg_acesso_4g)

                # Indicators 5 & 6 (now shifted): Flags de cobertura
                chunk['has_3g'] = (chunk['Tecnologia Geração'] == '3G').astype(int)
                chunk['has_4g'] = (chunk['Tecnologia Geração'] == '4G').astype(int)
                chunk['has_5g'] = (chunk['Tecnologia Geração'] == '5G').astype(int)

                agg_cov = chunk.groupby('Código IBGE Município')[['has_3g', 'has_4g', 'has_5g']].max().reset_index()
                coverage_accum.append(agg_cov)

        except Exception as e:
            print(f"Error loading mobile data: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        print("Aggregating mobile results...")

        # Agregação final de acessos 3G e 4G
        if acesso_3g_accum:
            acesso_3g_total = pd.concat(acesso_3g_accum)
            acesso_3g_final = acesso_3g_total.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
            acesso_3g_final.rename(columns={'Acessos': 'TOT_ACESSOS_3G'}, inplace=True)
        else:
            acesso_3g_final = pd.DataFrame(columns=['Código IBGE Município', 'TOT_ACESSOS_3G'])

        if acesso_4g_accum:
            acesso_4g_total = pd.concat(acesso_4g_accum)
            acesso_4g_final = acesso_4g_total.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
            acesso_4g_final.rename(columns={'Acessos': 'TOT_ACESSOS_4G_WCMDA'}, inplace=True)
        else:
            acesso_4g_final = pd.DataFrame(columns=['Código IBGE Município', 'TOT_ACESSOS_4G_WCMDA'])

        # Agregação final de cobertura
        if coverage_accum:
            coverage_total = pd.concat(coverage_accum)
            coverage_final = coverage_total.groupby('Código IBGE Município')[['has_3g', 'has_4g', 'has_5g']].max().reset_index()

            coverage_final['EC3G'] = coverage_final['has_3g']
            coverage_final['EC4G'] = coverage_final['has_4g']
            coverage_final['COB5G'] = coverage_final['has_5g'] * 3

            coverage_ec3g = coverage_final[['Código IBGE Município', 'EC3G']]
            coverage_ec4g = coverage_final[['Código IBGE Município', 'EC4G']]
            coverage_5g = coverage_final[['Código IBGE Município', 'COB5G']]
        else:
            coverage_ec3g = pd.DataFrame(columns=['Código IBGE Município', 'EC3G'])
            coverage_ec4g = pd.DataFrame(columns=['Código IBGE Município', 'EC4G'])
            coverage_5g = pd.DataFrame(columns=['Código IBGE Município', 'COB5G'])

        return acesso_3g_final, acesso_4g_final, coverage_ec3g, coverage_ec4g, coverage_5g

    def __process_estacoes_smp(self, download_dir: str, final_df: pd.DataFrame) -> pd.DataFrame:
        """Processa indicador QNTD_EST_SMP (quantidade de estações SMP por município)."""
        estacoes_file = os.path.join(download_dir, 'estacoes_municipio_faixa.xlsx')
        base_dir = os.path.dirname(download_dir)
        municipios_file = os.path.join(base_dir, 'municipios.csv')

        if not os.path.exists(estacoes_file) or not os.path.exists(municipios_file):
            print("Warning: estacoes_municipio_faixa.xlsx or municipios.csv not found. Skipping QNTD_EST_SMP.")
            final_df['QNTD_EST_SMP'] = 0
            return final_df

        print("Processing QNTD_EST_SMP...")

        # Carregar mapeamento de municípios
        df_mun = pd.read_csv(municipios_file)
        df_mun['key'] = df_mun['Nome_Município'].str.lower().str.strip() + '_' + df_mun['Nome_UF'].str.lower().str.strip()
        mun_map = df_mun.set_index('key')['Código_Município_Completo'].to_dict()

        # Carregar dados de estações
        df_est = pd.read_excel(estacoes_file)
        df_est = df_est.replace('-', 0)

        # Somar colunas de operadoras
        cols_to_sum = [c for c in df_est.columns if c not in ['Município-UF', 'Operadora']]
        for col in cols_to_sum:
            df_est[col] = pd.to_numeric(df_est[col], errors='coerce').fillna(0)
        df_est['QNTD_EST_SMP'] = df_est[cols_to_sum].sum(axis=1)

        # Extrair nome do município e UF
        df_est[['Nome_Município', 'UF_Sigla']] = df_est['Município-UF'].str.rsplit(' - ', n=1, expand=True)
        df_est['Nome_UF'] = df_est['UF_Sigla'].map(self.UF_MAP)

        # Mapear para código IBGE
        df_est['key'] = df_est['Nome_Município'].str.lower().str.strip() + '_' + df_est['Nome_UF'].str.lower().str.strip()
        df_est['Código IBGE Município'] = df_est['key'].map(mun_map)

        estacoes_final = df_est.groupby('Código IBGE Município')['QNTD_EST_SMP'].sum().reset_index()

        # Merge no DF final
        final_df = final_df.merge(estacoes_final, on='Código IBGE Município', how='left')
        final_df['QNTD_EST_SMP'] = final_df['QNTD_EST_SMP'].fillna(0).astype(int)

        return final_df

    def extract_processed_collection(self) -> list[ProcessedDataCollection]:
        """
        Fluxo unificado: faz o scraping dos dados da Anatel e em seguida processa
        todos os indicadores, retornando uma lista de ProcessedDataCollection.
        """
        # 1. Scraping: baixa todos os dados brutos
        print("=" * 60)
        print("ANATEL EXTRACTION - Starting unified scraping + processing")
        print("=" * 60)

        download_dir = self.__scrapper.extract_database()
        print(f"\nRaw data available at: {download_dir}")

        # 2. Processamento: gerar os indicadores
        print("\n--- Processing fixed broadband indicators ---")
        fixed_df = self.__process_fixed_broadband(download_dir)

        print("\n--- Processing mobile indicators ---")
        acesso_3g_final, acesso_4g_final, coverage_ec3g, coverage_ec4g, coverage_5g = self.__process_mobile(download_dir)

        # 3. Merge de todos os indicadores
        print("\n--- Merging all indicators ---")
        final_df = fixed_df.merge(acesso_3g_final, on='Código IBGE Município', how='outer')
        final_df = final_df.merge(acesso_4g_final, on='Código IBGE Município', how='outer')
        final_df = final_df.merge(coverage_ec3g, on='Código IBGE Município', how='outer')
        final_df = final_df.merge(coverage_ec4g, on='Código IBGE Município', how='outer')
        final_df = final_df.merge(coverage_5g, on='Código IBGE Município', how='outer')

        final_df = final_df.fillna(0)
        final_df['ECFO'] = final_df['ECFO'].astype(int)
        final_df['EC3G'] = final_df['EC3G'].astype(int)
        final_df['EC4G'] = final_df['EC4G'].astype(int)
        final_df['COB5G'] = final_df['COB5G'].astype(int)

        # 4. Indicador de estações SMP
        print("\n--- Processing QNTD_EST_SMP ---")
        final_df = self.__process_estacoes_smp(download_dir, final_df)

        # 5. Salvar CSV intermediário no diretório de dados
        output_file = os.path.join(download_dir, 'broadband_indicators.csv')
        print(f"\nSaving indicators to {output_file}...")
        final_df.to_csv(output_file, index=False, sep=';')
        print(f"Saved {len(final_df)} rows.")

        print(final_df.head())

        # 6. Salvar arquivos CSVs para cada variável dinamicamente
        final_standard_df = self.__build_standard_df(final_df)
        
        for sigla in final_standard_df[self.DATA_IDENTIFIER_COLUMN].unique():
            df_var = final_standard_df[final_standard_df[self.DATA_IDENTIFIER_COLUMN] == sigla].copy()
            df_var.rename(columns={
                self.CITY_CODE_COL: 'codigo_ibge',
                self.DATA_IDENTIFIER_COLUMN: 'sigla',
                self.YEAR_COLUMN: 'ano',
                self.DATA_VALUE_COLUMN: 'variavel_valor'
            }, inplace=True)
            if not df_var.empty:
                df_var = df_var[['codigo_ibge', 'sigla', 'ano', 'variavel_valor']]
            
            var_output = os.path.join(download_dir, f"{sigla}.csv")
            df_var.to_csv(var_output, index=False, sep=';', encoding='utf-8')
            print(f"Saved {sigla} to {var_output} ({len(df_var)} rows)")

        # 7. Retornar como ProcessedDataCollection
        data_collection = ProcessedDataCollection(
            category="Telecomunicações",
            dtype=DataTypes.FLOAT,
            data_name="anatel_indicators",
            time_series_years=[2025],
            df=final_standard_df
        )

        print("\n" + "=" * 60)
        print("ANATEL EXTRACTION - Complete!")
        print("=" * 60)

        return [data_collection]

    def __build_standard_df(self, indicators_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma o DF dos indicadores no formato padrão do Data Warehouse:
        colunas: municipio_cod_ibge, variavel_sigla, tempo_ano, valor_variavel
        """
        indicator_columns = [
            'Acesso_SCM', 'ECFO', 'Acesso_SCM>=12Mbps',
            'TOT_ACESSOS_3G', 'TOT_ACESSOS_4G_WCMDA', 'EC3G', 'EC4G', 'COB5G', 'QNTD_EST_SMP'
        ]

        rows = []
        for _, row in indicators_df.iterrows():
            cod_ibge = row['Código IBGE Município']
            for indicator in indicator_columns:
                if indicator in row.index:
                    rows.append({
                        self.YEAR_COLUMN: 2025,
                        self.CITY_CODE_COL: int(cod_ibge),
                        self.DATA_IDENTIFIER_COLUMN: indicator,
                        self.DATA_VALUE_COLUMN: row[indicator]
                    })

        return pd.DataFrame(rows)
