import pandas as pd
import os

def extract_acesso_scm():
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(base_dir, 'anatel_2024', 'Acessos_Banda_Larga_Fixa_2024.csv')
    output_file = os.path.join(base_dir, 'anatel_2024', 'broadband_indicators.csv')

    print(f"Loading data from {input_file}...")
    
    # Load CSV
    try:
        df = pd.read_csv(input_file, sep=';', decimal=',', encoding='utf-8')
    except UnicodeDecodeError:
        print("UTF-8 decode failed, trying latin-1...")
        df = pd.read_csv(input_file, sep=';', decimal=',', encoding='latin-1')

    print("Data loaded successfully.")
    
    # Ensure 'Acessos' is numeric
    df['Acessos'] = pd.to_numeric(df['Acessos'], errors='coerce').fillna(0)

    # --- Indicator 1: Escala de acesso a banda larga fixa ---
    # Formula: (Acesso_SCM/POP_TOT)*100 -> We need Acesso_SCM (Total Accesses)
    print("Calculating Acesso_SCM...")
    acesso_scm = df.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
    acesso_scm.rename(columns={'Acessos': 'Acesso_SCM'}, inplace=True)

    # --- Indicator 2: Cobertura de fibra ótica ---
    # Formula: Existence (Sim/Não -> 1/0) if 'Meio de Acesso' == 'Fibra'
    print("Calculating Cobertura_Fibra...")
    # Create a flag for Fiber
    df['is_fiber'] = (df['Meio de Acesso'] == 'Fibra').astype(int)
    # Group by municipality and take max (if at least one record is fiber, max will be 1)
    cobertura_fibra = df.groupby('Código IBGE Município')['is_fiber'].max().reset_index()
    cobertura_fibra.rename(columns={'is_fiber': 'Cobertura_Fibra'}, inplace=True)

    # --- Indicator 3: Escala de acesso a banda larga fixa de alta velocidade ---
    # Formula: (Acesso_SCM>=12Mbps/POP_TOT) * 100 -> We need Acesso_SCM >= 12Mbps
    # High speed ranges: '12Mbps a 34Mbps', '> 34Mbps'
    print("Calculating Acesso_SCM_HighSpeed...")
    high_speed_ranges = ['12Mbps a 34Mbps', '> 34Mbps']
    df_high_speed = df[df['Faixa de Velocidade'].isin(high_speed_ranges)]
    acesso_high_speed = df_high_speed.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
    acesso_high_speed.rename(columns={'Acessos': 'Acesso_SCM_HighSpeed'}, inplace=True)

    # --- Merge Indicators (Fixed Broadband) ---
    print("Merging fixed broadband indicators...")
    # Start with the main list of municipalities from acesso_scm
    fixed_df = acesso_scm
    
    # Merge Cobertura_Fibra
    fixed_df = fixed_df.merge(cobertura_fibra, on='Código IBGE Município', how='outer')
    
    # Merge Acesso_SCM_HighSpeed
    fixed_df = fixed_df.merge(acesso_high_speed, on='Código IBGE Município', how='outer')
    
    # Fill NaNs with 0 for fixed broadband columns
    fixed_df['Acesso_SCM'] = fixed_df['Acesso_SCM'].fillna(0)
    fixed_df['Acesso_SCM_HighSpeed'] = fixed_df['Acesso_SCM_HighSpeed'].fillna(0)
    fixed_df['Cobertura_Fibra'] = fixed_df['Cobertura_Fibra'].fillna(0).astype(int)

    # --- Mobile Indicators ---
    print("Processing mobile indicators (chunked)...")
    mobile_input_file = os.path.join(base_dir, 'anatel_2024', 'Acessos_Telefonia_Movel_2024_2S.csv')
    
    print(f"Loading mobile data from {mobile_input_file}...")
    
    chunk_size = 500000
    mobile_columns = ['Mês', 'Tipo de Produto', 'Tecnologia Geração', 'Acessos', 'Código IBGE Município']
    
    acesso_mobile_accum = []
    coverage_accum = []

    try:
        # Using utf-8-sig because inspection showed BOM
        for chunk in pd.read_csv(mobile_input_file, sep=';', decimal=',', encoding='utf-8-sig', usecols=mobile_columns, chunksize=chunk_size):
            # Filter for December 2024 and VOZ+DADOS
            chunk = chunk[(chunk['Mês'] == 12) & (chunk['Tipo de Produto'] == 'VOZ+DADOS')]
            if chunk.empty:
                continue
            
            chunk['Acessos'] = pd.to_numeric(chunk['Acessos'], errors='coerce').fillna(0)
            
            # Indicator 4: Accumulate sums
            chunk_3g_4g = chunk[chunk['Tecnologia Geração'].isin(['3G', '4G'])]
            if not chunk_3g_4g.empty:
                agg_acesso = chunk_3g_4g.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
                acesso_mobile_accum.append(agg_acesso)
            
            # Indicator 5 & 6: Accumulate presence flags
            chunk['has_3g'] = (chunk['Tecnologia Geração'] == '3G').astype(int)
            chunk['has_4g'] = (chunk['Tecnologia Geração'] == '4G').astype(int)
            chunk['has_5g'] = (chunk['Tecnologia Geração'] == '5G').astype(int)
            
            agg_cov = chunk.groupby('Código IBGE Município')[['has_3g', 'has_4g', 'has_5g']].max().reset_index()
            coverage_accum.append(agg_cov)
            
    except Exception as e:
        print(f"Error loading mobile data: {e}")
        return

    print("Aggregating mobile results...")
    
    # Final Aggregation for Accesses
    if acesso_mobile_accum:
        acesso_mobile_total = pd.concat(acesso_mobile_accum)
        acesso_mobile_final = acesso_mobile_total.groupby('Código IBGE Município')['Acessos'].sum().reset_index()
        acesso_mobile_final.rename(columns={'Acessos': 'Acesso_Banda_Larga_Movel'}, inplace=True)
    else:
        acesso_mobile_final = pd.DataFrame(columns=['Código IBGE Município', 'Acesso_Banda_Larga_Movel'])

    # Final Aggregation for Coverage
    if coverage_accum:
        coverage_total = pd.concat(coverage_accum)
        coverage_final = coverage_total.groupby('Código IBGE Município')[['has_3g', 'has_4g', 'has_5g']].max().reset_index()
        
        coverage_final['Cobertura_3G_4G'] = (coverage_final['has_3g'] * 1) + (coverage_final['has_4g'] * 2)
        coverage_final['Cobertura_5G'] = coverage_final['has_5g'] * 3
        
        coverage_3g_4g = coverage_final[['Código IBGE Município', 'Cobertura_3G_4G']]
        coverage_5g = coverage_final[['Código IBGE Município', 'Cobertura_5G']]
    else:
        coverage_3g_4g = pd.DataFrame(columns=['Código IBGE Município', 'Cobertura_3G_4G'])
        coverage_5g = pd.DataFrame(columns=['Código IBGE Município', 'Cobertura_5G'])

    # --- Final Merge ---
    print("Merging all indicators...")
    final_df = fixed_df.merge(acesso_mobile_final, on='Código IBGE Município', how='outer')
    final_df = final_df.merge(coverage_3g_4g, on='Código IBGE Município', how='outer')
    final_df = final_df.merge(coverage_5g, on='Código IBGE Município', how='outer')

    # Fill NaNs with 0
    final_df = final_df.fillna(0)

    # Ensure integer columns where appropriate
    final_df['Cobertura_Fibra'] = final_df['Cobertura_Fibra'].astype(int)
    final_df['Cobertura_3G_4G'] = final_df['Cobertura_3G_4G'].astype(int)
    final_df['Cobertura_5G'] = final_df['Cobertura_5G'].astype(int)

    # --- Indicator 7: QNTD_EST_SMP ---
    print("Processing QNTD_EST_SMP...")
    estacoes_file = os.path.join(base_dir, 'anatel_2024', 'estacoes_municipio_faixa.xlsx')
    municipios_file = os.path.join(base_dir, 'municipios.csv')

    if os.path.exists(estacoes_file) and os.path.exists(municipios_file):
        # Load Municipios Mapping
        df_mun = pd.read_csv(municipios_file)
        # Create a key for mapping: Nome_Município + Nome_UF
        df_mun['key'] = df_mun['Nome_Município'].str.lower().str.strip() + '_' + df_mun['Nome_UF'].str.lower().str.strip()
        mun_map = df_mun.set_index('key')['Código_Município_Completo'].to_dict()

        # Load Stations Data
        df_est = pd.read_excel(estacoes_file)
        
        # Calculate Total Stations (Sum all columns except the first two which are usually identifiers, 
        # but here we saw 'Município-UF' and 'Operadora' in the first check, wait, let's re-verify columns from previous tool output)
        # The previous output showed: ['Município-UF', 'Operadora', '-', 'ALGAR', 'BRISANET', 'CLARO', 'GIGA+', 'Iez! Telecom Ltda.', 'SERCOMTEL', 'TIM', 'UNIFIQUE TELECOMUNICACOES S/A', 'VIVO']
        # The user said: "QNTD_EST_SMP que é apenas o somatorio dos valores de todas as colunas dentro desse arquivo .xlsx... Colunas sem valor contém um traço '-'"
        # We need to sum the operator columns.
        
        # Replace '-' with 0
        df_est = df_est.replace('-', 0)
        
        # Identify numeric columns (operators) - exclude 'Município-UF' and 'Operadora' if present, but the user said "somatorio dos valores de todas as colunas". 
        # Looking at the head: 
        # 0      Abadia de Goiás - GO  ...     2
        # It seems 'Município-UF' is the identifier. 'Operadora' was in the columns list from my previous check? 
        # Wait, the previous check output was: ['Município-UF', 'Operadora', '-', 'ALGAR', ...]. 
        # Let's assume 'Município-UF' is the location. 'Operadora' might be a column? Or maybe the header is messy.
        # Actually, looking at the previous `read_excel` output:
        #    Município-UF  ...  VIVO
        # 0  Abadia de Goiás - GO  ...     2
        # It seems the columns are operators.
        
        # Let's drop 'Município-UF' and 'Operadora' (if exists) for summation
        cols_to_sum = [c for c in df_est.columns if c not in ['Município-UF', 'Operadora']]
        
        # Ensure they are numeric
        for col in cols_to_sum:
            df_est[col] = pd.to_numeric(df_est[col], errors='coerce').fillna(0)
            
        df_est['QNTD_EST_SMP'] = df_est[cols_to_sum].sum(axis=1)
        
        # Extract Municipality Name and UF Sigla
        # Format: "Município - UF"
        df_est[['Nome_Município', 'UF_Sigla']] = df_est['Município-UF'].str.rsplit(' - ', n=1, expand=True)
        
        # Map UF Sigla to UF Name
        uf_map = {
            'RO': 'Rondônia', 'AC': 'Acre', 'AM': 'Amazonas', 'RR': 'Roraima', 'PA': 'Pará', 'AP': 'Amapá', 'TO': 'Tocantins',
            'MA': 'Maranhão', 'PI': 'Piauí', 'CE': 'Ceará', 'RN': 'Rio Grande do Norte', 'PB': 'Paraíba', 'PE': 'Pernambuco',
            'AL': 'Alagoas', 'SE': 'Sergipe', 'BA': 'Bahia', 'MG': 'Minas Gerais', 'ES': 'Espírito Santo', 'RJ': 'Rio de Janeiro',
            'SP': 'São Paulo', 'PR': 'Paraná', 'SC': 'Santa Catarina', 'RS': 'Rio Grande do Sul', 'MS': 'Mato Grosso do Sul',
            'MT': 'Mato Grosso', 'GO': 'Goiás', 'DF': 'Distrito Federal'
        }
        
        df_est['Nome_UF'] = df_est['UF_Sigla'].map(uf_map)
        
        # Create key for mapping
        df_est['key'] = df_est['Nome_Município'].str.lower().str.strip() + '_' + df_est['Nome_UF'].str.lower().str.strip()
        
        # Map to IBGE Code
        df_est['Código IBGE Município'] = df_est['key'].map(mun_map)
        
        # Group by IBGE Code (in case of duplicates, though unlikely for this file structure) and sum
        estacoes_final = df_est.groupby('Código IBGE Município')['QNTD_EST_SMP'].sum().reset_index()
        
        # Merge into final_df
        final_df = final_df.merge(estacoes_final, on='Código IBGE Município', how='left')
        final_df['QNTD_EST_SMP'] = final_df['QNTD_EST_SMP'].fillna(0).astype(int)
        
    else:
        print("Warning: estacoes_municipio_faixa.xlsx or municipios.csv not found. Skipping QNTD_EST_SMP.")
        final_df['QNTD_EST_SMP'] = 0
    print(final_df.head())

    # Save to CSV
    print(f"Saving to {output_file}...")
    final_df.to_csv(output_file, index=False, sep=';')
    print("Done.")

if __name__ == "__main__":
    extract_acesso_scm()
