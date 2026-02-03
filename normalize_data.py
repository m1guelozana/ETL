import os
import pandas as pd

# 1 diretório de entrada e 1 diretório de saída.
# diretório de entrada: bronze
# diretório de saída: silver

input_path = "bronze" # diretório de entrada, onde estão os dados brutos. Ler arquivos da pasta bronze (csv e json)
output_path = "silver" # diretório de saída, onde estarão os dados normalizados. Salvar com parquet

os.makedirs(output_path, exist_ok=True)

# Iterar sobre os arquivos no diretório de entrada
for file in os.listdir(input_path):
    file_path = os.path.join(input_path, file)

    if file.endswith(".csv"):
        df = pd.read_csv(file_path)
        output_file = os.path.join(output_path, file.replace(".csv", ".parquet"))
        df.to_parquet(output_file, index=False)
    elif file.endswith(".json"):
        df = pd.read_json(file_path)
        output_file = os.path.join(output_path, file.replace(".json", ".parquet"))
        df.to_parquet(output_file, index=False)
    else:
        print(f"Unsupported file format: {file}")

    # Converter colunas que são listas convertendo-as em strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)


    df = df.drop_duplicates().reset_index(drop=True)

    # Salvar como parquet
    df.to_parquet(output_file, index=False)
    print(f"Processed and saved: {output_file}")

