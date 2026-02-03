import os
import pandas as pd


class NormalizeData:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

    # Normalização de dados
    def normalize_data(self):
        # Iterar sobre os arquivos no diretório de entrada
        for file in os.listdir(self.input_path):
            file_path = os.path.join(self.input_path, file)

            if file.endswith(".csv"):
                df = pd.read_csv(file_path)
            elif file.endswith(".json"):
                df = pd.read_json(file_path)
            else:
                print(f"Unsupported file format: {file}")
                continue

            # Converter colunas que são listas convertendo-as em strings
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, list)).any():
                    df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

            df = df.drop_duplicates().reset_index(drop=True)

            # Salvar como parquet
            output_file = os.path.join(self.output_path, file.rsplit('.', 1)[0] + ".parquet")
            df.to_parquet(output_file, index=False)
            print(f"Processed and saved: {output_file}")


if __name__ == "__main__":
    input_path = "bronze"
    output_path = "silver"

    normalizer = NormalizeData(input_path, output_path)
    normalizer.normalize_data()