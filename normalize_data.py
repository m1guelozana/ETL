import os
import pandas as pd


class NormalizeData:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

    def convert_to_string(self, df):
        # Converter colunas que são listas convertendo-as em strings
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)
        return df

    def load_df_from_file(self, file, ext):
        input_path = os.path.join(self.input_path, file)

        # Carregar DataFrame a partir de arquivo CSV ou JSON
        if ext.lower() == ".csv":
            df = pd.read_csv(input_path)
        elif ext.lower() == ".json":
            df = pd.read_json(input_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
        return df

    # Normalização de dados
    def normalize_data(self):
        for file in os.listdir(self.input_path):
            name, ext = os.path.splitext(file)
            output_file = os.path.join(self.output_path, f"{name}", file)

            df = self.load_df_from_file(file, ext)

            df = self.convert_to_string(df)
            df = df.drop_duplicates().reset_index(drop=True)

            # Salvar como parquet
            df.to_parquet(output_file, index=False)
            print(f"Processed and saved: {output_file}")


if __name__ == "__main__":
    input_path = "bronze"
    output_path = "silver"

    normalizer = NormalizeData(input_path, output_path)
    normalizer.normalize_data()