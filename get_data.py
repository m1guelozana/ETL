import requests as req
import pandas as pd

def get_data(cep):
    endpoint = f"https://viacep.com.br/ws/{cep}/json/"
    try:
      res = req.get(endpoint)

      if res.status_code == 200:
        return res.json()
      else:
        return {"error": "CEP not found"}
    except req.exceptions.ConnectionError as e:
      return {"error": str(e)}
    except req.exceptions.Timeout as e:
      return {"error": str(e)}
    except req.exceptions.RequestException as e:
      return {"error": str(e)}

users_path = "bronze/users.csv"
users_df = pd.read_csv(users_path)

cep_list = users_df["cep"].tolist()
cep_info_list = []

for cep in cep_list:
  cep_info = get_data(cep.replace("-", ""))
  cep_info_list.append(cep_info)
  if "error" in cep_info:
    continue
  print(cep_info)

cep_info_df = pd.DataFrame(cep_info_list)
cep_info_df.to_csv("bronze/cep_info.csv", index=False)

