import psycopg2

class Database:
  def __init__(self, db_name, user, password, host='localhost', port=5432):
    self.connection = psycopg2.connect(
      dbname=db_name,
      user=user,
      password=password,
      host=host,
      port=port
    )
    self.cursor = self.connection.cursor()

  def create_table(self, table_name, df):
    cursor = self.connection.cursor()
    columns = ", ".join([f"{col} TEXT" for col in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
    self.connection.commit()
    cursor.close()

  def insert_data(self, table_name, df):
    cursor = self.connection.cursor()
    for index, row in df.iterrows():
      placeholders = ", ".join(["%s"] * len(row))
      cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", tuple(row))
    self.connection.commit()
    cursor.close()

  def execute_query(self, query):
    cursor = self.connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results

  def select_all_data_from_table(self, table_name, limit=10):
    cursor = self.connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    results = cursor.fetchall()
    cursor.close()
    return results

  def close(self):
    self.cursor.close()
    self.connection.close()