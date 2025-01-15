import psycopg2
import pandas as pd


DB_NAME = "dbcontabil"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"


def criar_conexao():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Conexão com o banco de dados realizada com sucesso!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar no banco de dados: {e}")
        exit()


def criar_tabela_clientes():
    conn = criar_conexao()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clientes (
            ID_Cliente SERIAL PRIMARY KEY,
            Nome TEXT NOT NULL,
            Localizacao TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    print("Tabela 'clientes' criada com sucesso!")


def inserir_dados_clientes(caminho_csv):
    try:
    
        df = pd.read_csv(caminho_csv)

       
        print("Colunas do CSV:", df.columns)

        
        conn = criar_conexao()
        cursor = conn.cursor()

        
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO clientes (ID_Cliente, Nome, Localizacao) VALUES (%s, %s, %s)",
                (row['ID_Cliente'], row['Nome'], row['Localizacao'])
            )

        conn.commit()
        conn.close()
        print("Dados inseridos com sucesso na tabela 'clientes'!")
    except Exception as e:
        print(f"Erro ao inserir os dados no banco: {e}")


if __name__ == "__main__":
    caminho_csv = "Tabela_de_Clientes.csv" 
    criar_tabela_clientes()  
    inserir_dados_clientes(caminho_csv) 
